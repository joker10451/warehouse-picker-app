import sqlite3
from datetime import date, datetime
from pathlib import Path

from fastapi import FastAPI, Form, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

APP_DIR = Path(__file__).parent
DATA_DIR = APP_DIR / "data"
EXPORTS_DIR = APP_DIR / "exports"
PRINTS_DIR = APP_DIR / "prints"
DB_PATH = DATA_DIR / "warehouse.db"

WORK_TYPES = ["сборка", "упаковка", "приемка"]
WAREHOUSES = ["Инбев", "Балтика", "Кола", "3PL Инбев", "3PL Балтика"]

app = FastAPI(title="Складской учет")
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")


def ensure_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    EXPORTS_DIR.mkdir(exist_ok=True)
    PRINTS_DIR.mkdir(exist_ok=True)


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    ensure_dirs()
    conn = db()
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS work_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_date TEXT NOT NULL,
            work_time TEXT NOT NULL DEFAULT '20:00',
            picker TEXT NOT NULL,
            warehouse TEXT NOT NULL DEFAULT 'Инбев',
            truck_number TEXT NOT NULL DEFAULT '',
            order_number TEXT NOT NULL,
            work_type TEXT NOT NULL,
            quantity_kg INTEGER NOT NULL CHECK(quantity_kg > 0),
            comment TEXT DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pickers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        """
    )
    cur.execute("SELECT COUNT(*) AS cnt FROM pickers")
    if cur.fetchone()["cnt"] == 0:
        for name in ["Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов"]:
            cur.execute("INSERT INTO pickers(name) VALUES(?)", (name,))

    # Migrations from older schema.
    cur.execute("PRAGMA table_info(work_logs)")
    columns = {r["name"] for r in cur.fetchall()}
    if "work_time" not in columns:
        cur.execute("ALTER TABLE work_logs ADD COLUMN work_time TEXT NOT NULL DEFAULT '20:00'")
    if "warehouse" not in columns:
        cur.execute("ALTER TABLE work_logs ADD COLUMN warehouse TEXT NOT NULL DEFAULT 'Инбев'")
    if "truck_number" not in columns:
        cur.execute("ALTER TABLE work_logs ADD COLUMN truck_number TEXT NOT NULL DEFAULT ''")
    if "quantity_kg" not in columns and "quantity" in columns:
        cur.execute("ALTER TABLE work_logs ADD COLUMN quantity_kg INTEGER NOT NULL DEFAULT 1")
        cur.execute("UPDATE work_logs SET quantity_kg = COALESCE(quantity, 1)")
    elif "quantity_kg" not in columns:
        cur.execute("ALTER TABLE work_logs ADD COLUMN quantity_kg INTEGER NOT NULL DEFAULT 1")

    conn.commit()
    conn.close()


def get_pickers() -> list[str]:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT name FROM pickers ORDER BY name")
    rows = [r["name"] for r in cur.fetchall()]
    conn.close()
    return rows


def register_font() -> str:
    for p in [Path("C:/Windows/Fonts/arial.ttf"), Path("C:/Windows/Fonts/tahoma.ttf")]:
        if p.exists():
            pdfmetrics.registerFont(TTFont("WarehouseRU", str(p)))
            return "WarehouseRU"
    return "Helvetica"


def query_logs(
    work_date: str = "",
    picker: str = "",
    work_type: str = "",
    warehouse: str = "",
    truck_number: str = "",
    order_query: str = "",
) -> list[sqlite3.Row]:
    conn = db()
    sql = "SELECT * FROM work_logs WHERE 1=1"
    args: list[str] = []
    if work_date:
        sql += " AND work_date = ?"
        args.append(work_date)
    if picker:
        sql += " AND picker = ?"
        args.append(picker)
    if work_type:
        sql += " AND work_type = ?"
        args.append(work_type)
    if warehouse:
        sql += " AND warehouse = ?"
        args.append(warehouse)
    if truck_number:
        sql += " AND truck_number LIKE ?"
        args.append(f"%{truck_number}%")
    if order_query:
        sql += " AND order_number LIKE ?"
        args.append(f"%{order_query}%")
    sql += " ORDER BY work_date DESC, work_time DESC, id DESC LIMIT 5000"
    cur = conn.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall()
    conn.close()
    return rows


def export_excel(rows: list[sqlite3.Row], out: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Журнал"
    ws.append(["Дата", "Время", "Сборщик", "Склад", "Машина", "Номер заказа", "Тип работы", "КГ", "Комментарий"])
    for r in rows:
        ws.append(
            [
                r["work_date"],
                r["work_time"],
                r["picker"],
                r["warehouse"],
                r["truck_number"],
                r["order_number"],
                r["work_type"],
                r["quantity_kg"],
                r["comment"],
            ]
        )
    wb.save(out)


def export_pdf(title: str, headers: list[str], data: list[list[str]], out: Path) -> None:
    font = register_font()
    doc = SimpleDocTemplate(str(out), pagesize=landscape(A4), leftMargin=10 * mm, rightMargin=10 * mm)
    styles = getSampleStyleSheet()
    styles["Normal"].fontName = font
    styles["Title"].fontName = font
    t = Table([headers] + data, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F4E78")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
            ]
        )
    )
    doc.build([Paragraph(title, styles["Title"]), Spacer(1, 5 * mm), t])


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    context = {
        "request": request,
        "today": date.today().isoformat(),
        "now_time": datetime.now().strftime("%H:%M"),
        "pickers": get_pickers(),
        "work_types": WORK_TYPES,
        "warehouses": WAREHOUSES,
    }
    return templates.TemplateResponse(request=request, name="index.html", context=context)


@app.post("/add-picker")
def add_picker(name: str = Form(...)) -> RedirectResponse:
    conn = db()
    conn.execute("INSERT OR IGNORE INTO pickers(name) VALUES(?)", (name.strip(),))
    conn.commit()
    conn.close()
    return RedirectResponse("/", status_code=303)


@app.post("/add-log")
def add_log(
    work_date: str = Form(...),
    work_time: str = Form(...),
    picker: str = Form(...),
    warehouse: str = Form(...),
    truck_number: str = Form(...),
    order_number: str = Form(...),
    work_type: str = Form(...),
    quantity_kg: int = Form(...),
    comment: str = Form(""),
) -> RedirectResponse:
    conn = db()
    conn.execute(
        """
        INSERT INTO work_logs(work_date, work_time, picker, warehouse, truck_number, order_number, work_type, quantity_kg, comment, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            work_date,
            work_time,
            picker,
            warehouse,
            truck_number.strip(),
            order_number.strip(),
            work_type,
            quantity_kg,
            comment.strip(),
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/journal", status_code=303)


@app.get("/journal", response_class=HTMLResponse)
def journal(
    request: Request,
    work_date: str = Query(default=""),
    picker: str = Query(default=""),
    work_type: str = Query(default=""),
    warehouse: str = Query(default=""),
    truck_number: str = Query(default=""),
    order_query: str = Query(default=""),
) -> HTMLResponse:
    rows = query_logs(work_date, picker, work_type, warehouse, truck_number, order_query)
    context = {
        "request": request,
        "rows": rows,
        "pickers": get_pickers(),
        "work_types": WORK_TYPES,
        "warehouses": WAREHOUSES,
        "filters": {
            "work_date": work_date,
            "picker": picker,
            "work_type": work_type,
            "warehouse": warehouse,
            "truck_number": truck_number,
            "order_query": order_query,
        },
    }
    return templates.TemplateResponse(request=request, name="journal.html", context=context)


def stats_for(day: str, date_from: str, date_to: str) -> dict:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT picker, COUNT(DISTINCT order_number) AS orders_count, COALESCE(SUM(quantity_kg), 0) AS total_qty
        FROM work_logs WHERE work_date = ? GROUP BY picker ORDER BY picker
        """,
        (day,),
    )
    day_rows = cur.fetchall()
    cur.execute(
        """
        SELECT picker, COUNT(DISTINCT order_number) AS orders_count, COALESCE(SUM(quantity_kg), 0) AS total_qty
        FROM work_logs WHERE work_date BETWEEN ? AND ? GROUP BY picker ORDER BY picker
        """,
        (date_from, date_to),
    )
    period_rows = cur.fetchall()
    cur.execute("SELECT COALESCE(SUM(quantity_kg), 0) AS total FROM work_logs WHERE work_date BETWEEN ? AND ?", (date_from, date_to))
    total = int(cur.fetchone()["total"])
    cur.execute(
        """
        SELECT work_type, COALESCE(SUM(quantity_kg), 0) AS total_qty
        FROM work_logs WHERE work_date BETWEEN ? AND ? GROUP BY work_type ORDER BY work_type
        """,
        (date_from, date_to),
    )
    types_rows = cur.fetchall()
    cur.execute(
        """
        SELECT picker, GROUP_CONCAT(DISTINCT truck_number) AS trucks, GROUP_CONCAT(DISTINCT work_type) AS actions
        FROM work_logs WHERE work_date = ?
        GROUP BY picker ORDER BY picker
        """,
        (day,),
    )
    picker_details = cur.fetchall()
    conn.close()
    return {
        "day_rows": day_rows,
        "period_rows": period_rows,
        "total": total,
        "types_rows": types_rows,
        "picker_details": picker_details,
    }


@app.get("/stats", response_class=HTMLResponse)
def stats(
    request: Request,
    day: str = Query(default_factory=lambda: date.today().isoformat()),
    date_from: str = Query(default_factory=lambda: date.today().isoformat()),
    date_to: str = Query(default_factory=lambda: date.today().isoformat()),
) -> HTMLResponse:
    data = stats_for(day, date_from, date_to)
    context = {"request": request, "day": day, "date_from": date_from, "date_to": date_to, **data}
    return templates.TemplateResponse(request=request, name="stats.html", context=context)


@app.get("/export/journal.xlsx")
def export_journal_xlsx(
    work_date: str = Query(default=""),
    picker: str = Query(default=""),
    work_type: str = Query(default=""),
    warehouse: str = Query(default=""),
    truck_number: str = Query(default=""),
    order_query: str = Query(default=""),
) -> FileResponse:
    out = EXPORTS_DIR / f"journal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    export_excel(query_logs(work_date, picker, work_type, warehouse, truck_number, order_query), out)
    return FileResponse(str(out), filename=out.name)


@app.get("/export/journal.pdf")
def export_journal_pdf(
    work_date: str = Query(default=""),
    picker: str = Query(default=""),
    work_type: str = Query(default=""),
    warehouse: str = Query(default=""),
    truck_number: str = Query(default=""),
    order_query: str = Query(default=""),
) -> FileResponse:
    rows = query_logs(work_date, picker, work_type, warehouse, truck_number, order_query)
    data = [
        [
            r["work_date"],
            r["work_time"],
            r["picker"],
            r["warehouse"],
            r["truck_number"],
            r["order_number"],
            r["work_type"],
            str(r["quantity_kg"]),
            r["comment"],
        ]
        for r in rows
    ]
    out = PRINTS_DIR / f"journal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    export_pdf("Журнал работ (A4)", ["Дата", "Время", "Сборщик", "Склад", "Машина", "Заказ", "Тип", "КГ", "Комментарий"], data, out)
    return FileResponse(str(out), filename=out.name)


@app.get("/export/stats.pdf")
def export_stats_pdf(date_from: str = Query(...), date_to: str = Query(...)) -> FileResponse:
    data = stats_for(day=date_from, date_from=date_from, date_to=date_to)
    rows = [[r["picker"], str(r["orders_count"]), str(r["total_qty"])] for r in data["period_rows"]]
    out = PRINTS_DIR / f"stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    export_pdf(f"Статистика {date_from} - {date_to}", ["Сборщик", "Заказов", "КГ"], rows, out)
    return FileResponse(str(out), filename=out.name)
