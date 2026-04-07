import sqlite3
import os
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
import psycopg
from psycopg.rows import dict_row

APP_DIR = Path(__file__).parent
DATA_DIR = APP_DIR / "data"
EXPORTS_DIR = APP_DIR / "exports"
PRINTS_DIR = APP_DIR / "prints"
DB_PATH = DATA_DIR / "warehouse.db"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = DATABASE_URL.startswith("postgres")

WAREHOUSES = ["Инбев", "Балтика", "Кола", "3PL Инбев", "3PL Балтика"]

app = FastAPI(title="Складской учет")
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")


def ensure_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    EXPORTS_DIR.mkdir(exist_ok=True)
    PRINTS_DIR.mkdir(exist_ok=True)


def db() -> sqlite3.Connection:
    if USE_POSTGRES:
        return psycopg.connect(DATABASE_URL, row_factory=dict_row)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def run_sql(cur, sql: str, params: tuple | list = ()) -> None:
    if USE_POSTGRES:
        cur.execute(sql.replace("?", "%s"), params)
    else:
        cur.execute(sql, params)


def init_db() -> None:
    ensure_dirs()
    conn = db()
    cur = conn.cursor()
    if USE_POSTGRES:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS work_logs (
                id BIGSERIAL PRIMARY KEY,
                work_date TEXT NOT NULL,
                work_time TEXT NOT NULL DEFAULT '20:00',
                picker TEXT NOT NULL,
                warehouse TEXT NOT NULL DEFAULT 'Инбев',
                truck_number TEXT NOT NULL DEFAULT '',
                order_number TEXT NOT NULL DEFAULT '',
                work_type TEXT NOT NULL,
                quantity_kg INTEGER NOT NULL CHECK(quantity_kg > 0),
                comment TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pickers (
                id BIGSERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS shift_attendance (
                id BIGSERIAL PRIMARY KEY,
                shift_date TEXT NOT NULL,
                picker TEXT NOT NULL,
                UNIQUE(shift_date, picker)
            );
            """
        )
    else:
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
            CREATE TABLE IF NOT EXISTS shift_attendance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shift_date TEXT NOT NULL,
                picker TEXT NOT NULL,
                UNIQUE(shift_date, picker)
            );
            """
        )
    run_sql(cur, "SELECT COUNT(*) AS cnt FROM pickers")
    if cur.fetchone()["cnt"] == 0:
        for name in ["Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов"]:
            run_sql(cur, "INSERT INTO pickers(name) VALUES(?)", (name,))

    if not USE_POSTGRES:
        # Migrations from older sqlite schema.
        run_sql(cur, "PRAGMA table_info(work_logs)")
        columns = {r["name"] for r in cur.fetchall()}
        if "work_time" not in columns:
            run_sql(cur, "ALTER TABLE work_logs ADD COLUMN work_time TEXT NOT NULL DEFAULT '20:00'")
        if "warehouse" not in columns:
            run_sql(cur, "ALTER TABLE work_logs ADD COLUMN warehouse TEXT NOT NULL DEFAULT 'Инбев'")
        if "truck_number" not in columns:
            run_sql(cur, "ALTER TABLE work_logs ADD COLUMN truck_number TEXT NOT NULL DEFAULT ''")
        if "quantity_kg" not in columns and "quantity" in columns:
            run_sql(cur, "ALTER TABLE work_logs ADD COLUMN quantity_kg INTEGER NOT NULL DEFAULT 1")
            run_sql(cur, "UPDATE work_logs SET quantity_kg = COALESCE(quantity, 1)")
        elif "quantity_kg" not in columns:
            run_sql(cur, "ALTER TABLE work_logs ADD COLUMN quantity_kg INTEGER NOT NULL DEFAULT 1")

    conn.commit()
    conn.close()


def get_pickers() -> list[str]:
    conn = db()
    cur = conn.cursor()
    run_sql(cur, "SELECT name FROM pickers ORDER BY name")
    rows = [r["name"] for r in cur.fetchall()]
    conn.close()
    return rows


def get_work_types() -> list[str]:
    conn = db()
    cur = conn.cursor()
    run_sql(cur, "SELECT DISTINCT work_type FROM work_logs WHERE TRIM(work_type) <> '' ORDER BY work_type")
    rows = [r["work_type"] for r in cur.fetchall()]
    conn.close()
    return rows


def get_shift_pickers(shift_date: str) -> list[str]:
    conn = db()
    cur = conn.cursor()
    run_sql(cur, "SELECT picker FROM shift_attendance WHERE shift_date = ? ORDER BY picker", (shift_date,))
    rows = [r["picker"] for r in cur.fetchall()]
    conn.close()
    return rows


def set_shift_pickers(shift_date: str, pickers: list[str]) -> None:
    conn = db()
    cur = conn.cursor()
    run_sql(cur, "DELETE FROM shift_attendance WHERE shift_date = ?", (shift_date,))
    for picker in sorted({p.strip() for p in pickers if p.strip()}):
        if USE_POSTGRES:
            cur.execute(
                "INSERT INTO shift_attendance(shift_date, picker) VALUES(%s, %s) ON CONFLICT (shift_date, picker) DO NOTHING",
                (shift_date, picker),
            )
        else:
            run_sql(cur, "INSERT OR IGNORE INTO shift_attendance(shift_date, picker) VALUES(?, ?)", (shift_date, picker))
    conn.commit()
    conn.close()


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
    sql += " ORDER BY work_date DESC, work_time DESC, id DESC LIMIT 5000"
    cur = conn.cursor()
    run_sql(cur, sql, tuple(args))
    rows = cur.fetchall()
    conn.close()
    return rows


def export_excel(rows: list[sqlite3.Row], out: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Журнал"
    ws.append(["Дата", "Время", "Сборщик", "Склад", "Машина", "Тип работы", "КГ", "Комментарий"])
    for r in rows:
        ws.append(
            [
                r["work_date"],
                r["work_time"],
                r["picker"],
                r["warehouse"],
                r["truck_number"],
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
    today = date.today().isoformat()
    context = {
        "request": request,
        "today": today,
        "now_time": datetime.now().strftime("%H:%M"),
        "pickers": get_pickers(),
        "warehouses": WAREHOUSES,
        "shift_pickers_today": get_shift_pickers(today),
    }
    return templates.TemplateResponse(request=request, name="index.html", context=context)


@app.post("/add-picker")
def add_picker(name: str = Form(...)) -> RedirectResponse:
    conn = db()
    cur = conn.cursor()
    if USE_POSTGRES:
        cur.execute("INSERT INTO pickers(name) VALUES(%s) ON CONFLICT (name) DO NOTHING", (name.strip(),))
    else:
        run_sql(cur, "INSERT OR IGNORE INTO pickers(name) VALUES(?)", (name.strip(),))
    conn.commit()
    conn.close()
    return RedirectResponse("/", status_code=303)


@app.post("/set-shift")
def set_shift(shift_date: str = Form(...), pickers: list[str] = Form(default=[])) -> RedirectResponse:
    set_shift_pickers(shift_date, pickers)
    return RedirectResponse("/", status_code=303)


@app.post("/add-log")
def add_log(
    work_date: str = Form(...),
    work_time: str = Form(...),
    picker: str = Form(...),
    warehouse: str = Form(...),
    truck_number: str = Form(...),
    work_type: str = Form(...),
    quantity_kg: int = Form(...),
    comment: str = Form(""),
) -> RedirectResponse:
    conn = db()
    cur = conn.cursor()
    run_sql(
        cur,
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
            "",
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
) -> HTMLResponse:
    rows = query_logs(work_date, picker, work_type, warehouse, truck_number)
    context = {
        "request": request,
        "rows": rows,
        "rows_count": len(rows),
        "pickers": get_pickers(),
        "work_types": get_work_types(),
        "warehouses": WAREHOUSES,
        "filters": {
            "work_date": work_date,
            "picker": picker,
            "work_type": work_type,
            "warehouse": warehouse,
            "truck_number": truck_number,
        },
    }
    return templates.TemplateResponse(request=request, name="journal.html", context=context)


def stats_for(day: str, date_from: str, date_to: str) -> dict:
    conn = db()
    cur = conn.cursor()
    run_sql(
        cur,
        """
        SELECT picker, COUNT(DISTINCT truck_number) AS trucks_count, COALESCE(SUM(quantity_kg), 0) AS total_qty
        FROM work_logs WHERE work_date = ? GROUP BY picker ORDER BY picker
        """,
        (day,),
    )
    day_rows = cur.fetchall()
    run_sql(
        cur,
        """
        SELECT picker, COUNT(DISTINCT truck_number) AS trucks_count, COALESCE(SUM(quantity_kg), 0) AS total_qty
        FROM work_logs WHERE work_date BETWEEN ? AND ? GROUP BY picker ORDER BY picker
        """,
        (date_from, date_to),
    )
    period_rows = cur.fetchall()
    run_sql(cur, "SELECT COALESCE(SUM(quantity_kg), 0) AS total FROM work_logs WHERE work_date BETWEEN ? AND ?", (date_from, date_to))
    total = int(cur.fetchone()["total"])
    run_sql(
        cur,
        """
        SELECT work_type, COALESCE(SUM(quantity_kg), 0) AS total_qty
        FROM work_logs WHERE work_date BETWEEN ? AND ? GROUP BY work_type ORDER BY work_type
        """,
        (date_from, date_to),
    )
    types_rows = cur.fetchall()
    run_sql(
        cur,
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


def live_dashboard(day: str) -> dict:
    conn = db()
    cur = conn.cursor()
    run_sql(
        cur,
        """
        WITH latest_per_truck AS (
            SELECT
                truck_number,
                picker,
                warehouse,
                work_type,
                work_time,
                ROW_NUMBER() OVER (
                    PARTITION BY truck_number
                    ORDER BY work_time DESC, id DESC
                ) AS rn
            FROM work_logs
            WHERE work_date = ? AND TRIM(truck_number) <> ''
        )
        SELECT truck_number, picker, warehouse, work_type, work_time
        FROM latest_per_truck
        WHERE rn = 1
        ORDER BY work_time DESC, truck_number
        """,
        (day,),
    )
    latest_rows = cur.fetchall()

    active = [r for r in latest_rows if "закры" not in (r["work_type"] or "").lower()]
    closed = [r for r in latest_rows if "закры" in (r["work_type"] or "").lower()]
    busy_pickers = {r["picker"] for r in active if (r["picker"] or "").strip()}
    on_shift = get_shift_pickers(day)
    source = set(on_shift) if on_shift else set(get_pickers())
    free_pickers = sorted(source - busy_pickers)

    conn.close()
    return {
        "active_trucks": active,
        "closed_trucks": closed,
        "free_pickers": free_pickers,
        "busy_count": len(active),
        "closed_count": len(closed),
        "free_count": len(free_pickers),
        "on_shift_count": len(source),
        "on_shift_pickers": sorted(source),
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


@app.get("/live", response_class=HTMLResponse)
def live(request: Request, day: str = Query(default_factory=lambda: date.today().isoformat())) -> HTMLResponse:
    data = live_dashboard(day)
    context = {"request": request, "day": day, "updated_at": datetime.now().strftime("%H:%M"), **data}
    return templates.TemplateResponse(request=request, name="live.html", context=context)


@app.get("/export/journal.xlsx")
def export_journal_xlsx(
    work_date: str = Query(default=""),
    picker: str = Query(default=""),
    work_type: str = Query(default=""),
    warehouse: str = Query(default=""),
    truck_number: str = Query(default=""),
) -> FileResponse:
    out = EXPORTS_DIR / f"journal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    export_excel(query_logs(work_date, picker, work_type, warehouse, truck_number), out)
    return FileResponse(str(out), filename=out.name)


@app.get("/export/journal.pdf")
def export_journal_pdf(
    work_date: str = Query(default=""),
    picker: str = Query(default=""),
    work_type: str = Query(default=""),
    warehouse: str = Query(default=""),
    truck_number: str = Query(default=""),
) -> FileResponse:
    rows = query_logs(work_date, picker, work_type, warehouse, truck_number)
    data = [
        [
            r["work_date"],
            r["work_time"],
            r["picker"],
            r["warehouse"],
            r["truck_number"],
            r["work_type"],
            str(r["quantity_kg"]),
            r["comment"],
        ]
        for r in rows
    ]
    out = PRINTS_DIR / f"journal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    export_pdf("Журнал работ (A4)", ["Дата", "Время", "Сборщик", "Склад", "Машина", "Тип", "КГ", "Комментарий"], data, out)
    return FileResponse(str(out), filename=out.name)


@app.get("/export/stats.pdf")
def export_stats_pdf(date_from: str = Query(...), date_to: str = Query(...)) -> FileResponse:
    data = stats_for(day=date_from, date_from=date_from, date_to=date_to)
    rows = [[r["picker"], str(r["trucks_count"]), str(r["total_qty"])] for r in data["period_rows"]]
    out = PRINTS_DIR / f"stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    export_pdf(f"Статистика {date_from} - {date_to}", ["Сборщик", "Машин", "КГ"], rows, out)
    return FileResponse(str(out), filename=out.name)
