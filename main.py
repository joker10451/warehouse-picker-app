import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import flet as ft
from openpyxl import Workbook
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Spacer, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet


APP_DIR = Path(__file__).parent
DATA_DIR = APP_DIR / "data"
EXPORTS_DIR = APP_DIR / "exports"
PRINTS_DIR = APP_DIR / "prints"
DB_PATH = DATA_DIR / "warehouse.db"

WORK_TYPES = ["сборка", "упаковка", "приемка"]


def ensure_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    EXPORTS_DIR.mkdir(exist_ok=True)
    PRINTS_DIR.mkdir(exist_ok=True)


@dataclass
class LogEntry:
    work_date: str
    picker: str
    order_number: str
    work_type: str
    quantity: int
    comment: str


class Storage:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS work_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_date TEXT NOT NULL,
                picker TEXT NOT NULL,
                order_number TEXT NOT NULL,
                work_type TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK(quantity > 0),
                comment TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pickers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            );
            """
        )
        self.conn.commit()
        self._seed_defaults()

    def _seed_defaults(self) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM pickers")
        if cur.fetchone()["cnt"] == 0:
            for name in ["Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов"]:
                cur.execute("INSERT INTO pickers(name) VALUES(?)", (name,))
            self.conn.commit()

    def list_pickers(self) -> list[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM pickers ORDER BY name")
        return [r["name"] for r in cur.fetchall()]

    def add_picker(self, name: str) -> None:
        cur = self.conn.cursor()
        cur.execute("INSERT OR IGNORE INTO pickers(name) VALUES(?)", (name.strip(),))
        self.conn.commit()

    def add_log(self, entry: LogEntry) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO work_logs(work_date, picker, order_number, work_type, quantity, comment, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.work_date,
                entry.picker,
                entry.order_number,
                entry.work_type,
                entry.quantity,
                entry.comment,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        self.conn.commit()

    def query_logs(
        self,
        work_date: str = "",
        picker: str = "",
        work_type: str = "",
        order_query: str = "",
    ) -> list[sqlite3.Row]:
        sql = """
        SELECT id, work_date, picker, order_number, work_type, quantity, comment, created_at
        FROM work_logs
        WHERE 1=1
        """
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
        if order_query:
            sql += " AND order_number LIKE ?"
            args.append(f"%{order_query}%")
        sql += " ORDER BY work_date DESC, id DESC LIMIT 5000"
        cur = self.conn.cursor()
        cur.execute(sql, args)
        return cur.fetchall()

    def stats_by_day(self, day: str) -> list[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT picker,
                   COUNT(DISTINCT order_number) AS orders_count,
                   SUM(quantity) AS total_qty
            FROM work_logs
            WHERE work_date = ?
            GROUP BY picker
            ORDER BY picker
            """,
            (day,),
        )
        return cur.fetchall()

    def stats_by_period(self, date_from: str, date_to: str) -> list[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT picker,
                   COUNT(DISTINCT order_number) AS orders_count,
                   SUM(quantity) AS total_qty
            FROM work_logs
            WHERE work_date BETWEEN ? AND ?
            GROUP BY picker
            ORDER BY picker
            """,
            (date_from, date_to),
        )
        return cur.fetchall()

    def total_for_period(self, date_from: str, date_to: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT COALESCE(SUM(quantity), 0) AS total_qty FROM work_logs WHERE work_date BETWEEN ? AND ?",
            (date_from, date_to),
        )
        return int(cur.fetchone()["total_qty"])

    def by_work_type_for_period(self, date_from: str, date_to: str) -> list[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT work_type, COALESCE(SUM(quantity), 0) AS total_qty
            FROM work_logs
            WHERE work_date BETWEEN ? AND ?
            GROUP BY work_type
            ORDER BY work_type
            """,
            (date_from, date_to),
        )
        return cur.fetchall()


def register_cyrillic_font() -> Optional[str]:
    candidates = [
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/tahoma.ttf"),
        Path("C:/Windows/Fonts/calibri.ttf"),
    ]
    for font_path in candidates:
        if font_path.exists():
            name = "WarehouseRU"
            pdfmetrics.registerFont(TTFont(name, str(font_path)))
            return name
    return None


def export_excel(rows: list[sqlite3.Row], output_path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Журнал"
    headers = ["Дата", "Сборщик", "Номер заказа", "Тип работы", "Количество", "Комментарий"]
    ws.append(headers)
    for row in rows:
        ws.append(
            [
                row["work_date"],
                row["picker"],
                row["order_number"],
                row["work_type"],
                row["quantity"],
                row["comment"],
            ]
        )
    for idx, width in enumerate([14, 18, 18, 16, 12, 40], start=1):
        ws.column_dimensions[chr(64 + idx)].width = width
    wb.save(output_path)


def export_pdf(title: str, headers: list[str], data: list[list[str]], output_path: Path) -> None:
    font_name = register_cyrillic_font() or "Helvetica"
    doc = SimpleDocTemplate(str(output_path), pagesize=landscape(A4), leftMargin=12 * mm, rightMargin=12 * mm)
    styles = getSampleStyleSheet()
    styles["Normal"].fontName = font_name
    styles["Title"].fontName = font_name
    elements = [
        Paragraph(title, styles["Title"]),
        Spacer(1, 5 * mm),
    ]
    table_data = [headers] + data
    table = Table(table_data, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F4E78")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
            ]
        )
    )
    elements.append(table)
    doc.build(elements)


class WarehouseApp:
    def __init__(self, page: ft.Page, storage: Storage):
        self.page = page
        self.storage = storage

        self.page.title = "Учет работы складских сборщиков"
        self.page.window_width = 1280
        self.page.window_height = 780
        self.page.padding = 16

        today_iso = date.today().isoformat()

        self.input_date = ft.TextField(label="Дата (YYYY-MM-DD)", value=today_iso, width=200)
        self.input_picker = ft.Dropdown(label="Сборщик", width=220, options=[])
        self.input_order = ft.TextField(label="Номер заказа", width=220)
        self.input_type = ft.Dropdown(label="Тип работы", width=200, options=[ft.dropdown.Option(x) for x in WORK_TYPES])
        self.input_quantity = ft.TextField(label="Количество", width=160, value="1")
        self.input_comment = ft.TextField(label="Комментарий", width=420)
        self.new_picker_field = ft.TextField(label="Новый сборщик", width=220)

        self.filter_date = ft.TextField(label="Дата (YYYY-MM-DD)", width=180)
        self.filter_picker = ft.Dropdown(label="Сборщик", width=180, options=[])
        self.filter_type = ft.Dropdown(label="Тип", width=180, options=[ft.dropdown.Option("")] + [ft.dropdown.Option(x) for x in WORK_TYPES])
        self.filter_order = ft.TextField(label="Поиск заказа", width=200)
        self.journal_table = ft.DataTable(columns=[], rows=[], heading_row_color=ft.colors.BLUE_GREY_100)

        self.stats_day = ft.TextField(label="За день (YYYY-MM-DD)", value=today_iso, width=200)
        self.stats_from = ft.TextField(label="Период с", value=today_iso, width=200)
        self.stats_to = ft.TextField(label="Период по", value=today_iso, width=200)
        self.stats_day_table = ft.DataTable(columns=[], rows=[])
        self.stats_period_table = ft.DataTable(columns=[], rows=[])
        self.stats_types_table = ft.DataTable(columns=[], rows=[])
        self.total_label = ft.Text("Общий объем: 0", size=16, weight=ft.FontWeight.BOLD)

        self._refresh_pickers()
        self._build_layout()
        self.refresh_journal()
        self.refresh_stats()

    def _show_msg(self, text: str, error: bool = False) -> None:
        self.page.snack_bar = ft.SnackBar(ft.Text(text), bgcolor=ft.colors.RED_400 if error else ft.colors.GREEN_600)
        self.page.snack_bar.open = True
        self.page.update()

    def _refresh_pickers(self) -> None:
        options = [ft.dropdown.Option("")] + [ft.dropdown.Option(p) for p in self.storage.list_pickers()]
        self.input_picker.options = options[1:]
        self.filter_picker.options = options

    def _build_layout(self) -> None:
        input_tab = ft.Tab(
            text="Ввод данных",
            content=ft.Column(
                controls=[
                    ft.Text("Быстрый ввод работы сборщика", size=20, weight=ft.FontWeight.BOLD),
                    ft.Row([self.input_date, self.input_picker, self.input_order]),
                    ft.Row([self.input_type, self.input_quantity, self.input_comment]),
                    ft.Row(
                        [
                            ft.ElevatedButton("Сохранить", icon=ft.icons.SAVE, on_click=self.on_save_log),
                            ft.OutlinedButton("Очистить", on_click=self.on_clear_form),
                            ft.ElevatedButton("Печать сменного отчета", icon=ft.icons.PRINT, on_click=self.on_print_stats_pdf),
                        ]
                    ),
                    ft.Divider(),
                    ft.Text("Справочник сборщиков", weight=ft.FontWeight.BOLD),
                    ft.Row(
                        [
                            self.new_picker_field,
                            ft.ElevatedButton("Добавить сборщика", on_click=self.on_add_picker),
                        ]
                    ),
                    ft.Text(
                        "Инструкция: заполните форму -> Сохранить -> журнал и статистика обновятся автоматически.",
                        size=13,
                    ),
                ],
                scroll=ft.ScrollMode.AUTO,
            ),
        )

        self.journal_table.columns = [
            ft.DataColumn(ft.Text("Дата")),
            ft.DataColumn(ft.Text("Сборщик")),
            ft.DataColumn(ft.Text("Заказ")),
            ft.DataColumn(ft.Text("Тип")),
            ft.DataColumn(ft.Text("Кол-во")),
            ft.DataColumn(ft.Text("Комментарий")),
        ]
        journal_tab = ft.Tab(
            text="Журнал",
            content=ft.Column(
                controls=[
                    ft.Row(
                        [
                            self.filter_date,
                            self.filter_picker,
                            self.filter_type,
                            self.filter_order,
                            ft.ElevatedButton("Применить фильтры", on_click=lambda e: self.refresh_journal()),
                            ft.OutlinedButton("Сброс", on_click=self.on_reset_filters),
                        ]
                    ),
                    ft.Row(
                        [
                            ft.ElevatedButton("Экспорт Excel", icon=ft.icons.DOWNLOAD, on_click=self.on_export_excel),
                            ft.ElevatedButton("Экспорт PDF", icon=ft.icons.PICTURE_AS_PDF, on_click=self.on_export_journal_pdf),
                            ft.ElevatedButton("Печать", icon=ft.icons.PRINT, on_click=self.on_export_journal_pdf),
                        ]
                    ),
                    ft.Container(content=ft.Column([self.journal_table], scroll=ft.ScrollMode.AUTO), height=560),
                ]
            ),
        )

        self.stats_day_table.columns = [
            ft.DataColumn(ft.Text("Сборщик")),
            ft.DataColumn(ft.Text("Заказов за день")),
            ft.DataColumn(ft.Text("Объем за день")),
        ]
        self.stats_period_table.columns = [
            ft.DataColumn(ft.Text("Сборщик")),
            ft.DataColumn(ft.Text("Заказов за период")),
            ft.DataColumn(ft.Text("Объем за период")),
        ]
        self.stats_types_table.columns = [
            ft.DataColumn(ft.Text("Тип работы")),
            ft.DataColumn(ft.Text("Объем")),
        ]

        stats_tab = ft.Tab(
            text="Статистика",
            content=ft.Column(
                controls=[
                    ft.Row(
                        [
                            self.stats_day,
                            self.stats_from,
                            self.stats_to,
                            ft.ElevatedButton("Обновить", on_click=lambda e: self.refresh_stats()),
                        ]
                    ),
                    self.total_label,
                    ft.Text("За день", weight=ft.FontWeight.BOLD),
                    self.stats_day_table,
                    ft.Divider(),
                    ft.Text("За период", weight=ft.FontWeight.BOLD),
                    self.stats_period_table,
                    ft.Divider(),
                    ft.Text("Объем по типам работ", weight=ft.FontWeight.BOLD),
                    self.stats_types_table,
                    ft.Row(
                        [
                            ft.ElevatedButton("Экспорт PDF", icon=ft.icons.PICTURE_AS_PDF, on_click=self.on_print_stats_pdf),
                            ft.ElevatedButton("Печать", icon=ft.icons.PRINT, on_click=self.on_print_stats_pdf),
                        ]
                    ),
                ],
                scroll=ft.ScrollMode.AUTO,
            ),
        )

        self.page.add(ft.Tabs(tabs=[input_tab, journal_tab, stats_tab], expand=1))

    def _validate_date(self, value: str) -> bool:
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def on_add_picker(self, e: ft.ControlEvent) -> None:
        name = self.new_picker_field.value.strip()
        if not name:
            self._show_msg("Введите имя сборщика", error=True)
            return
        self.storage.add_picker(name)
        self.new_picker_field.value = ""
        self._refresh_pickers()
        self.page.update()
        self._show_msg("Сборщик добавлен")

    def on_clear_form(self, e: ft.ControlEvent) -> None:
        self.input_date.value = date.today().isoformat()
        self.input_picker.value = None
        self.input_order.value = ""
        self.input_type.value = None
        self.input_quantity.value = "1"
        self.input_comment.value = ""
        self.page.update()

    def on_save_log(self, e: ft.ControlEvent) -> None:
        if not self._validate_date(self.input_date.value):
            self._show_msg("Дата должна быть в формате YYYY-MM-DD", error=True)
            return
        if not self.input_picker.value:
            self._show_msg("Выберите сборщика", error=True)
            return
        if not self.input_order.value.strip():
            self._show_msg("Введите номер заказа", error=True)
            return
        if not self.input_type.value:
            self._show_msg("Выберите тип работы", error=True)
            return
        try:
            qty = int(self.input_quantity.value)
            if qty <= 0:
                raise ValueError
        except ValueError:
            self._show_msg("Количество должно быть целым числом > 0", error=True)
            return

        entry = LogEntry(
            work_date=self.input_date.value.strip(),
            picker=self.input_picker.value,
            order_number=self.input_order.value.strip(),
            work_type=self.input_type.value,
            quantity=qty,
            comment=self.input_comment.value.strip(),
        )
        self.storage.add_log(entry)
        self.refresh_journal()
        self.refresh_stats()
        self._show_msg("Запись сохранена")

    def on_reset_filters(self, e: ft.ControlEvent) -> None:
        self.filter_date.value = ""
        self.filter_picker.value = ""
        self.filter_type.value = ""
        self.filter_order.value = ""
        self.refresh_journal()

    def refresh_journal(self) -> None:
        rows = self.storage.query_logs(
            work_date=self.filter_date.value.strip(),
            picker=(self.filter_picker.value or "").strip(),
            work_type=(self.filter_type.value or "").strip(),
            order_query=self.filter_order.value.strip(),
        )
        self.journal_table.rows = [
            ft.DataRow(
                cells=[
                    ft.DataCell(ft.Text(r["work_date"])),
                    ft.DataCell(ft.Text(r["picker"])),
                    ft.DataCell(ft.Text(r["order_number"])),
                    ft.DataCell(ft.Text(r["work_type"])),
                    ft.DataCell(ft.Text(str(r["quantity"]))),
                    ft.DataCell(ft.Text(r["comment"])),
                ]
            )
            for r in rows
        ]
        self.page.update()

    def refresh_stats(self) -> None:
        if not (self._validate_date(self.stats_day.value) and self._validate_date(self.stats_from.value) and self._validate_date(self.stats_to.value)):
            self._show_msg("Проверьте даты статистики (YYYY-MM-DD)", error=True)
            return

        day_rows = self.storage.stats_by_day(self.stats_day.value)
        period_rows = self.storage.stats_by_period(self.stats_from.value, self.stats_to.value)
        type_rows = self.storage.by_work_type_for_period(self.stats_from.value, self.stats_to.value)
        total_qty = self.storage.total_for_period(self.stats_from.value, self.stats_to.value)

        self.stats_day_table.rows = [
            ft.DataRow(
                cells=[ft.DataCell(ft.Text(r["picker"])), ft.DataCell(ft.Text(str(r["orders_count"] or 0))), ft.DataCell(ft.Text(str(r["total_qty"] or 0)))]
            )
            for r in day_rows
        ]
        self.stats_period_table.rows = [
            ft.DataRow(
                cells=[ft.DataCell(ft.Text(r["picker"])), ft.DataCell(ft.Text(str(r["orders_count"] or 0))), ft.DataCell(ft.Text(str(r["total_qty"] or 0)))]
            )
            for r in period_rows
        ]
        self.stats_types_table.rows = [
            ft.DataRow(cells=[ft.DataCell(ft.Text(r["work_type"])), ft.DataCell(ft.Text(str(r["total_qty"] or 0)))]) for r in type_rows
        ]
        self.total_label.value = f"Общий объем: {total_qty}"
        self.page.update()

    def _current_journal_rows(self) -> list[sqlite3.Row]:
        return self.storage.query_logs(
            work_date=self.filter_date.value.strip(),
            picker=(self.filter_picker.value or "").strip(),
            work_type=(self.filter_type.value or "").strip(),
            order_query=self.filter_order.value.strip(),
        )

    def on_export_excel(self, e: ft.ControlEvent) -> None:
        rows = self._current_journal_rows()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = EXPORTS_DIR / f"journal_{ts}.xlsx"
        export_excel(rows, out)
        self._show_msg(f"Excel сохранен: {out.name}")

    def on_export_journal_pdf(self, e: ft.ControlEvent) -> None:
        rows = self._current_journal_rows()
        data = [[r["work_date"], r["picker"], r["order_number"], r["work_type"], str(r["quantity"]), r["comment"]] for r in rows]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = PRINTS_DIR / f"journal_{ts}.pdf"
        export_pdf("Журнал работ (A4)", ["Дата", "Сборщик", "Заказ", "Тип", "Кол-во", "Комментарий"], data, out)
        self._show_msg(f"PDF сохранен: {out.name}")

    def on_print_stats_pdf(self, e: ft.ControlEvent) -> None:
        if not (self._validate_date(self.stats_from.value) and self._validate_date(self.stats_to.value)):
            self._show_msg("Проверьте даты периода", error=True)
            return
        rows = self.storage.stats_by_period(self.stats_from.value, self.stats_to.value)
        data = [[r["picker"], str(r["orders_count"] or 0), str(r["total_qty"] or 0)] for r in rows]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = PRINTS_DIR / f"stats_{ts}.pdf"
        export_pdf(
            f"Статистика за период {self.stats_from.value} - {self.stats_to.value}",
            ["Сборщик", "Заказов", "Объем"],
            data,
            out,
        )
        self._show_msg(f"PDF сохранен: {out.name}")


def main(page: ft.Page) -> None:
    ensure_dirs()
    storage = Storage(DB_PATH)
    WarehouseApp(page, storage)


if __name__ == "__main__":
    ft.app(target=main)
