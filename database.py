from __future__ import annotations

import json
import sqlite3
from datetime import date
from typing import Any

VALID_FIELDS = {
    "bookid", "title", "author", "status", "last_updated",
    "intro", "tags", "press", "word_count", "animation", "cover",
    "crawl_date",
}
NUMERIC_FIELDS = {"bookid", "word_count", "animation"}
PREFIX_FIELDS = {"last_updated", "crawl_date"}


def init_db(db_path: str = "novels.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS novels (
            bookid       INTEGER PRIMARY KEY,
            title        TEXT,
            author       TEXT,
            status       TEXT,
            last_updated TEXT,
            intro        TEXT,
            tags         TEXT,
            press        TEXT,
            word_count   INTEGER,
            animation    INTEGER,
            cover        TEXT,
            crawl_date   TEXT
        )
    """)
    # 兼容旧库：若 crawl_date 列不存在则自动添加
    existing = {row[1] for row in conn.execute("PRAGMA table_info(novels)")}
    if "crawl_date" not in existing:
        conn.execute("ALTER TABLE novels ADD COLUMN crawl_date TEXT")
    # 高频查询字段建索引（CREATE INDEX IF NOT EXISTS 幂等）
    conn.execute("CREATE INDEX IF NOT EXISTS idx_author ON novels(author)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_title  ON novels(title)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_date ON novels(crawl_date)")
    conn.commit()
    return conn


def _serialize(data: dict) -> dict:
    """将 Python 对象转换为 SQLite 可存储的类型。"""
    row = dict(data)
    if isinstance(row.get("tags"), list):
        row["tags"] = json.dumps(row["tags"], ensure_ascii=False)
    if isinstance(row.get("animation"), bool):
        row["animation"] = 1 if row["animation"] else 0
    return row


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    if d.get("tags"):
        try:
            d["tags"] = json.loads(d["tags"])
        except (json.JSONDecodeError, TypeError):
            pass
    if d.get("animation") is not None:
        d["animation"] = bool(d["animation"])
    return d


def upsert_novel(conn: sqlite3.Connection, data: dict) -> None:
    row = _serialize(data)
    # 始终注入当天日期作为 crawl_date（首次写入=创建时间，再次写入=更新时间）
    row["crawl_date"] = date.today().isoformat()
    columns = ", ".join(row.keys())
    placeholders = ", ".join("?" for _ in row)
    sql = f"INSERT OR REPLACE INTO novels ({columns}) VALUES ({placeholders})"
    conn.execute(sql, list(row.values()))
    conn.commit()


def get_novel_by_id(conn: sqlite3.Connection, bookid: int) -> dict | None:
    cur = conn.execute("SELECT * FROM novels WHERE bookid = ?", (bookid,))
    row = cur.fetchone()
    return _row_to_dict(row) if row else None


def get_all_novels(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute("SELECT * FROM novels ORDER BY bookid")
    return [_row_to_dict(r) for r in cur.fetchall()]


def query_novels(conn: sqlite3.Connection, field: str, value: str) -> list[dict]:
    if field not in VALID_FIELDS:
        raise ValueError(f"不支持的查询字段: {field}，允许的字段: {sorted(VALID_FIELDS)}")

    if field in NUMERIC_FIELDS:
        try:
            sql = f"SELECT * FROM novels WHERE {field} = ?"
            cur = conn.execute(sql, (int(value),))
        except ValueError:
            return []
    elif field in PREFIX_FIELDS:
        sql = f"SELECT * FROM novels WHERE {field} LIKE ?"
        cur = conn.execute(sql, (value + "%",))
    else:
        sql = f"SELECT * FROM novels WHERE {field} LIKE ?"
        cur = conn.execute(sql, ("%" + value + "%",))

    return [_row_to_dict(r) for r in cur.fetchall()]
