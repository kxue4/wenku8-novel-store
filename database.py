from __future__ import annotations

import json
import sqlite3
from datetime import date
from typing import Any

TAG_CATEGORIES: dict[str, list[str]] = {
    "日常系":   ["校园", "青春", "恋爱", "治愈", "群像", "竞技", "音乐", "美食", "旅行", "欢乐向", "经营", "职场", "斗智", "脑洞", "宅文化"],
    "幻想系":   ["穿越", "奇幻", "魔法", "异能", "战斗", "科幻", "机战", "战争", "冒险", "龙傲天"],
    "黑深残":   ["悬疑", "犯罪", "复仇", "黑暗", "猎奇", "惊悚", "间谍", "末日", "游戏", "大逃杀"],
    "人物属性": ["青梅竹马", "妹妹", "女儿", "JK", "JC", "大小姐", "性转", "伪娘", "人外"],
    "特殊属性": ["后宫", "百合", "耽美", "NTR", "女性视角"],
}

VALID_FIELDS = {
    "bookid", "title", "author", "status", "last_updated",
    "intro", "tags", "press", "word_count", "animation",
    "crawl_date",
}
NUMERIC_FIELDS = {"bookid", "word_count", "animation", "status"}
PREFIX_FIELDS  = {"last_updated", "crawl_date"}


def init_db(db_path: str = "novels.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("""
        CREATE TABLE IF NOT EXISTS novels (
            bookid       INTEGER PRIMARY KEY,
            title        TEXT,
            author       TEXT,
            status       INTEGER,  -- 0=连载中, 1=已完结
            last_updated TEXT,
            intro        TEXT,
            tags         TEXT,     -- JSON array，保留方便直接读取
            press        TEXT,
            word_count   INTEGER,
            animation    INTEGER,
            crawl_date   TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS novel_tags (
            bookid  INTEGER NOT NULL,
            tag     TEXT    NOT NULL,
            PRIMARY KEY (bookid, tag),
            FOREIGN KEY (bookid) REFERENCES novels(bookid) ON DELETE CASCADE
        )
    """)

    # ── 自动迁移 ────────────────────────────────────────
    cols = {row[1] for row in conn.execute("PRAGMA table_info(novels)")}

    # 兼容旧库：crawl_date 列不存在则添加
    if "crawl_date" not in cols:
        conn.execute("ALTER TABLE novels ADD COLUMN crawl_date TEXT")

    # 迁移：status TEXT → INTEGER（"已完结"→1, "连载中"→0）
    sample = conn.execute(
        "SELECT status FROM novels WHERE status IS NOT NULL LIMIT 1"
    ).fetchone()
    if sample and isinstance(sample[0], str):
        conn.execute("ALTER TABLE novels ADD COLUMN status_int INTEGER")
        conn.execute("""
            UPDATE novels
            SET status_int = CASE WHEN status = '已完结' THEN 1 ELSE 0 END
        """)
        conn.execute("ALTER TABLE novels DROP COLUMN status")
        conn.execute("ALTER TABLE novels RENAME COLUMN status_int TO status")

    # 迁移：删除冗余 cover 列（可由 bookid 推导）
    cols = {row[1] for row in conn.execute("PRAGMA table_info(novels)")}
    if "cover" in cols:
        conn.execute("ALTER TABLE novels DROP COLUMN cover")

    # 迁移：从 novels.tags JSON 填充 novel_tags（首次建表时执行）
    tag_count = conn.execute("SELECT COUNT(*) FROM novel_tags").fetchone()[0]
    if tag_count == 0:
        rows = conn.execute(
            "SELECT bookid, tags FROM novels WHERE tags IS NOT NULL AND tags != '[]'"
        ).fetchall()
        for row in rows:
            try:
                for tag in json.loads(row[1]):
                    if tag:
                        conn.execute(
                            "INSERT OR IGNORE INTO novel_tags (bookid, tag) VALUES (?, ?)",
                            (row[0], tag),
                        )
            except (json.JSONDecodeError, TypeError):
                pass

    # ── 索引 ────────────────────────────────────────────
    conn.execute("CREATE INDEX IF NOT EXISTS idx_author     ON novels(author)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_title      ON novels(title)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_date ON novels(crawl_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status     ON novels(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tag        ON novel_tags(tag, bookid)")

    conn.commit()
    return conn


def _serialize(data: dict) -> dict:
    """将 Python 对象转换为 SQLite 可存储的类型。"""
    row = {k: v for k, v in data.items() if k != "cover"}   # cover 不再存储
    if isinstance(row.get("tags"), list):
        row["tags"] = json.dumps(row["tags"], ensure_ascii=False)
    if isinstance(row.get("animation"), bool):
        row["animation"] = 1 if row["animation"] else 0
    # status: "已完结"→1, "连载中"→0，整数直接透传
    if isinstance(row.get("status"), str):
        row["status"] = 1 if row["status"] == "已完结" else 0
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
    row["crawl_date"] = date.today().isoformat()
    columns      = ", ".join(row.keys())
    placeholders = ", ".join("?" for _ in row)
    conn.execute(
        f"INSERT OR REPLACE INTO novels ({columns}) VALUES ({placeholders})",
        list(row.values()),
    )
    # 同步 novel_tags
    bookid = data["bookid"]
    tags   = data.get("tags") or []
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except (json.JSONDecodeError, TypeError):
            tags = []
    conn.execute("DELETE FROM novel_tags WHERE bookid = ?", (bookid,))
    for tag in tags:
        if tag:
            conn.execute(
                "INSERT OR IGNORE INTO novel_tags (bookid, tag) VALUES (?, ?)",
                (bookid, tag),
            )
    conn.commit()


def query_novels_by_tag(
    conn: sqlite3.Connection, tags: list[str], match_all: bool = True
) -> list[dict]:
    """按标签查询。match_all=True 为 AND（全部匹配），False 为 OR（任意匹配）。"""
    if not tags:
        return []
    placeholders = ", ".join("?" for _ in tags)
    if match_all:
        sql = f"""
            SELECT n.* FROM novels n
            JOIN novel_tags t ON n.bookid = t.bookid
            WHERE t.tag IN ({placeholders})
            GROUP BY n.bookid
            HAVING COUNT(DISTINCT t.tag) = ?
            ORDER BY n.bookid
        """
        cur = conn.execute(sql, tags + [len(tags)])
    else:
        sql = f"""
            SELECT DISTINCT n.* FROM novels n
            JOIN novel_tags t ON n.bookid = t.bookid
            WHERE t.tag IN ({placeholders})
            ORDER BY n.bookid
        """
        cur = conn.execute(sql, tags)
    return [_row_to_dict(r) for r in cur.fetchall()]


def query_novels_by_category(
    conn: sqlite3.Connection,
    category: str | None,
    extra_tags: list[str] | None = None,
    match_all_extra: bool = True,
) -> list[dict]:
    """
    按一级分类查询（含该分类任意标签即匹配）。

    Args:
        category:        一级分类名，如 "日常系"、"幻想系"，传 None 则不限分类
        extra_tags:      额外的标签约束（二级精确过滤），如 ["后宫", "百合"]
        match_all_extra: True = extra_tags 全部命中（AND），False = 任意命中（OR）

    示例:
        # 所有日常系
        query_novels_by_category(conn, "日常系")
        # 日常系 AND 后宫
        query_novels_by_category(conn, "日常系", extra_tags=["后宫"])
        # 日常系 AND (百合 OR 耽美)
        query_novels_by_category(conn, "日常系", extra_tags=["百合", "耽美"], match_all_extra=False)
    """
    if category is not None and category not in TAG_CATEGORIES:
        raise ValueError(f"未知分类: {category}，可用分类: {list(TAG_CATEGORIES.keys())}")

    category_tags = TAG_CATEGORIES.get(category, []) if category else []
    extra_tags    = extra_tags or []

    if not category_tags and not extra_tags:
        return get_all_novels(conn)

    params: list[Any] = []
    conditions: list[str] = []

    if category_tags:
        ph = ", ".join("?" for _ in category_tags)
        conditions.append(f"""
            EXISTS (
                SELECT 1 FROM novel_tags t1
                WHERE t1.bookid = n.bookid AND t1.tag IN ({ph})
            )
        """)
        params.extend(category_tags)

    if extra_tags:
        if match_all_extra:
            for tag in extra_tags:
                conditions.append("""
                    EXISTS (
                        SELECT 1 FROM novel_tags t2
                        WHERE t2.bookid = n.bookid AND t2.tag = ?
                    )
                """)
                params.append(tag)
        else:
            ph = ", ".join("?" for _ in extra_tags)
            conditions.append(f"""
                EXISTS (
                    SELECT 1 FROM novel_tags t2
                    WHERE t2.bookid = n.bookid AND t2.tag IN ({ph})
                )
            """)
            params.extend(extra_tags)

    sql = f"SELECT DISTINCT n.* FROM novels n WHERE {' AND '.join(conditions)} ORDER BY n.bookid"
    cur = conn.execute(sql, params)
    return [_row_to_dict(r) for r in cur.fetchall()]


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
