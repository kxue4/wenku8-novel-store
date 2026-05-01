"""
batch_crawl.py — 批量抓取 wenku8 书籍元数据（长期维护版）

用法示例:
    python3 batch_crawl.py --start 1 --end 1000
    python3 batch_crawl.py --start 1001 --end 2000 --delay-ok 2.0
    python3 batch_crawl.py --start 1 --end 1000  # 自动跳过已抓取条目
    python3 batch_crawl.py --full                 # 全表更新

crawl_log.txt 列定义（TSV）:
    aid  status  crawl_date  novel_status  title  author  error

跳过规则（每次执行前自动应用）:
    - status=OK 且 novel_status=1（已完结）→ 永久跳过
    - status=OK 且 novel_status=0（连载中）且距今 < 30 天 → 跳过
    - status=FAIL / 距今 ≥ 30 天的连载书 → 重新抓取
"""
from __future__ import annotations

import argparse
import asyncio
import os
import time
from datetime import date, datetime

from dotenv import load_dotenv

from database import init_db, upsert_novel
from wenku8.client import Wenku8Client, warmup

# ── 默认配置 ─────────────────────────────────────────
DEFAULT_DELAY_OK    = 1.5   # 成功后间隔（秒）
DEFAULT_DELAY_FAIL  = 3.0   # 失败后间隔（秒）
DEFAULT_DELAY_BURST = 10.0  # 连续 5 次失败后冷却（秒）
CONSEC_FAIL_THRESHOLD = 5

DB_PATH  = "novels.db"
LOG_PATH = "crawl_log.txt"

LOG_HEADER = "# wenku8 crawl log — maintained by batch_crawl.py\n"
LOG_COLS   = "# aid\tstatus\tcrawl_date\tnovel_status\ttitle\tauthor\terror\n"
# ─────────────────────────────────────────────────────


# ════════════════════════════════════════════════════
#  日志读写
# ════════════════════════════════════════════════════

def _load_log() -> dict[int, dict]:
    """
    加载 crawl_log.txt → dict[aid, entry]。
    同一 aid 出现多次时保留最后一条（最新记录）。
    """
    entries: dict[int, dict] = {}
    if not os.path.exists(LOG_PATH):
        return entries
    with open(LOG_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 7:
                parts += [""] * (7 - len(parts))
            try:
                aid = int(parts[0])
            except ValueError:
                continue
            entries[aid] = {
                "aid":          aid,
                "status":       parts[1],
                "crawl_date":   parts[2],
                "novel_status": parts[3],   # "1" / "0" / ""
                "title":        parts[4],
                "author":       parts[5],
                "error":        parts[6],
            }
    return entries


def _write_log(entries: dict[int, dict]) -> None:
    """将内存中的全量记录按 aid 排序原子写回 crawl_log.txt（先写临时文件再 rename，防崩溃损坏）。"""
    tmp = LOG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(LOG_HEADER)
        f.write(LOG_COLS)
        for aid in sorted(entries):
            e = entries[aid]
            f.write(
                f"{e['aid']}\t{e['status']}\t{e['crawl_date']}\t"
                f"{e['novel_status']}\t{e['title']}\t{e['author']}\t{e['error']}\n"
            )
    os.replace(tmp, LOG_PATH)


def _should_skip(entry: dict, today: date) -> bool:
    if entry["status"] != "OK":
        return False
    ns = entry.get("novel_status", "")
    if ns == "1":
        return True
    if ns == "0":
        cd_str = entry.get("crawl_date", "")
        if cd_str:
            try:
                delta = today - date.fromisoformat(cd_str)
                if delta.days < 30:
                    return True
            except ValueError:
                pass
    return False


# ════════════════════════════════════════════════════
#  主逻辑
# ════════════════════════════════════════════════════

async def run(
    start: int | None,
    end: int | None,
    full: bool,
    delay_ok: float,
    delay_fail: float,
    delay_burst: float,
) -> None:
    load_dotenv()
    username = os.environ.get("WENKU8_USERNAME", "")
    password = os.environ.get("WENKU8_PASSWORD", "")
    if not username or not password:
        print("[错误] 请在 .env 中配置 WENKU8_USERNAME 和 WENKU8_PASSWORD")
        return

    today  = date.today()
    log    = _load_log()
    client = Wenku8Client()
    conn   = init_db(DB_PATH)

    # ── --full 模式：自动取最大 bookid ───────────────
    if full:
        print(f"[{_ts()}] 登录中...")
        await client.login(username, password)
        print(f"[{_ts()}] CF 暖机中...")
        await warmup(client)
        print(f"[{_ts()}] 获取最新上架 bookid...")
        latest = await client.get_latest_bookid()
        if not latest:
            print("[错误] 无法获取最新 bookid，请检查网络")
            conn.close()
            await client.close()
            return
        start, end = 1, latest
        print(f"[{_ts()}] 全表更新模式：范围 1~{latest}")
        already_logged_in = True
    else:
        already_logged_in = False

    # ── 计算实际需要抓取的 aid 列表 ──────────────────
    to_crawl: list[int] = []
    skipped  = 0
    for aid in range(start, end + 1):
        if aid in log and _should_skip(log[aid], today):
            skipped += 1
        else:
            to_crawl.append(aid)

    print(f"[{_ts()}] 范围 {start}~{end}，共 {end - start + 1} 条")
    print(f"[{_ts()}] 跳过 {skipped} 条，待抓取 {len(to_crawl)} 条")

    if not to_crawl:
        print("[完成] 全部条目均已是最新，无需抓取。")
        conn.close()
        await client.close()
        return

    # ── 登录 + 暖机 ──────────────────────────────────
    if not already_logged_in:
        print(f"[{_ts()}] 登录中...")
        await client.login(username, password)
        print(f"[{_ts()}] CF 暖机中...")
        await warmup(client)
    print(f"[{_ts()}] 暖机完成，开始抓取")

    ok_count    = 0
    fail_count  = 0
    consec_fail = 0
    LOG_FLUSH_EVERY = 10   # 每抓 N 条写一次日志（最后一条也会写）

    try:
        for i, aid in enumerate(to_crawl, 1):
            print(f"[{_ts()}] [{i}/{len(to_crawl)}] aid={aid} 抓取中...", end=" ", flush=True)
            t0 = time.monotonic()

            try:
                info = await client.get_novel_info(aid)
                err_msg = ""
            except Exception as e:
                info    = None
                err_msg = str(e)[:100]

            elapsed = time.monotonic() - t0

            if info and info.get("title"):
                upsert_novel(conn, info)
                title        = info.get("title", "")
                author       = info.get("author", "")
                novel_status = "1" if info.get("status") == "已完结" else "0"
                print(f"OK  《{title}》/ {author}  ({elapsed:.1f}s)")

                log[aid] = {
                    "aid":          aid,
                    "status":       "OK",
                    "crawl_date":   today.isoformat(),
                    "novel_status": novel_status,
                    "title":        title,
                    "author":       author,
                    "error":        "",
                }
                ok_count    += 1
                consec_fail  = 0
                delay        = delay_ok
            else:
                reason = err_msg or "返回空数据"
                print(f"FAIL  {reason}  ({elapsed:.1f}s)")

                log[aid] = {
                    "aid":          aid,
                    "status":       "FAIL",
                    "crawl_date":   today.isoformat(),
                    "novel_status": "",
                    "title":        "",
                    "author":       "",
                    "error":        reason,
                }
                fail_count  += 1
                consec_fail += 1
                delay        = delay_fail

            # 每 LOG_FLUSH_EVERY 条或最后一条写回磁盘
            if i % LOG_FLUSH_EVERY == 0 or i == len(to_crawl):
                _write_log(log)

            if consec_fail >= CONSEC_FAIL_THRESHOLD:
                print(f"[{_ts()}] 连续失败 {consec_fail} 次，冷却 {delay_burst}s...")
                await asyncio.sleep(delay_burst)
                consec_fail = 0
            else:
                await asyncio.sleep(delay)

            # 每 300 条主动刷新一次 CF session，防止长时间爬取中 session 过期
            if i % 300 == 0:
                print(f"[{_ts()}] 定期刷新 CF session（第 {i} 条）...")
                await warmup(client)
    finally:
        _write_log(log)   # 确保中断时也落盘
        conn.close()
        await client.close()
    print("\n" + "=" * 50)
    print(f"完成！成功 {ok_count} 条，失败 {fail_count} 条，跳过 {skipped} 条")
    print(f"数据库: {DB_PATH}  |  日志: {LOG_PATH}")


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ════════════════════════════════════════════════════
#  CLI
# ════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="批量抓取 wenku8 书籍元数据",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--start",       type=int, default=None,               help="起始 aid（含），--full 时忽略")
    parser.add_argument("--end",         type=int, default=None,               help="结束 aid（含），--full 时忽略")
    parser.add_argument("--full",        action="store_true",                  help="全表更新：自动获取最大 bookid，从 1 跑到最新")
    parser.add_argument("--delay-ok",    type=float, default=DEFAULT_DELAY_OK,    help="成功后间隔秒数")
    parser.add_argument("--delay-fail",  type=float, default=DEFAULT_DELAY_FAIL,  help="失败后间隔秒数")
    parser.add_argument("--delay-burst", type=float, default=DEFAULT_DELAY_BURST, help="连续失败冷却秒数")
    args = parser.parse_args()

    if not args.full and (args.start is None or args.end is None):
        parser.error("必须指定 --start 和 --end，或使用 --full 模式")

    asyncio.run(run(
        start       = args.start,
        end         = args.end,
        full        = args.full,
        delay_ok    = args.delay_ok,
        delay_fail  = args.delay_fail,
        delay_burst = args.delay_burst,
    ))
