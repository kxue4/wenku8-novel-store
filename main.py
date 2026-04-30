from __future__ import annotations

import argparse
import asyncio
import os

from dotenv import load_dotenv

from database import init_db, upsert_novel
from wenku8.client import Wenku8Client, warmup

DB_PATH = "novels.db"


async def run(ids: list[int], names: list[str]) -> None:
    load_dotenv()
    username = os.environ.get("WENKU8_USERNAME", "")
    password = os.environ.get("WENKU8_PASSWORD", "")

    if not username or not password:
        print("[错误] 请在 .env 中配置 WENKU8_USERNAME 和 WENKU8_PASSWORD")
        return

    client = Wenku8Client()
    conn = init_db(DB_PATH)
    try:
        print("[1/3] 正在登录...")
        await client.login(username, password)
        print("[1/3] 登录完成")

        print("[2/3] CF 暖机中...")
        await warmup(client)
        print("[2/3] 暖机完成")

        print(f"[3/3] 开始抓取，共 {len(ids) + len(names)} 条任务")

        # 按 ID 抓取
        for aid in ids:
            print(f"  → 抓取 id={aid} ...", end=" ", flush=True)
            info = await client.get_novel_info(aid)
            if info is None:
                print("失败（返回 None）")
                continue
            upsert_novel(conn, info)
            title = info.get("title") or "（无标题）"
            author = info.get("author") or "（未知）"
            wc = info.get("word_count")
            print(f"OK  《{title}》/ {author} / 字数={wc}")

        # 按名称搜索后抓取
        for name in names:
            print(f"  → 搜索「{name}」...", end=" ", flush=True)
            aid = await client.search_by_name(name)
            if aid is None:
                print("未找到结果")
                continue
            print(f"找到 aid={aid}，正在抓取详情...", end=" ", flush=True)
            info = await client.get_novel_info(aid)
            if info is None:
                print("详情获取失败")
                continue
            upsert_novel(conn, info)
            title = info.get("title") or "（无标题）"
            author = info.get("author") or "（未知）"
            print(f"OK  《{title}》/ {author}")
    finally:
        conn.close()
        await client.close()

    print("\n全部完成，数据已写入", DB_PATH)


def main() -> None:
    parser = argparse.ArgumentParser(description="抓取轻小说文库书籍元数据并存入 SQLite")
    parser.add_argument(
        "--id",
        dest="ids",
        default="",
        help="书籍 ID，多个用逗号分隔，如 --id 1,500,1159",
    )
    parser.add_argument(
        "--name",
        dest="names",
        action="append",
        default=[],
        help="书名关键词（可多次使用），如 --name 魔法科高校 --name 禁书目录",
    )
    args = parser.parse_args()

    id_list = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
    name_list = [n.strip() for n in args.names if n.strip()]

    if not id_list and not name_list:
        parser.print_help()
        return

    asyncio.run(run(id_list, name_list))


if __name__ == "__main__":
    main()
