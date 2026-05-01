# wenku8-novel-store

> 本项目基于 [pywenku8api](https://github.com/WorldObservationLog/pywenku8api) 二次开发，提取并重写了其核心 HTTP 客户端与 Cloudflare 绕过模块，无需安装原始包即可独立运行。
>
> 完全使用Claude Sonnet 4.6进行vibe coding

轻小说文库（wenku8.net）书籍元数据抓取工具。支持按书名或书籍 ID 抓取元数据并存入本地 SQLite。

---

## 功能

- 根据书籍 **ID** 或**书名关键词**抓取元数据
- 自动绕过 Cloudflare 防火墙（基于 zendriver 无头浏览器）
- 版权下架书籍（如 id=1《文学少女》）同样可正常抓取
- 数据**明文存储**于本地 SQLite，可用任意数据库工具查看
- 批量抓取支持智能跳过（已完结永久跳过，连载中 30 天内跳过）
- 每次抓取结果记录于 `crawl_log.txt`，长期维护
- 支持按单个或多个标签检索书籍

---

## 项目结构

```
wenku8-novel-store/
├── wenku8/
│   ├── __init__.py
│   ├── cf_solver.py      # Cloudflare 绕过（zendriver，源自 pywenku8api）
│   └── client.py         # 轻量 HTTP 客户端（login / get_novel_info / search_by_name）
├── database.py           # SQLite CRUD
├── main.py               # CLI 单条/少量抓取入口
├── batch_crawl.py        # 批量抓取（含跳过逻辑、日志管理）
├── requirements.txt
└── .env                  # 账号配置
```

---

## 数据字段

数据库包含两张表：

### `novels` 表（书籍主表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `bookid` | INTEGER | 书籍 ID（主键）|
| `title` | TEXT | 书名 |
| `author` | TEXT | 作者 |
| `status` | INTEGER | 连载状态（0=连载中，1=已完结）|
| `last_updated` | TEXT | 最后更新日期 YYYY-MM-DD |
| `intro` | TEXT | 简介 |
| `tags` | TEXT | 标签（JSON 数组字符串，冗余保留方便直接读取）|
| `press` | TEXT | 文库分类 |
| `word_count` | INTEGER | 总字数 |
| `animation` | INTEGER | 是否动画化（0/1）|
| `crawl_date` | TEXT | 最近抓取/更新日期 YYYY-MM-DD |

### `novel_tags` 表（标签关联表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `bookid` | INTEGER | 书籍 ID（外键 → novels.bookid）|
| `tag` | TEXT | 单个标签值 |

> `(bookid, tag)` 为联合主键；`(tag, bookid)` 有索引，支持高效的标签检索。

> `cover` 字段已移除，封面图 URL 可通过 `bookid` 直接推导（见下方）。
>
> 版权下架书籍的 `last_updated` / `word_count` 可能为 NULL，其余字段正常返回。

### 封面图 URL 拼接规则

封面图不存储在数据库中，需要时按以下规则拼接：

```
https://img.wenku8.com/image/{aid // 1000}/{aid}/{aid}s.jpg
```

示例：

| bookid | 封面图 URL |
|--------|-----------|
| 1 | `https://img.wenku8.com/image/0/1/1s.jpg` |
| 500 | `https://img.wenku8.com/image/0/500/500s.jpg` |
| 1159 | `https://img.wenku8.com/image/1/1159/1159s.jpg` |

Python 代码：

```python
def cover_url(bookid: int) -> str:
    return f"https://img.wenku8.com/image/{bookid // 1000}/{bookid}/{bookid}s.jpg"
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置账号

创建 `.env` 文件：

```env
WENKU8_USERNAME=你的账号
WENKU8_PASSWORD=你的密码
```

### 3. 单条/少量抓取

```bash
# 按 ID 抓取（支持逗号分隔批量）
python main.py --id 1,500,1159

# 按书名搜索后抓取（自动取第一条结果）
python main.py --name "魔法科高校"

# 混合使用
python main.py --id 500 --name "禁书目录"
```

首次运行会触发 Cloudflare 暖机，耗时约 30s，后续同 session 请求速度正常。

### 4. 批量抓取

```bash
# 抓取 aid 1~1000
python3 batch_crawl.py --start 1 --end 1000

# 自定义间隔（单位：秒）
python3 batch_crawl.py --start 1001 --end 2000 --delay-ok 2.0 --delay-fail 5.0

# 再次运行同一范围：自动跳过已完结书籍和近 30 天内已抓取的连载书
python3 batch_crawl.py --start 1 --end 1000

# 全表更新：自动获取 wenku8 最新 bookid，从 1 跑到最新（配合跳过逻辑，只抓新书和需要更新的连载）
python3 batch_crawl.py --full
```

### 5. 标签查询

```python
from database import init_db, query_novels_by_tag

conn = init_db("novels.db")

# 单标签
results = query_novels_by_tag(conn, ["校园"])

# 多标签 AND（同时包含所有标签）
results = query_novels_by_tag(conn, ["校园", "恋爱"], match_all=True)

# 多标签 OR（包含任意标签）
results = query_novels_by_tag(conn, ["校园", "恋爱"], match_all=False)
```

---

## crawl_log.txt

每次批量抓取的结果记录于 `crawl_log.txt`（TSV 格式），字段：

```
aid  status  crawl_date  novel_status  title  author  error
```

**智能跳过规则：**
- `novel_status=1`（已完结）→ 永久跳过，不重复抓取
- `novel_status=0`（连载中）且距 `crawl_date` 不足 30 天 → 跳过
- `status=FAIL` 或连载超 30 天 → 重新抓取

---

## 注意事项

- **Cloudflare 暖机**：每个新进程首次请求需约 30s 建立 CF session，属正常现象
- **CF session 保活**：批量抓取每 300 条自动刷新一次 CF session，避免长时间爬取中途失效
- **版权书籍**：版权下架仅影响下载，书籍信息页仍可访问，本工具支持抓取
- **低序号 aid**：aid < 100 的书籍可正常抓取，无需特殊处理
- **连续失败保护**：连续失败 5 次自动冷却 10s，避免触发封禁
- **无下载功能**：本项目仅抓取书籍元数据，不提供正文下载

---

## 依赖

| 包 | 用途 |
|----|------|
| httpx + httpx-curl-cffi | HTTP 客户端（Chrome 模拟）|
| zendriver | 无头浏览器（CF bypass）|
| lxml | HTML 解析 |
| python-dotenv | 环境变量 |

---

## 致谢

核心 Cloudflare 绕过方案来自 [pywenku8api](https://github.com/WorldObservationLog/pywenku8api)，感谢原作者 [WorldObservationLog](https://github.com/WorldObservationLog) 的工作。
