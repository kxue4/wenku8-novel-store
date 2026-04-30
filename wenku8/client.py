from __future__ import annotations

import re
from urllib.parse import quote

import httpx
from httpx_curl_cffi import AsyncCurlTransport, CurlOpt
from lxml import etree

ENDPOINT = "https://www.wenku8.net"
WARMUP_AIDS = [1, 100, 500, 1000]


def _extract(parser, xpath: str, split: bool = False) -> str | None:
    nodes = parser.xpath(xpath)
    if not nodes:
        return None
    text = nodes[0].text
    if text is None:
        return None
    if split:
        # 处理中文冒号分隔（︰ 或 ：）
        if "︰" in text:
            parts = text.split("︰", 1)
        else:
            parts = text.split("：", 1)
        return parts[1] if len(parts) > 1 else text
    return text


class Wenku8Client:
    def __init__(self) -> None:
        self.session = httpx.AsyncClient(
            transport=AsyncCurlTransport(
                impersonate="chrome",
                default_headers=True,
                curl_options={CurlOpt.FRESH_CONNECT: True},
            ),
            follow_redirects=True,
        )

    async def close(self) -> None:
        await self.session.aclose()

    async def _bypass_cloudflare(self, url: str) -> None:
        from .cf_solver import get_cloudflare_clearance
        user_agent, cookies = await get_cloudflare_clearance(url=url, timeout=30.0)
        self.session.headers.update({"User-Agent": user_agent})
        self.session.cookies.update(cookies)

    async def _request(self, *args, **kwargs):
        cf_bypassed = kwargs.pop("_cf_bypassed", False)
        try:
            result = await self.session.request(*args, **kwargs)
            result.raise_for_status()
            return result
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 503) and not cf_bypassed:
                url = args[1] if len(args) > 1 else kwargs.get("url")
                if url:
                    try:
                        await self._bypass_cloudflare(str(url))
                    except Exception:
                        pass
                kwargs["_cf_bypassed"] = True
                return await self._request(*args, **kwargs)
            raise

    async def login(self, username: str, password: str, validity: str = "2592000") -> None:
        form_data = {
            "username": username,
            "password": password,
            "usercookie": validity,
            "action": "login",
            "submit": "%26%23160%3B%B5%C7%26%23160%3B%26%23160%3B%C2%BC%26%23160%3B",
        }
        await self._request("POST", ENDPOINT + "/login.php", data=form_data)

    async def get_novel_info(self, aid: int) -> dict | None:
        try:
            resp = await self._request(
                "GET",
                ENDPOINT + f"/modules/article/articleinfo.php?id={aid}&charset=gbk",
            )
        except Exception as e:
            print(f"[get_novel_info] aid={aid} 请求失败: {e}")
            return None

        resp.encoding = "gbk"
        parser = etree.HTML(resp.text)
        if parser is None:
            print(f"[get_novel_info] aid={aid} 响应体为空，无法解析")
            return None

        # 版权下架书籍的 HTML 结构不同（缺少某些节点）
        is_copyright_removed = bool(
            len(parser.xpath('//*[@id="content"]/div[1]/table[2]/tr/td[2]/span[2]/b/br'))
        )

        if is_copyright_removed:
            last_updated = None
            word_count = None
            intro = "".join(
                parser.xpath('//*[@id="content"]/div[1]/table[2]/tr/td[2]/span[4]//text()')
            )
        else:
            last_updated = _extract(
                parser, '//*[@id="content"]/div[1]/table[1]/tr[2]/td[4]', split=True
            )
            word_count_str = _extract(
                parser, '//*[@id="content"]/div[1]/table[1]/tr[2]/td[5]', split=True
            )
            try:
                word_count = int(word_count_str.replace("字", "")) if word_count_str else None
            except ValueError:
                word_count = None
            intro = "".join(
                parser.xpath('//*[@id="content"]/div[1]/table[2]/tr/td[2]/span[6]//text()')
            )

        title = _extract(
            parser, '//*[@id="content"]/div[1]/table[1]/tr[1]/td/table/tr/td[1]/span/b'
        )
        author = _extract(
            parser, '//*[@id="content"]/div[1]/table[1]/tr[2]/td[2]', split=True
        )
        status = _extract(
            parser, '//*[@id="content"]/div[1]/table[1]/tr[2]/td[3]', split=True
        )
        press = _extract(
            parser, '//*[@id="content"]/div[1]/table[1]/tr[2]/td[1]', split=True
        )
        tags_raw = _extract(
            parser, '//*[@id="content"]/div[1]/table[2]/tr/td[2]/span[1]/b', split=True
        )
        tags = [t for t in tags_raw.split(" ") if t] if tags_raw else []
        animation = bool(
            len(parser.xpath('//*[@id="content"]/div[1]/table[2]/tr/td[1]/span/b'))
        )

        return {
            "bookid": aid,
            "title": title,
            "author": author,
            "status": status,
            "last_updated": last_updated,
            "intro": intro.strip() if intro else None,
            "tags": tags,
            "press": press,
            "word_count": word_count,
            "animation": animation,
            "cover": f"https://img.wenku8.com/image/{aid // 1000}/{aid}/{aid}s.jpg",
        }

    async def search_by_name(self, keyword: str) -> int | None:
        """按书名搜索，返回第一条结果的 aid；未找到返回 None。"""
        encoded = quote(keyword.encode("gbk"))
        url = (
            ENDPOINT
            + f"/modules/article/search.php?searchtype=articlename&searchkey={encoded}&page=1"
        )
        try:
            resp = await self._request("GET", url)
        except Exception as e:
            print(f"[search_by_name] 搜索失败: {e}")
            return None

        resp.encoding = "gbk"

        # 只有一个结果时直接重定向到书籍页面
        if str(resp.url).endswith(".htm"):
            m = re.search(r"(\d+)\.htm", str(resp.url))
            return int(m.group(1)) if m else None

        parser = etree.HTML(resp.text)
        rows = parser.xpath('//*[@id="content"]/table/tr/td')
        if not rows or not len(rows[0]):
            return None

        try:
            first = rows[0][0]
            href = first[1][0][0].get("href")
            m = re.search(r"(\d+)\.htm", href)
            return int(m.group(1)) if m else None
        except (IndexError, TypeError):
            return None


async def warmup(client: Wenku8Client) -> None:
    """探测封面 URL，让 zendriver 在后台建立 cf_clearance cookie。"""
    for aid in WARMUP_AIDS:
        try:
            await client._request(
                "GET",
                f"https://img.wenku8.com/image/{aid // 1000}/{aid}/{aid}s.jpg",
            )
            print(f"[warmup] CF session 建立成功（aid={aid}）")
            return
        except Exception:
            continue
    print("[warmup] 所有探测均失败，将在正式请求时自动重试 CF bypass")
