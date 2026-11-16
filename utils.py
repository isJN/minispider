# -*- coding: utf-8 -*-
"""零碎工具：URL 标准化、字节格式化、布隆参数、连接串拼接。"""

from urllib.parse import quote, urldefrag, urljoin, urlparse, urlunsplit


def normalize_url(url: str, referer_url: str) -> str:
    """href 转成可入队的绝对 URL。

    去锚点、补相对路径、只留 http/https、主机名小写、去掉默认端口，
    避免同一个页面因为写法不同被抓两遍。返回 "" 表示不用管这个链接。
    """
    if not url:
        return ""

    url = url.strip()
    if not url:
        return ""

    lowered = url.lower()
    if lowered.startswith(("javascript:", "mailto:", "tel:", "data:")):
        return ""

    # 纯锚点（href="#top"）去掉后发现是空的，也直接丢弃
    url = urldefrag(url)[0]
    if not url:
        return ""

    if not urlparse(url).scheme:
        url = urljoin(referer_url, url)

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return ""

    netloc = parsed.netloc.lower()
    if parsed.scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    elif parsed.scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    path = parsed.path or "/"
    return urlunsplit((parsed.scheme, netloc, path, parsed.query, ""))


def human_bytes(number: int) -> str:
    """1024 → 1.00 KB。"""
    size = float(number)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} GB"


def bloom_params(capacity: int, error_rate: float) -> tuple[int, int]:
    """算布隆过滤器要多少位、几个哈希函数：m = -n·ln(p)/(ln2)²，k = m/n·ln2。"""
    from math import log

    n = max(1, int(capacity))
    p = min(max(float(error_rate), 1e-6), 0.5)

    bits = int(-n * log(p) / (log(2) ** 2))
    bits = max(64, (bits + 7) // 8 * 8)          # 补齐到整字节
    funcs = max(1, int(round(bits / n * log(2))))
    return bits, funcs


def looks_like_text(data: bytes) -> bool:
    """按内容判断文本还是二进制，含 NUL 字节的当二进制。

    按内容而不按后缀，Dockerfile、Makefile 这类没后缀的文本才能正确转换行尾，
    .whl / .png 这类二进制也才不会被改坏。
    """
    return b"\x00" not in data


def redis_dsn() -> str:
    """REDIS_URL 填了就用它，否则用账密拼。"""
    from config import REDIS_DB, REDIS_HOST, REDIS_PASSWORD, REDIS_PORT, REDIS_URL

    if REDIS_URL:
        return REDIS_URL
    auth = f":{quote(REDIS_PASSWORD, safe='')}@" if REDIS_PASSWORD else ""
    return f"redis://{auth}{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"


def mongo_dsn() -> str:
    """MONGO_URL 填了就用它，否则用账密拼。"""
    from config import MONGO_HOST, MONGO_PASSWORD, MONGO_PORT, MONGO_URL, MONGO_USER

    if MONGO_URL:
        return MONGO_URL
    if MONGO_USER:
        auth = f"{quote(MONGO_USER, safe='')}:{quote(MONGO_PASSWORD, safe='')}@"
    else:
        auth = ""
    return f"mongodb://{auth}{MONGO_HOST}:{MONGO_PORT}"
