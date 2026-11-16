# -*- coding: utf-8 -*-
"""代理管理。

启动先拿一个，之后每次请求前检查：超过 PROXY_TTL 秒就换新的，没超时接着用。
PROXY_TTL = 0 就是每次请求都换。多线程共用一个实例，换代理在锁里做。

取代理两种方式：
  1. 改下面的 custom_fetch_proxy()，想怎么取都行，返回 {"http":..., "https":...} 或 None；
  2. config.PROXY_API_URL 填接口地址，认两种返回：
     JSON {"code": 200, "results": ["1.2.3.4:8888", ...]} 随机取一个
     纯文本 第一行当代理地址

账号密码的代理在 config 填 PROXY_USER / PROXY_PASSWORD，会自动拼进地址，日志里打码。
取不到时继续用手里的，没有就直连，不会卡住抓取。
"""

import random
import threading
import time
from urllib.parse import quote

import requests
from loguru import logger

import config

_FETCH_TIMEOUT = (5, 15)      # 连接 5 秒 / 读 15 秒
_FETCH_RETRIES = 3            # 连续失败几次就放弃


def custom_fetch_proxy():
    """自定义取代理就改这里，返回 {"http": ..., "https": ...} 或 None。

        def custom_fetch_proxy():
            resp = requests.get("http://my-api/proxies?n=1", timeout=5)
            proxy = resp.json()["data"][0]
            return {"http": f"http://{proxy}", "https": f"http://{proxy}"}

    返回 None 表示不接管，继续走 config.PROXY_API_URL。
    """
    return None


def _apply_auth(address: str) -> str:
    """把账密拼进代理地址。

    1.2.3.4:8888         -> http://user:pass@1.2.3.4:8888
    socks5://1.2.3.4:1080 -> socks5://user:pass@1.2.3.4:1080
    没配账密就原样返回，顺便补全协议头。
    """
    user = config.PROXY_USER
    password = config.PROXY_PASSWORD

    if user and password:
        auth = f"{quote(user, safe='')}:{quote(password, safe='')}@"
        if "://" in address:
            scheme, rest = address.split("://", 1)
            return f"{scheme}://{auth}{rest}"
        return f"http://{auth}{address}"

    # 没配认证：没写协议头就补 http://
    if "://" not in address:
        address = "http://" + address
    return address


def _mask(address: str) -> str:
    """日志里藏掉账密。"""
    if "@" not in address:
        return address
    scheme = ""
    rest = address
    if "://" in address:
        scheme, rest = address.split("://", 1)
    return f"{scheme}{'://' if scheme else ''}***@{rest.split('@', 1)[1]}"


class ProxyManager:
    """持有当前代理，超时换新，线程安全。"""

    def __init__(self):
        self.proxy = None
        self.start_time = 0.0
        self._lock = threading.Lock()

    def get_proxy(self) -> dict | None:
        """每次请求前调用。TTL = 0 时用 >=，保证同一时刻连续调用也会换新。"""
        if not config.PROXY_API_URL:
            return None

        with self._lock:
            elapsed = time.time() - self.start_time
            ttl = max(0, config.PROXY_TTL)
            expired = elapsed >= ttl if ttl == 0 else elapsed > ttl

            if self.proxy is None or expired:
                new_proxy = self._fetch()
                if new_proxy is not None:
                    self.proxy = new_proxy
                self.start_time = time.time()
            return self.proxy

    # ---- 取代理 ----

    def _fetch(self) -> dict | None:
        # 自定义逻辑优先
        try:
            result = custom_fetch_proxy()
            if result:
                return result
        except Exception as exc:
            logger.warning("custom_fetch_proxy() 出错：{}", exc)

        # 走接口，失败几次就返回 None（用手里的或直连）
        for attempt in range(1, _FETCH_RETRIES + 1):
            try:
                resp = requests.get(config.PROXY_API_URL, timeout=_FETCH_TIMEOUT)
                if resp.status_code != 200:
                    raise RuntimeError(f"状态码 {resp.status_code}")

                data = resp.json()
                results = data.get("results") if isinstance(data, dict) else None
                if isinstance(data, dict) and data.get("code") not in (200, "200"):
                    raise RuntimeError(f"接口返回 code={data.get('code')}")

                if results:
                    address = str(random.choice(results)).strip()
                else:
                    # 兼容纯文本接口：取第一行
                    address = resp.text.strip().splitlines()[0].strip()

                if not address:
                    raise RuntimeError("接口没返回代理地址")

                address = _apply_auth(address)

                logger.info("拿到新代理：{}（{}s 后换新）",
                            _mask(address), config.PROXY_TTL)
                return {"http": address, "https": address}

            except Exception as exc:
                logger.warning("取代理失败（第 {}/{} 次）：{}",
                               attempt, _FETCH_RETRIES, exc)
                time.sleep(1)

        return None


# 全局一个实例，所有抓取线程共用
_manager = ProxyManager()


def get_proxy() -> dict | None:
    """common_crawler 每次下载前调用。返回 requests 的 proxies 字典，None = 直连。"""
    return _manager.get_proxy()
