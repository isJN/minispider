# -*- coding: utf-8 -*-
"""通用爬虫：队列取 URL → 下载 → 内容页输出 → 抽新链接去重后入队。

子类只关心两个方法：
    parse_needed_urls(html, referer_url)  挑出还要抓的 URL
    parse_content_page(html, url)         内容页输出什么，返回 dict

不覆写就走默认实现：所有 <a> 都跟，内容页输出 url + html。
"""

import re
import signal
import threading
import time
from typing import Iterable, Optional

import requests
from bs4 import BeautifulSoup
from loguru import logger

import config
from dedup import build_dedup
from logger_setup import ensure_logger
from output import build_output
from proxy_pool import get_proxy
from queue_store import build_queue
from utils import normalize_url


class CommonCrawler:
    """爬虫基类。子类覆写 parse_content_page 就能用，需要自定义跟链规则再覆写 parse_needed_urls。"""

    def __init__(self):
        ensure_logger()

        self.task_name = config.TASK_NAME
        self.start_urls = list(config.START_URLS)
        self.thread_number = config.THREAD_NUMBER
        self.timeout = config.DOWNLOAD_TIMEOUT
        self.max_retry = config.MAX_RETRY
        self.retry_http_codes = list(config.RETRY_HTTP_CODES)
        self.idle_sleep = config.IDLE_SLEEP
        self.idle_rounds = config.IDLE_ROUNDS
        self.log_every = config.LOG_EVERY

        self._allowed = [re.compile(p) for p in config.ALLOWED_URL_PATTERNS]
        self._content = [re.compile(p) for p in config.CONTENT_URL_PATTERNS]

        # 三个后端都由 config 决定
        self.queue = build_queue(self.task_name)
        self.dedup = build_dedup(self.task_name)
        self.output = build_output(self.task_name)

        self.headers = {
            "User-Agent": config.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }

        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._active = 0            # 正在处理 URL 的线程数
        self._processed = 0         # 已处理总数
        self._stats = {"hit": 0, "content": 0, "queued": 0, "failed": 0, "parse_error": 0}

    # ---- 子类覆写这两个 ----

    def parse_needed_urls(self, html: str, referer_url: str) -> Iterable[str]:
        """从页面挑出还要抓的 URL，默认取所有 <a href>。"""
        soup = BeautifulSoup(html, "html.parser")
        urls = set()
        for link in soup.find_all("a", href=True):
            url = normalize_url(link["href"], referer_url)
            if url and self.is_allowed_url(url):
                urls.add(url)
        return urls

    def parse_content_page(self, html: str, url: str) -> Optional[dict]:
        """内容页输出什么，默认带整个 html。"""
        return {
            "task_name": self.task_name,
            "url": url,
            "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "html": html,
        }

    # ---- 判定 ----

    def is_allowed_url(self, url: str) -> bool:
        return any(p.match(url) for p in self._allowed)

    def is_content_page(self, url: str) -> bool:
        return any(p.match(url) for p in self._content)

    # ---- 下载 ----

    def fetch(self, url: str) -> Optional[str]:
        """下载页面返回文本，最终失败返回 None。"""
        attempts = self.max_retry + 1

        for attempt in range(attempts):
            if self._stop.is_set():
                return None

            # 每次请求（含重试）都重新取一次代理，可能是同一个也可能是新的
            proxies = get_proxy()
            try:
                response = requests.get(
                    url, headers=self.headers, proxies=proxies, timeout=self.timeout
                )
            except requests.RequestException as exc:
                logger.warning("请求失败（第 {}/{} 次）{}：{}",
                               attempt + 1, attempts, url, exc)
                self._backoff(attempt, attempts)
                continue

            if response.status_code == 200:
                return self._decode(response)

            if response.status_code in self.retry_http_codes:
                logger.warning("状态码 {}（第 {}/{} 次）{}",
                               response.status_code, attempt + 1, attempts, url)
                self._backoff(attempt, attempts)
                continue

            # 404 / 403 这类重试也没用的，直接放弃
            logger.debug("状态码 {}，跳过 {}", response.status_code, url)
            return None

        return None

    def _backoff(self, attempt: int, attempts: int) -> None:
        """指数退避，最多等 10 秒；退出信号来了立刻打断。"""
        if attempt + 1 >= attempts:
            return
        delay = min(2 ** attempt, 10)
        self._stop.wait(delay)

    @staticmethod
    def _decode(response: requests.Response) -> str:
        """很多站点不声明 charset，requests 会默认当成 ISO-8859-1，中文就乱码了。"""
        if not response.encoding or response.encoding.lower() in ("iso-8859-1", "ascii"):
            response.encoding = response.apparent_encoding or "utf-8"
        return response.text

    # ---- 入队 ----

    def _enqueue(self, urls: Iterable[str]) -> int:
        """去重后入队，返回新增条数。add_if_absent 原子，多线程不会重复入队。"""
        fresh = [url for url in urls if self.dedup.add_if_absent(url)]
        if fresh:
            self.queue.push(fresh)
        return len(fresh)

    # ---- 处理一个 URL ----

    def _handle(self, url: str) -> None:
        html = self.fetch(url)

        if html is None:
            with self._lock:
                self._stats["failed"] += 1
            return

        with self._lock:
            self._stats["hit"] += 1

        if self.is_content_page(url):
            try:
                item = self.parse_content_page(html, url)
                if item:
                    self.output.write(item)
                    with self._lock:
                        self._stats["content"] += 1
                    logger.debug("内容页 {}", url)
            except Exception:
                logger.exception("解析内容页失败：{}", url)
                with self._lock:
                    self._stats["parse_error"] += 1

        try:
            new_urls = self.parse_needed_urls(html, url)
        except Exception:
            logger.exception("抽取链接失败：{}", url)
            return

        added = self._enqueue(new_urls)
        if added:
            with self._lock:
                self._stats["queued"] += added

    # ---- 线程 ----

    def _worker(self, index: int) -> None:
        idle = 0
        while not self._stop.is_set():
            url = self.queue.pop()

            if url is None:
                if self._should_exit(idle):
                    break
                idle += 1
                self._stop.wait(self.idle_sleep)
                continue

            idle = 0
            with self._lock:
                self._active += 1
            try:
                self._handle(url)
            finally:
                with self._lock:
                    self._active -= 1
                    self._processed += 1
                    processed = self._processed
                if self.log_every and processed % self.log_every == 0:
                    self._log_progress()

        logger.debug("worker-{} 退出", index)

    def _should_exit(self, idle: int) -> bool:
        """空转够多轮 + 队列空 + 没有线程在干活，才算抓完。

        只看队列空会误判：别的线程正在解析，马上会塞新链接进来。
        """
        if idle + 1 < self.idle_rounds:
            return False
        return self.queue.size() == 0 and self._active == 0

    def _log_progress(self) -> None:
        with self._lock:
            stats = dict(self._stats)
            processed = self._processed
        logger.info(
            "进度 | 已处理 {} | 内容页 {} | 新入队 {} | 失败 {} | 队列剩余 {}",
            processed, stats["content"], stats["queued"], stats["failed"], self.queue.size(),
        )

    # ---- 启动与收尾 ----

    def start(self) -> None:
        """阻塞运行，直到抓完或收到 Ctrl+C / SIGTERM。"""
        self._log_banner()
        self._install_signal_handlers()

        if config.RESET_DEDUP:
            logger.warning("RESET_DEDUP = True，清空去重和队列后重跑")
            self.dedup.clear()
            self.queue.clear()

        # 起始页先记进去重表，否则页面里的自链接会让它被再抓一次
        for url in self.start_urls:
            self.dedup.add(url)
        self.queue.push(self.start_urls)
        logger.info("起始页 {} 个入队，队列 {} 条", len(self.start_urls), self.queue.size())

        threads = [
            threading.Thread(target=self._worker, args=(i,), name=f"worker-{i}", daemon=False)
            for i in range(1, self.thread_number + 1)
        ]

        try:
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            logger.warning("收到 Ctrl+C，正在停止（最多等 15 秒）")
            self._stop.set()
            for t in threads:
                t.join(timeout=15)
        finally:
            self._shutdown()

    def stop(self) -> None:
        """外部停止入口。"""
        self._stop.set()

    def _shutdown(self) -> None:
        try:
            self.output.close()
        except Exception as exc:
            logger.warning("关闭输出失败：{}", exc)
        try:
            self.dedup.save()
        except Exception as exc:
            logger.warning("保存去重记录失败：{}", exc)
        try:
            self.queue.close()
        except Exception as exc:
            logger.warning("保存队列失败：{}", exc)

        with self._lock:
            stats = dict(self._stats)
            processed = self._processed
        logger.info(
            "任务结束 | {} | 处理 {} 条 | 内容页 {} | 新入队 {} | 失败 {} | 解析异常 {}",
            self.task_name, processed, stats["content"], stats["queued"],
            stats["failed"], stats["parse_error"],
        )

    def _install_signal_handlers(self) -> None:
        """kill 时也走 finally，把队列和去重记录存下来。"""
        def handler(signum, _frame):
            logger.warning("收到信号 {}，准备退出", signum)
            self._stop.set()

        for name in ("SIGTERM", "SIGINT"):
            sig = getattr(signal, name, None)
            if sig is None:
                continue
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass    # 非主线程或不支持，忽略

    def _log_banner(self) -> None:
        logger.info("任务 {} 启动 | 队列 {} / 去重 {} / 输出 {}",
                    self.task_name, config.QUEUE_BACKEND, config.DEDUP_BACKEND,
                    config.OUTPUT_BACKEND)
        logger.info("线程 {} | 超时 {}s | 重试 {} 次 | 代理 {}",
                    self.thread_number, self.timeout, self.max_retry,
                    config.PROXY_API_URL or "无")
        logger.info("起始页 {}", ", ".join(self.start_urls))
