# -*- coding: utf-8 -*-
"""待抓 URL 队列两个实现。

    LocalQueue    进程内 deque，退出时存成 json，下次启动接着抓
    RedisQueue    Redis List，多进程 / 多机可以一起消费

单机单进程用 local 就行，要多开或多机才需要 redis。
"""

import json
import threading
from collections import deque
from pathlib import Path

from loguru import logger

import config
from utils import redis_dsn

_redis_client = None
_redis_lock = threading.Lock()


def get_redis():
    """全局共用一个连接，队列和去重都用它。"""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    with _redis_lock:
        if _redis_client is None:
            try:
                import redis
            except ImportError:
                raise RuntimeError("使用 Redis 需要先装依赖：pip install redis") from None
            _redis_client = redis.Redis.from_url(
                redis_dsn(), decode_responses=True, socket_timeout=10
            )
            _redis_client.ping()      # 连不上立刻报错，别等抓取时才炸
            logger.debug("Redis 已连接")
    return _redis_client


class LocalQueue:
    """进程内队列，退出时可选择落盘续抓。"""

    def __init__(self, task_name: str):
        self.task_name = task_name
        self.path = Path(config.DATA_DIR) / f"{task_name}_queue.json"
        self._dump_enabled = config.DUMP_QUEUE
        self._items: deque[str] = deque()
        self._lock = threading.Lock()

        self._restore()

    # ---- 对外接口 ----

    def push(self, urls) -> int:
        """批量入队，返回实际入队条数。"""
        if isinstance(urls, str):
            urls = [urls]
        items = [u for u in urls if u]
        if not items:
            return 0
        with self._lock:
            self._items.extend(items)
        return len(items)

    def pop(self) -> str | None:
        """取一个 URL，队列空返回 None。"""
        with self._lock:
            if not self._items:
                return None
            return self._items.popleft()

    def size(self) -> int:
        with self._lock:
            return len(self._items)

    def clear(self) -> None:
        with self._lock:
            self._items.clear()
        try:
            self.path.unlink(missing_ok=True)
        except Exception:
            pass

    def close(self) -> None:
        self._dump()

    # ---- 续抓 ----

    def _restore(self) -> None:
        if not self._dump_enabled or not self.path.is_file():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                self._items.extend(str(x) for x in data if x)
            logger.info("已恢复上次未抓的 {} 条 URL", len(self._items))
        except Exception as exc:
            logger.warning("读取未抓队列失败（{}），忽略", exc)
        finally:
            try:
                self.path.unlink(missing_ok=True)
            except Exception:
                pass

    def _dump(self) -> None:
        if not self._dump_enabled:
            return
        with self._lock:
            remaining = list(self._items)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if remaining:
                self.path.write_text(
                    json.dumps(remaining, ensure_ascii=False), encoding="utf-8"
                )
                logger.info("已保存 {} 条未抓 URL，下次启动自动接着抓", len(remaining))
            else:
                self.path.unlink(missing_ok=True)
        except Exception as exc:
            logger.warning("保存未抓队列失败：{}", exc)


class RedisQueue:
    """Redis List 队列，多进程 / 多机共享。"""

    def __init__(self, task_name: str):
        self.task_name = task_name
        self.key = f"{task_name}_url_queue"
        self.client = get_redis()

    def push(self, urls) -> int:
        if isinstance(urls, str):
            urls = [urls]
        items = [u for u in urls if u]
        if not items:
            return 0
        self.client.rpush(self.key, *items)
        return len(items)

    def pop(self) -> str | None:
        return self.client.lpop(self.key)

    def size(self) -> int:
        return int(self.client.llen(self.key))

    def clear(self) -> None:
        self.client.delete(self.key)

    def close(self) -> None:
        """Redis 自己持有数据，不需要落盘。"""
        return


def build_queue(task_name: str):
    """按 QUEUE_BACKEND 选一个实现。"""
    if config.QUEUE_BACKEND == "redis":
        return RedisQueue(task_name)
    if config.QUEUE_BACKEND == "local":
        return LocalQueue(task_name)
    raise ValueError(
        f"QUEUE_BACKEND 只能是 local 或 redis，现在是 {config.QUEUE_BACKEND!r}"
    )
