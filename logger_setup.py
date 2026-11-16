# -*- coding: utf-8 -*-
"""日志：loguru + 单文件 + 异步写盘。

只出一个文件，写满 10MB 就截断重写，不产生 .1 / .2；写盘放在后台线程，
业务线程只往内存队列里塞字符串，抓取不会被日志拖慢。队列满了丢日志不阻塞。
"""

import atexit
import sys
import threading
import time
from collections import deque
from pathlib import Path

from loguru import logger

import config

# 文件里的格式：时间 | 级别 | 进程 | 线程 | 位置 - 消息
FILE_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | pid {process: <7} | "
    "{thread.name: <18} | {name}:{function}:{line} - {message}"
)

# 终端里的格式：短一些，带颜色
CONSOLE_FORMAT = (
    "<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | "
    "<cyan>{name}:{line}</cyan> - <level>{message}</level>"
)


class SingleFileSink:
    """固定路径、写满清空的日志文件。

    loguru 的 rotation 是改名新建，会留一串 .1 / .2；这里直接截断重写，只有一个文件。
    """

    def __init__(self, path: str | Path, max_bytes: int):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max(1024, int(max_bytes))
        self._lock = threading.Lock()

        # buffering=0：追加写不经用户态缓冲，_size 和磁盘大小始终一致
        self._fh = open(self.path, "ab", buffering=0)
        self._size = self._fh.tell()
        self._closed = False

    def write(self, message) -> None:
        data = str(message).encode("utf-8")

        # 单条就超过上限（比如一条巨大的异常堆栈）：截断，避免反复清空
        if len(data) > self.max_bytes:
            data = data[: self.max_bytes]

        with self._lock:
            if self._closed:
                return
            if self._size + len(data) > self.max_bytes:
                self._truncate()
            self._fh.write(data)
            self._size += len(data)

    def _truncate(self) -> None:
        """把文件清空，写一行分隔说明，然后从 0 开始记。"""
        self._fh.truncate(0)
        self._size = 0

        head = (
            f"===== {time.strftime('%Y-%m-%d %H:%M:%S')} "
            f"日志达到上限 {self.max_bytes / 1024 / 1024:.0f}MB，已清空重写 =====\n"
        ).encode("utf-8")
        self._fh.write(head)
        self._size = len(head)

    def flush(self) -> None:
        with self._lock:
            if not self._closed:
                try:
                    self._fh.flush()
                except Exception:
                    pass

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._fh.flush()
                self._fh.close()
            except Exception:
                pass


class AsyncFileSink:
    """内存队列 + 后台写线程，套在 SingleFileSink 外面。

    write() 只做一次 append 就返回；磁盘 IO 全在 log-writer 线程里，攒一批写一次。
    """

    def __init__(
        self,
        path: str | Path,
        max_bytes: int,
        max_queue: int = 50_000,
        batch: int = 256,
    ):
        self._sink = SingleFileSink(path, max_bytes)
        self.path = self._sink.path

        self._max_queue = max(1000, int(max_queue))
        self._batch = max(1, int(batch))
        self._queue: deque[str] = deque()
        self._cond = threading.Condition(threading.Lock())
        self._closed = False
        self._dropped = 0
        self._writes = 0

        self._thread = threading.Thread(target=self._run, name="log-writer", daemon=True)
        self._thread.start()

    # ---- 业务线程走这里 ----

    def write(self, message) -> None:
        text = str(message)
        with self._cond:
            if self._closed:
                return
            if len(self._queue) >= self._max_queue:
                # 写盘跟不上：丢这条，不阻塞业务线程
                self._dropped += 1
                return
            was_empty = not self._queue
            self._queue.append(text)
            if was_empty:
                self._cond.notify()

    # ---- 后台线程 ----

    def _run(self) -> None:
        while True:
            with self._cond:
                while not self._queue and not self._closed:
                    self._cond.wait(1.0)
                if not self._queue:
                    return                      # 已关闭且队列空了
                count = min(self._batch, len(self._queue))
                chunk = "".join(self._queue.popleft() for _ in range(count))

            self._sink.write(chunk)
            self._writes += 1

    # ---- 收尾 ----

    def flush(self, timeout: float = 10.0) -> None:
        """等队列里剩下的写完，再刷盘。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._cond:
                if not self._queue:
                    break
            time.sleep(0.02)
        self._sink.flush()

    def close(self) -> None:
        with self._cond:
            if self._closed:
                return
            self._closed = True
            self._cond.notify_all()

        self._thread.join(timeout=15)

        if self._dropped:
            try:
                self._sink.write(
                    f"{time.strftime('%Y-%m-%d %H:%M:%S')} | WARNING  | "
                    f"日志写入跟不上，本次运行共丢弃 {self._dropped} 条\n"
                )
            except Exception:
                pass

        self._sink.close()

    @property
    def dropped(self) -> int:
        return self._dropped


# ---------------------------------------------------------------- 初始化

_sink: AsyncFileSink | None = None


def setup_logger() -> Path:
    """按 config 初始化全局日志，返回日志路径。重复调用会先关掉旧的。"""
    global _sink

    logger.remove()

    if config.LOG_CONSOLE:
        logger.add(
            sys.stderr,
            level=config.LOG_LEVEL,
            format=CONSOLE_FORMAT,
            colorize=True,
            backtrace=False,
            diagnose=False,
        )

    if _sink is not None:
        _sink.close()

    _sink = AsyncFileSink(
        Path(config.LOG_DIR) / config.LOG_FILE_NAME, config.LOG_MAX_BYTES
    )
    logger.add(
        _sink.write,
        level=config.LOG_LEVEL,
        format=FILE_FORMAT,
        colorize=False,
        enqueue=False,     # 异步由 AsyncFileSink 自己做，比 loguru 的 enqueue 更轻
        catch=True,
        backtrace=False,
        diagnose=False,
    )

    atexit.register(close_logger)
    return _sink.path


def flush_logs() -> None:
    """程序退出前调用，把队列里的日志写完。"""
    if _sink is not None:
        try:
            _sink.flush()
        except Exception:
            pass


def close_logger() -> None:
    """退出时最后调：写完队列、补上丢弃统计、关文件。"""
    if _sink is not None:
        try:
            _sink.close()
        except Exception:
            pass


_configured = False


def ensure_logger() -> Path:
    """只初始化一次，重复调用直接返回已有路径。"""
    global _configured
    if _sink is not None:
        return _sink.path
    _configured = True
    return setup_logger()
