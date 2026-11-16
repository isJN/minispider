# -*- coding: utf-8 -*-
"""URL 去重：布隆过滤器两个实现。

    LocalBloomFilter 内存里一个 bytearray，退出落盘，单机零依赖
    RedisBloomFilter 位存 Redis，多台机器共用一份

对外就三个方法：exists / add / add_if_absent。add_if_absent 是原子的，
多线程同时判断同一个 URL 时只有一个能拿到 True，不会重复入队。
"""

import hashlib
import os
import struct
import threading
from pathlib import Path

from loguru import logger

import config
from utils import bloom_params

_MAGIC = b"SCBF1"   # 落盘文件头


class LocalBloomFilter:
    """内存布隆过滤器，存到 data/<task>_bloom.bin。"""

    def __init__(self, task_name: str):
        self.task_name = task_name
        self.path = Path(config.DATA_DIR) / f"{task_name}_bloom.bin"
        self.save_every = max(1, config.BLOOM_SAVE_EVERY)

        bits, funcs = bloom_params(config.BLOOM_CAPACITY, config.BLOOM_ERROR_RATE)
        self._bits = bits
        self._funcs = funcs
        self._buf = bytearray(bits // 8)
        self._lock = threading.RLock()   # 可重入：_after_add 里会再次调用 save()
        self._added = 0        # 自上次落盘以来新增的数量
        self._total = 0

        self._load()

    # ---- 对外接口 ----

    def exists(self, url: str) -> bool:
        h1, h2 = self._hashes(url)
        with self._lock:
            for index in self._positions(h1, h2):
                if not self._buf[index >> 3] & (1 << (index & 7)):
                    return False
            return True

    def add(self, url: str) -> None:
        h1, h2 = self._hashes(url)
        with self._lock:
            self._set_bits(h1, h2)
            self._after_add()

    def add_if_absent(self, url: str) -> bool:
        """不存在则插入并返回 True；已存在返回 False。整个判断加插入在一把锁里。"""
        h1, h2 = self._hashes(url)
        with self._lock:
            for index in self._positions(h1, h2):
                if not self._buf[index >> 3] & (1 << (index & 7)):
                    self._set_bits(h1, h2)
                    self._after_add()
                    return True
            return False

    def save(self, force: bool = True) -> None:
        """把位数组写盘。先写临时文件再原子替换，避免写一半被强杀留下坏文件。"""
        with self._lock:
            if not force and self._added < self.save_every:
                return
            payload = _MAGIC + struct.pack("<QII", self._bits, self._funcs, 0) + bytes(self._buf)
            self._added = 0

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_bytes(payload)
            os.replace(tmp, self.path)
        except Exception as exc:
            logger.warning("布隆过滤器落盘失败：{}", exc)

    def clear(self) -> None:
        """清空去重记录（重跑任务时用）。"""
        with self._lock:
            self._buf = bytearray(self._bits // 8)
            self._total = 0
            self._added = 0
        try:
            self.path.unlink(missing_ok=True)
        except Exception:
            pass

    # ---- 内部 ----

    def _hashes(self, url: str) -> tuple[int, int]:
        # double hashing：用 md5 的前后 64 位算出两个基值，
        # 第 i 个位置 = (h1 + i * h2) % bits，比循环算 k 次快得多。
        digest = hashlib.md5(url.encode("utf-8", "replace")).digest()
        h1 = int.from_bytes(digest[:8], "little")
        h2 = int.from_bytes(digest[8:], "little") | 1     # 保证是奇数
        return h1, h2

    def _positions(self, h1: int, h2: int):
        bits = self._bits
        for i in range(self._funcs):
            yield (h1 + i * h2) % bits

    def _set_bits(self, h1: int, h2: int) -> None:
        for index in self._positions(h1, h2):
            self._buf[index >> 3] |= 1 << (index & 7)

    def _after_add(self) -> None:
        self._total += 1
        self._added += 1
        if self._added >= self.save_every:
            self.save(force=False)

    def _load(self) -> None:
        if not self.path.is_file():
            return
        try:
            raw = self.path.read_bytes()
            if len(raw) < len(_MAGIC) + 16 or not raw.startswith(_MAGIC):
                logger.warning("布隆过滤器文件格式不认识，忽略：{}", self.path)
                return
            bits, funcs, _ = struct.unpack("<QII", raw[len(_MAGIC):len(_MAGIC) + 16])
            payload = raw[len(_MAGIC) + 16:]

            if bits != self._bits or funcs != self._funcs:
                logger.warning(
                    "布隆过滤器容量与当前配置不一致（文件 {} 位 / 配置 {} 位），已忽略旧文件",
                    bits, self._bits,
                )
                return
            if len(payload) != self._bits // 8:
                logger.warning("布隆过滤器文件长度异常，已忽略：{}", self.path)
                return

            self._buf = bytearray(payload)
            logger.info("已加载去重记录：{}", self.path)
        except Exception as exc:
            logger.warning("读取布隆过滤器失败（{}），从空开始", exc)

    @property
    def total(self) -> int:
        return self._total

    def __len__(self) -> int:
        return self._total


class RedisBloomFilter:
    """位存在 Redis 的一个 string 里，多台机器共用。"""

    # 判断 + 插入合成一个原子操作，避免多进程/多线程下重复入队
    _SCRIPT = """
    local bits = tonumber(ARGV[1])
    local funcs = tonumber(ARGV[2])
    local exists = true
    for i = 1, funcs do
        local pos = tonumber(ARGV[2 + i])
        if redis.call('getbit', KEYS[1], pos) == 0 then
            exists = false
            break
        end
    end
    if exists then
        return 0
    end
    for i = 1, funcs do
        redis.call('setbit', KEYS[1], tonumber(ARGV[2 + i]), 1)
    end
    return 1
    """

    def __init__(self, client, task_name: str):
        self.client = client
        self.task_name = task_name
        self.key = f"{task_name}_url_bloom"

        bits, funcs = bloom_params(config.BLOOM_CAPACITY, config.BLOOM_ERROR_RATE)
        self._bits = bits
        self._funcs = funcs
        self._script = client.register_script(self._SCRIPT)

    # ---- 对外接口 ----

    def exists(self, url: str) -> bool:
        positions = self._positions(url)
        pipe = self.client.pipeline(transaction=False)
        for pos in positions:
            pipe.getbit(self.key, pos)
        return all(pipe.execute())

    def add(self, url: str) -> None:
        pipe = self.client.pipeline(transaction=False)
        for pos in self._positions(url):
            pipe.setbit(self.key, pos, 1)
        pipe.execute()

    def add_if_absent(self, url: str) -> bool:
        positions = self._positions(url)
        args = [self._bits, self._funcs, *positions]
        return bool(self._script(keys=[self.key], args=args))

    def save(self, force: bool = True) -> None:
        """Redis 自己负责持久化，这里不用做事。"""
        return

    def clear(self) -> None:
        self.client.delete(self.key)

    # ---- 内部 ----

    def _positions(self, url: str) -> list[int]:
        digest = hashlib.md5(url.encode("utf-8", "replace")).digest()
        h1 = int.from_bytes(digest[:8], "little")
        h2 = int.from_bytes(digest[8:], "little") | 1
        return [(h1 + i * h2) % self._bits for i in range(self._funcs)]

    @property
    def total(self) -> int:
        return 0

    def __len__(self) -> int:
        return 0


def build_dedup(task_name: str):
    """按 DEDUP_BACKEND 选一个实现。"""
    if config.DEDUP_BACKEND == "redis":
        from queue_store import get_redis
        return RedisBloomFilter(get_redis(), task_name)
    if config.DEDUP_BACKEND == "local":
        return LocalBloomFilter(task_name)
    raise ValueError(
        f"DEDUP_BACKEND 只能是 local 或 redis，现在是 {config.DEDUP_BACKEND!r}"
    )
