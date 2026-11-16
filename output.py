# -*- coding: utf-8 -*-
"""抓取结果出口，三种实现。

    FileOutput   data/<task>_result.jsonl，一行一条
    KafkaOutput  投到 topic，支持 SASL
    MongoOutput  写集合

接口统一是 write(dict) / close()。kafka-python、pymongo 都是按需 import，
只用 file 模式不用装。
"""

import json
import threading
from datetime import date, datetime
from pathlib import Path

from loguru import logger

import config
from utils import mongo_dsn


def _json_default(value):
    """json 不认 datetime，转字符串。"""
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _to_json(item: dict) -> str:
    return json.dumps(item, ensure_ascii=False, default=_json_default)


class FileOutput:
    """JSONL，一行一条，追加写。"""

    def __init__(self, task_name: str):
        self.path = Path(config.DATA_DIR) / f"{task_name}_result.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # buffering=1：每行都落到系统，进程被 kill 最多丢正在写的那行
        self._fh = open(self.path, "a", encoding="utf-8", buffering=1)
        self._lock = threading.Lock()
        self._count = 0

    def write(self, item: dict) -> None:
        line = _to_json(item) + "\n"
        with self._lock:
            self._fh.write(line)
            self._count += 1

    def close(self) -> None:
        with self._lock:
            if not self._fh.closed:
                self._fh.close()
        logger.info("结果文件已关闭：{}（{} 条）", self.path, self._count)


class KafkaOutput:
    """投递到 Kafka。需要 pip install kafka-python。"""

    def __init__(self):
        try:
            from kafka import KafkaProducer
        except ImportError:
            raise RuntimeError("使用 Kafka 需要先装依赖：pip install kafka-python") from None

        options = {
            "bootstrap_servers": config.KAFKA_SERVERS.split(","),
            "acks": "all",
            "value_serializer": lambda v: json.dumps(
                v, ensure_ascii=False, default=_json_default
            ).encode("utf-8"),
            "retries": 3,
            "linger_ms": 50,
        }
        if config.KAFKA_SASL_USER:
            options.update(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_plain_username=config.KAFKA_SASL_USER,
                sasl_plain_password=config.KAFKA_SASL_PASSWORD,
            )

        self.topic = config.KAFKA_TOPIC
        self.producer = KafkaProducer(**options)
        self._lock = threading.Lock()
        self._count = 0
        logger.info("Kafka 已连接：{} -> topic {}", config.KAFKA_SERVERS, self.topic)

    def write(self, item: dict) -> None:
        # send 是异步的，只放进本地缓冲，不等 broker
        with self._lock:
            self.producer.send(self.topic, value=item)
            self._count += 1

    def close(self) -> None:
        try:
            self.producer.flush(timeout=30)   # 退出前把缓冲发完
        except Exception as exc:
            logger.warning("Kafka flush 超时：{}", exc)
        finally:
            self.producer.close(timeout=10)
        logger.info("Kafka 已关闭（发送 {} 条）", self._count)


class MongoOutput:
    """写 MongoDB。需要 pip install pymongo。"""

    def __init__(self, task_name: str):
        try:
            import pymongo
        except ImportError:
            raise RuntimeError("使用 MongoDB 需要先装依赖：pip install pymongo") from None

        client = pymongo.MongoClient(mongo_dsn(), serverSelectionTimeoutMS=10000)
        client.admin.command("ping")        # 连不上立刻报错
        db = client[config.MONGO_DB]
        self.collection = db[config.MONGO_COLLECTION or f"{task_name}_result"]
        self.client = client
        self._count = 0
        logger.info("MongoDB 已连接：{}.{}", config.MONGO_DB, self.collection.name)

    def write(self, item: dict) -> None:
        self.collection.insert_one(item)
        self._count += 1

    def close(self) -> None:
        self.client.close()
        logger.info("MongoDB 已关闭（写入 {} 条）", self._count)


def build_output(task_name: str):
    """按 OUTPUT_BACKEND 选一个实现。"""
    backend = config.OUTPUT_BACKEND
    if backend == "file":
        return FileOutput(task_name)
    if backend == "kafka":
        return KafkaOutput()
    if backend == "mongo":
        return MongoOutput(task_name)
    raise ValueError(f"OUTPUT_BACKEND 只能是 file / kafka / mongo，现在是 {backend!r}")
