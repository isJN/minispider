# -*- coding: utf-8 -*-
"""全部配置。仿 scrapy settings，扁平大写变量，改哪个直接搜变量名。

队列、去重、输出三个后端互相独立，可以混着用：
    QUEUE_BACKEND  local | redis
    DEDUP_BACKEND  local | redis
    OUTPUT_BACKEND file | kafka | mongo

默认全是本地/文件，装完 requirements.txt 就能跑，不需要 Redis、Mongo、Kafka。
"""

import os

# ---------------------------------------------------------------- 一、爬虫任务

# 任务名，决定队列、去重、结果文件的名字，不同任务互不干扰
TASK_NAME = "demo"

START_URLS = [
    "https://quotes.toscrape.com/",
]

# 匹配这些正则的 URL 才入队（re.match，从开头匹配）
ALLOWED_URL_PATTERNS = [
    r"^https://quotes\.toscrape\.com/($|page/\d+/$|author/|tag.*)",
]

# 匹配这些正则的算内容页，交给 parse_content_page 解析后输出
CONTENT_URL_PATTERNS = [
    r"^https://quotes\.toscrape\.com/author/",
]

THREAD_NUMBER = 8

# 单次请求超时（秒）
DOWNLOAD_TIMEOUT = 10

# 失败后额外重试次数，0 = 不重试
MAX_RETRY = 2

# 只有这些状态码值得重试，404 / 403 直接放弃
RETRY_HTTP_CODES = [408, 429, 500, 502, 503, 504]

# 队列空了睡几秒再看
IDLE_SLEEP = 3

# 连续空转这么多轮、且没有线程在干活，判定抓完了
IDLE_ROUNDS = 5

# 每处理多少条打一次进度日志
LOG_EVERY = 200

# 重跑同一任务时清空去重和队列。平时保持 False，否则每次启动都从头抓
RESET_DEDUP = False

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------- 二、存储后端

QUEUE_BACKEND = "local"      # local | redis
DEDUP_BACKEND = "local"      # local | redis
OUTPUT_BACKEND = "file"      # file | kafka | mongo

# ---- 本地模式（后端选 local / file 时生效）

# 布隆过滤器、结果文件、退出时保存的队列都放这里
DATA_DIR = "data"

# 预计要过滤的 URL 总数，超了误判率会上升
BLOOM_CAPACITY = 100_000_000
# 允许的误判率，越小越占内存
BLOOM_ERROR_RATE = 0.001
# 每新增多少条落盘一次，防止被强杀丢太多
BLOOM_SAVE_EVERY = 20_000
# 退出时保存没抓完的 URL，下次启动接着抓
DUMP_QUEUE = True

# ---- Redis（QUEUE_BACKEND / DEDUP_BACKEND 选 redis 时生效）

# REDIS_URL 填了只用它，下面四项忽略
REDIS_URL = ""               # 例：redis://:密码@127.0.0.1:6379/0
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = ""

# ---- MongoDB（OUTPUT_BACKEND = mongo 时生效）

# MONGO_URL 填了只用它，下面几项忽略
MONGO_URL = ""               # 例：mongodb://user:pass@host:27017/crawler?authSource=admin
MONGO_HOST = "127.0.0.1"
MONGO_PORT = 27017
MONGO_USER = ""
MONGO_PASSWORD = ""
MONGO_DB = "crawler"
MONGO_COLLECTION = ""        # 留空 = <任务名>_result

# ---- Kafka（OUTPUT_BACKEND = kafka 时生效）

KAFKA_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "crawl_html"
# 用户名留空 = PLAINTEXT；填了就走 SASL_PLAINTEXT / PLAIN
KAFKA_SASL_USER = ""
KAFKA_SASL_PASSWORD = ""

# ---------------------------------------------------------------- 三、日志

# 单文件，写满 LOG_MAX_BYTES 就清空从头写，不产生 .1 / .2 轮转文件
LOG_DIR = "logs"
LOG_FILE_NAME = "crawler.log"
LOG_MAX_BYTES = 10 * 1024 * 1024   # 10MB
LOG_LEVEL = "INFO"                 # DEBUG / INFO / WARNING / ERROR
LOG_CONSOLE = True                 # 是否同时打到终端

# ---------------------------------------------------------------- 四、代理

# 留空 = 不用代理，直连。
#
# 填了接口地址就按 ProxyManager 的方式工作：启动先拿一个，之后每次请求前检查，
# 用超过 PROXY_TTL 秒就换新，没超时继续用。PROXY_TTL = 0 表示每次请求都换。
#
# 接口返回两种格式都认：
#   JSON  : {"code": 200, "results": ["1.2.3.4:8888", ...]}，随机取一个
#   纯文本: 第一行当代理地址
#
# 接口取不到时继续用手里这个，手里没有就本次直连，不会卡住抓取。
# 想自己写取法（查库、拨号、多接口轮询）改 proxy_pool.py 的 custom_fetch_proxy()。
PROXY_API_URL = ""

# 代理有效秒数，超过就换新；0 = 每次请求都换
PROXY_TTL = 30

# 账号密码模式的代理：两个都填才生效，会拼成 http://用户:密码@ip:port
PROXY_USER = ""
PROXY_PASSWORD = ""

# ---------------------------------------------------------------- 五、SSH 部署

# 一台也行，多台也行，deploy.py 会并发推到每一台。
#   host     必填，IP 或域名
#   user     默认 ubuntu
#   port     默认 22
#   password 密码登录，和 key_file 二选一
#   key_file 密钥登录
#
# 密码也可以只写环境变量 SSH_PASSWORD，列表里不填 password 时用它（所有机器共用）。
SERVERS = [
    # {"host": "1.2.3.5", "user": "ubuntu", "password": "另一台的密码"},
    # {"host": "1.2.3.6", "user": "ubuntu", "key_file": "C:/Users/.ssh/id_rsa"},
]

# 部署到服务器哪个目录，留空 = /home/<user>/minispider
REMOTE_DIR = ""

# 部署完在每台机器上后台跑哪个文件
ENTRY_FILE = "crawler_demo.py"

# ---------------------------------------------------------------- 六、环境变量覆盖
# 不想把密码写进文件时，export 对应变量即可，优先级高于上面的值。

REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or REDIS_PASSWORD
MONGO_USER = os.environ.get("MONGO_USER") or MONGO_USER
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD") or MONGO_PASSWORD
KAFKA_SASL_USER = os.environ.get("KAFKA_SASL_USER") or KAFKA_SASL_USER
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD") or KAFKA_SASL_PASSWORD
PROXY_PASSWORD = os.environ.get("PROXY_PASSWORD") or PROXY_PASSWORD

_SSH_PASSWORD_ENV = os.environ.get("SSH_PASSWORD")
if _SSH_PASSWORD_ENV:
    for _item in SERVERS:
        _item.setdefault("password", _SSH_PASSWORD_ENV)
    del _item
del _SSH_PASSWORD_ENV
