# MiniSpider

一个可以直接拿来用的 Python 多线程爬虫框架。写完 `parse_content_page` 就是一个能跑的爬虫，
剩下的 URL 去重、断点续抓、结果落地、代理切换、日志、部署，框架都已经做好了。

默认配置零外部服务：不需要 Redis、MongoDB、Kafka，也不需要 Docker，`pip install` 之后直接 `python crawler_demo.py` 就能抓。

配置集中在一个 `config.py` 里，写法仿 scrapy 的 settings，全是扁平的大写变量。

- Python 3.10+
- 依赖：`requests`、`beautifulsoup4`、`loguru`（本地模式只需要这三个）

## 功能

| 能力 | 说明 |
|---|---|
| 多线程抓取 | 固定线程数消费同一个队列，线程自己判断什么时候抓完 |
| URL 规则 | 两个正则列表分别控制「哪些链接要跟」和「哪些页面算内容页」 |
| 去重 | 布隆过滤器，本地内存版或 Redis 版；`add_if_absent` 原子操作，多线程不会重复入队 |
| 断点续抓 | 本地模式退出时把没抓完的 URL 存成 json，下次启动自动接着抓 |
| 结果输出 | JSONL 文件 / Kafka / MongoDB，接口一致，改一个配置项就切换 |
| 日志 | loguru 单文件，写满 10MB 清空重写，后台线程批量写盘，不阻塞抓取 |
| 代理 | 默认直连；支持代理接口、代理 TTL 到期换新、账号密码代理、完全自定义取法 |
| 部署 | SSH 并发推送到多台 Linux 服务器，逐文件校验 SHA256，服务器上建独立环境并后台运行，全程不需要 root |

## 目录结构

```
config.py          全部配置，改东西只改这个文件
common_crawler.py  爬虫基类：下载、重试、解析、去重、入队、统计
crawler_demo.py    示例爬虫，写新爬虫照抄它
logger_setup.py    日志：单文件 + 写满清空 + 异步写盘
utils.py           URL 标准化、字节格式化、布隆参数计算、连接串拼接
dedup.py           去重：LocalBloomFilter / RedisBloomFilter
queue_store.py     队列：LocalQueue / RedisQueue
output.py          输出：FileOutput / KafkaOutput / MongoOutput
proxy_pool.py      代理：ProxyManager，按 TTL 换新
deploy.py          本地执行，把项目推到服务器并启动
run.sh             服务器上执行，准备环境 + 装依赖 + 后台启动
requirements.txt   依赖清单
```

## 快速开始

```bash
pip install -r requirements.txt
python crawler_demo.py
```

示例会抓 `quotes.toscrape.com` 的作者页：

- 结果：`data/demo_result.jsonl`，一行一条 JSON
- 日志：`logs/crawler.log`，同时打到终端（`LOG_CONSOLE = True`）
- 终端最后会输出一条汇总，例如 `任务结束 | demo | 处理 213 条 | 内容页 50 | 新入队 162 | 失败 0 | 解析异常 0`

抓完之后进程不会立刻退出：队列空了还要再等几轮，确认没有别的线程正在产出新链接。
默认等 5 轮 × 3 秒，也就是大约 15 秒没有新 URL 才结束。嫌慢就调小 `IDLE_ROUNDS` 和 `IDLE_SLEEP`。

## 运行流程

```
启动
 ├── 读 config，创建队列 / 去重 / 输出
 ├── 起始 URL 先写进去重表，再入队（避免页面里的自链接让它被抓第二遍）
 └── 启动 N 个 worker，每个循环做：
       从队列取一个 URL
       ├── 空 → 空转计数 +1，睡 IDLE_SLEEP 秒
       └── 有 → 下载（失败按 MAX_RETRY 指数退避重试，每次重试重新取代理）
               ├── 是内容页 → parse_content_page() → 写出
               └── 抽页面里的链接 → URL 标准化 → 布隆过滤 → 新链接入队

退出条件：队列空 + 没有线程正在处理 + 空转满 IDLE_ROUNDS 轮
收尾：关闭输出 → 保存去重记录 → 保存未抓队列
```

线程只会因为「确实抓完了」或者收到 `Ctrl+C` / `SIGTERM` 才退出，不会中途自己死掉。

## 写一个自己的爬虫

### 1. 改 config.py 的任务段

```python
TASK_NAME = "mysite"          # 决定 data/ 下的文件名，不同任务互不干扰

START_URLS = [
    "https://example.com/",
]

# 只有匹配这些正则的 URL 才会入队（re.match，从字符串开头匹配）
ALLOWED_URL_PATTERNS = [
    r"^https://example\.com/",
]

# 匹配这些正则的算内容页，会交给 parse_content_page 解析后输出
CONTENT_URL_PATTERNS = [
    r"^https://example\.com/article/\d+",
]
```

`re.match` 是从头匹配，所以正则要写到能覆盖整个前缀，`$` 结尾要自己加。

### 2. 新建一个入口文件

```python
# my_spider.py
import time

from bs4 import BeautifulSoup

from common_crawler import CommonCrawler


class MyCrawler(CommonCrawler):
    def parse_content_page(self, html: str, url: str):
        soup = BeautifulSoup(html, "html.parser")

        title = soup.select_one("h1")
        body = soup.select_one("div.content")

        return {
            "task_name": self.task_name,
            "url": url,
            "title": title.get_text(strip=True) if title else "",
            "text": body.get_text(" ", strip=True) if body else "",
            "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        }


if __name__ == "__main__":
    MyCrawler().start()
```

返回什么就存什么。返回 `None` 或空 dict 表示这页不输出。

### 3. 需要控制「跟哪些链接」时再覆写

默认实现是取页面里所有 `<a href>`，标准化后按 `ALLOWED_URL_PATTERNS` 过滤。
想只跟列表页的下一页、或者按 DOM 位置挑链接，就覆写这个方法：

```python
from bs4 import BeautifulSoup
from common_crawler import CommonCrawler
from utils import normalize_url


class MyCrawler(CommonCrawler):
    def parse_needed_urls(self, html, referer_url):
        soup = BeautifulSoup(html, "html.parser")
        urls = set()
        for link in soup.select("div.pager a.next, ul.list a[href]"):
            url = normalize_url(link["href"], referer_url)
            if url and self.is_allowed_url(url):
                urls.add(url)
        return urls
```

`normalize_url` 会去掉 `#锚点`、补全相对路径、统一小写、丢掉 `javascript:` / `mailto:` 这类链接。

## 配置说明

全部在 `config.py`，按用途分成六段。

### 一、爬虫任务

| 变量 | 默认 | 说明 |
|---|---|---|
| `TASK_NAME` | `"demo"` | 任务名，决定 `data/` 下文件名和 Redis key 前缀 |
| `START_URLS` | 示例站 | 起始页列表 |
| `ALLOWED_URL_PATTERNS` | 示例站 | 允许入队的 URL 正则列表 |
| `CONTENT_URL_PATTERNS` | 示例站 | 算内容页的 URL 正则列表 |
| `THREAD_NUMBER` | `8` | 并发线程数 |
| `DOWNLOAD_TIMEOUT` | `10` | 单次请求超时（秒） |
| `MAX_RETRY` | `2` | 失败后额外重试次数，`0` = 不重试 |
| `RETRY_HTTP_CODES` | `408, 429, 500, 502, 503, 504` | 这些状态码才重试，404 / 403 直接放弃 |
| `IDLE_SLEEP` | `3` | 队列空时睡几秒再看 |
| `IDLE_ROUNDS` | `5` | 连续空转这么多轮且无活跃线程，判定抓完 |
| `LOG_EVERY` | `200` | 每处理多少条打一次进度日志 |
| `RESET_DEDUP` | `False` | 启动时清空去重和队列，重跑任务时临时改 `True` |
| `USER_AGENT` | Chrome UA | 请求头里的 UA |

请求头固定带 `Accept` / `Accept-Language` / `Connection: keep-alive`，要加 Cookie 或 Referer 改 `common_crawler.py` 的 `__init__` 里的 `self.headers`。

### 二、存储后端

```python
QUEUE_BACKEND = "local"      # local | redis
DEDUP_BACKEND = "local"      # local | redis
OUTPUT_BACKEND = "file"      # file | kafka | mongo
```

三个开关互相独立，可以混着用。本地模式相关的变量：

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `"data"` | 布隆文件、队列文件、结果文件都在这里 |
| `BLOOM_CAPACITY` | `100_000_000` | 预计要过滤的 URL 总数 |
| `BLOOM_ERROR_RATE` | `0.001` | 允许的误判率 |
| `BLOOM_SAVE_EVERY` | `20_000` | 每新增多少条落盘一次 |
| `DUMP_QUEUE` | `True` | 退出时保存未抓 URL，下次接着抓 |

内存占用由 `BLOOM_CAPACITY` 和 `BLOOM_ERROR_RATE` 决定，公式是 `m = -n·ln(p)/(ln2)²`。
默认 1 亿条 / 0.1% 大约 180MB，小任务把 `BLOOM_CAPACITY` 调到实际量级（比如 100 万）只需要 2MB 左右。

### 三、日志

| 变量 | 默认 | 说明 |
|---|---|---|
| `LOG_DIR` | `"logs"` | 日志目录 |
| `LOG_FILE_NAME` | `"crawler.log"` | 日志文件名 |
| `LOG_MAX_BYTES` | `10 * 1024 * 1024` | 单文件上限 |
| `LOG_LEVEL` | `"INFO"` | 级别，排查问题改 `DEBUG` |
| `LOG_CONSOLE` | `True` | 是否同时输出到终端 |

### 四、代理

| 变量 | 默认 | 说明 |
|---|---|---|
| `PROXY_API_URL` | `""` | 取代理的接口地址，留空 = 直连 |
| `PROXY_TTL` | `30` | 代理有效秒数，`0` = 每次请求都换 |
| `PROXY_USER` | `""` | 代理认证账号 |
| `PROXY_PASSWORD` | `""` | 代理认证密码 |

### 五、SSH 部署

| 变量 | 默认 | 说明 |
|---|---|---|
| `SERVERS` | 示例 | 服务器列表，每项一个 dict |
| `REMOTE_DIR` | `""` | 远程目录，留空 = `/home/<user>/minispider` |
| `ENTRY_FILE` | `"crawler_demo.py"` | 部署完在服务器上跑哪个文件 |

### 六、环境变量覆盖

不想把密码写进文件时用环境变量，优先级高于文件里的值：

```bash
export SSH_PASSWORD=xxx          # SERVERS 里没写 password 的项都用它
export REDIS_PASSWORD=xxx
export MONGO_USER=xxx
export MONGO_PASSWORD=xxx
export KAFKA_SASL_USER=xxx
export KAFKA_SASL_PASSWORD=xxx
export PROXY_PASSWORD=xxx
```

## 后端组合

| 场景 | 配置 |
|---|---|
| 单机练手，不装任何服务 | `local` + `local` + `file` |
| 单机多进程，共用队列和去重 | `QUEUE_BACKEND="redis"`、`DEDUP_BACKEND="redis"` |
| 多台机器一起抓同一个任务 | 同上，`REDIS_URL` 指向同一台 |
| 结果要进 Kafka / MongoDB | `OUTPUT_BACKEND="kafka"` 或 `"mongo"` |

### Redis

`REDIS_URL` 填了只用它，下面四项忽略：

```python
REDIS_URL = "redis://:密码@127.0.0.1:6379/0"
# 或者
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = ""
```

队列和去重共用一个连接。key 分别是 `<TASK_NAME>_url_queue` 和 `<TASK_NAME>_url_bloom`。

### MongoDB

```python
MONGO_URL = "mongodb://user:pass@host:27017/crawler?authSource=admin"
# 或者用 MONGO_HOST / MONGO_PORT / MONGO_USER / MONGO_PASSWORD
MONGO_DB = "crawler"
MONGO_COLLECTION = ""        # 留空 = <任务名>_result
```

### Kafka

```python
KAFKA_SERVERS = "127.0.0.1:9092"    # 多个用逗号分隔
KAFKA_TOPIC = "crawl_html"
KAFKA_SASL_USER = ""                 # 填了就走 SASL_PLAINTEXT / PLAIN
KAFKA_SASL_PASSWORD = ""
```

`kafka-python`、`pymongo`、`redis` 都是按需 import，只用本地模式不用装。

## 数据文件

本地模式下，一个任务在 `data/` 里对应三个文件：

```
data/demo_bloom.bin     去重记录，二进制，重启自动加载
data/demo_queue.json    未抓 URL，退出时写入，下次启动读取后删除
data/demo_result.jsonl  抓取结果，一行一条 JSON，追加写
```

重跑同一个任务有两种方式：

- `RESET_DEDUP = True`，启动时清空去重和队列
- 或者手动删掉 `data/demo_bloom.bin` 和 `data/demo_queue.json`

只删结果文件不删布隆文件的话，所有 URL 都算「见过」，会直接退出、什么都不抓。

## 日志

只有一个文件 `logs/crawler.log`，不会出现 `.1` / `.2` 轮转文件。写满 10MB 时清空重写，文件开头留一行标记：

```
===== 2026-11-15 13:25:01 日志达到上限 10MB，已清空重写 =====
```

写盘在独立线程里，并且攒一批再写，抓取线程只做一次入队（实测约 15 微秒/条），不会被磁盘 IO 卡住。
日志队列上限 5 万条，写盘跟不上时丢弃并计数，退出时在日志里写明丢了多少 —— 宁可丢日志，也不拖慢抓取。

格式：

```
2026-11-15 13:25:01.482 | INFO | pid 8412 | worker-3 | common_crawler:_log_progress:235 - 进度 | 已处理 200 | 内容页 87 | 新入队 156 | 失败 2 | 队列剩余 43
```

时间、级别、进程号、线程名、代码位置、消息。多线程时靠线程名区分是哪一路。

服务器上查看：

```bash
tail -f logs/crawler.log                              # 实时跟
tail -n 200 logs/crawler.log                          # 最后 200 行
grep -E "WARNING|ERROR" logs/crawler.log | tail -n 50  # 只看异常
grep "任务结束" logs/crawler.log                       # 看最终统计
```

单文件会覆盖旧内容，需要保留历史就定时 `cp logs/crawler.log logs/crawler-$(date +%m%d-%H%M).log`。

## 代理

默认不用代理。`PROXY_API_URL` 填了之后走 ProxyManager 模式：

```
启动先取一个代理
每次请求前检查计时
 ├── 没超过 PROXY_TTL 秒 → 继续用当前这个
 └── 超过了            → 取新代理，重置计时
```

多线程共用一个 ProxyManager，换代理在锁里做。取代理失败会重试 3 次（连接 5 秒 / 读 15 秒超时），
还不行就继续用手里的，手里没有就本次直连 —— 不会卡住抓取。

### 接口返回格式

两种都认：

```json
{"code": 200, "results": ["1.2.3.4:8888", "5.6.7.8:3128"]}
```

随机取一个。或者纯文本，取第一行：

```
1.2.3.4:8888
```

没写协议头会自动补 `http://`。

### 账号密码代理

隧道代理常见这种：接口只返回 `ip:port`，认证靠固定的账号密码。

```python
PROXY_USER = "nsphs4"
PROXY_PASSWORD = "q7ifwf9t"      # 也可以 export PROXY_PASSWORD=xxx
```

会自动拼成 `http://nsphs4:q7ifwf9t@1.2.3.4:8000`，密码里的 `@` `:` 等字符会 URL 转义，
日志里显示为 `http://***@1.2.3.4:8000`。两个都填才生效。

### 完全自定义

改 `proxy_pool.py` 的 `custom_fetch_proxy()`，查库、调付费接口、多接口轮询、本地拨号都行，返回 `{"http": ..., "https": ...}` 或 `None`：

```python
def custom_fetch_proxy():
    resp = requests.get("http://my-api/proxies?n=1", timeout=5)
    proxy = resp.json()["data"][0]
    return {"http": f"http://{proxy}", "https": f"http://{proxy}"}
```

自定义函数优先于 `PROXY_API_URL`。

## 部署到 Linux 服务器

`deploy.py` 在本地跑，把当前目录推到服务器并启动。服务器只要有 `python3` 和一个能登录的普通账号，
不需要 root，不需要 Docker，不需要预装任何 Python 包。

### 1. 填服务器列表

`SERVERS` 一台也行，多台也行，部署时并发推送：

```python
SERVERS = [
    {"host": "43.142.34.238", "user": "ubuntu", "password": "密码"},
    {"host": "1.2.3.5",       "user": "ubuntu", "password": "密码"},
    {"host": "1.2.3.6",       "user": "root",   "key_file": "C:/Users/jn/.ssh/id_rsa"},
]

REMOTE_DIR = ""                  # 留空 = /home/<user>/minispider
ENTRY_FILE = "crawler_demo.py"
```

字段：`host` 必填；`user` 默认 `ubuntu`；`port` 默认 `22`；认证用 `password` 或 `key_file`，二选一。
不想把密码写在文件里，就用 `export SSH_PASSWORD=xxx`，列表里不写 `password` 的项都用它。
缺 `host` 或两个认证方式都没填，启动时直接报错并指明是第几项。

本机需要 `paramiko`：`pip install paramiko`。

### 2. 推送

```bash
python deploy.py
```

每台机器做四件事：

1. SSH 连上去
2. 递归上传当前目录，跳过 `logs` `data` `__pycache__` `.git` `.venv` `.uv` 等
3. 逐个比对远程文件大小和 SHA256（分批调 `sha256sum`，不是一文件一次）
4. 执行 `bash run.sh <ENTRY_FILE>`

上传时文本文件自动把 CRLF 转成 LF，否则服务器上 bash 会报 `\r: command not found`。判断方式是看内容里有没有 NUL 字节，不看后缀，所以 `.whl`、`.png` 这类二进制不会被改坏，`Dockerfile` 这种没后缀的文本也能正确转换。

输出：

```
[部署] 2 台，入口 crawler_demo.py
       ubuntu@43.142.34.238:22    -> /home/ubuntu/minispider
       ubuntu@1.2.3.5:22          -> /home/ubuntu/minispider

[43.142.34.238] 连接成功
[43.142.34.238] 传输 13 个文件完成（86.20 KB），校验通过
[43.142.34.238] 依赖安装完成（7 个）
[43.142.34.238] 运行成功，PID 8412
[43.142.34.238] 停止：bash run.sh stop
[43.142.34.238] 日志：tail -f /home/ubuntu/minispider/logs/crawler.log
[1.2.3.5] 连接成功
...

[汇总] 成功 2 台 / 共 2 台
```

一台失败不影响其他台，失败的会在汇总里列出来，退出码为 1。校验不通过时会逐条打印是哪个文件、什么原因。

### 3. 服务器上的 run.sh

```bash
bash run.sh              # 启动 crawler_demo.py（已在跑就先停旧的）
bash run.sh xxx.py       # 启动指定文件
bash run.sh stop         # 停止
```

正常输出：

```
依赖安装完成（7 个）
运行成功，PID 8412
停止：bash run.sh stop
日志：tail -f /home/ubuntu/minispider/logs/crawler.log
```

它做的事：

- **准备环境**：优先 `python3 -m venv .venv`；发行版自带 venv/pip 不可用（精简镜像常见）时，把 uv 下到 `.uv/bin/`，用 `uv venv` + `uv pip install`。两条路都不碰系统 Python，也不需要 root。首次部署会多一行 `创建运行环境 .venv`。
- **装依赖**：默认走阿里云源。`requirements.txt` 没变就跳过安装，重复部署一两秒。安装过程不刷屏，失败才把最后 20 行打出来。
- **后台运行**：`nohup .venv/bin/python -u <入口>`，stdout 丢 `/dev/null`（日志由程序自己写，不会出现两份），进程号记在 `.crawler.pid`。
- **启动自检**：起完等 2 秒确认进程活着，退出了就打印日志尾部 20 行。

服务器上的目录结构：

```
.venv/        虚拟环境和依赖
.uv/          只有缺 venv 时才出现：uv 本体、缓存、它下载的 Python
.venv/.installer    记录用的哪种方式（pip / uv）
.venv/.requirements  requirements.txt 的副本，用来判断要不要重装
logs/ data/   日志和结果
.crawler.pid  进程号
```

整个目录是自包含的，删掉不会在系统里留东西。

几个细节：

- 停止前会核对 `/proc/<pid>/cmdline` 确实是 python，防止服务器重启后 pid 被别的进程占用导致误杀。
- 停止先发 `TERM`，给 10 秒保存队列和去重记录，还不退才 `KILL`。
- 换源：`PIP_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple bash run.sh`
- 换解释器：`PYTHON=python3.11 bash run.sh`

### 多台一起抓同一个任务

`local` 模式的队列和去重都在各自进程内存里，多台同时跑就是各抓各的、互相重复。
要把 `QUEUE_BACKEND` 和 `DEDUP_BACKEND` 都切成 `redis`，`REDIS_URL` 指向所有机器都能访问的同一台 Redis。

## 常见问题

**抓完不退出，还在等？**
队列空不等于抓完，别的线程可能正在解析页面、马上要入队新链接。所以线程会空转等 `IDLE_ROUNDS` 轮，
确认「队列空 + 没有线程在干活」才退出。调小 `IDLE_ROUNDS` / `IDLE_SLEEP` 可以缩短等待。

**本地模式能不能多开几个进程？**
不能。`local` 的队列和去重都在各自进程内存里，多开就是各抓各的。要多进程或多机，两个后端都切 `redis`。

**改了 config 需要重新部署吗？**
需要，`config.py` 也是上传的文件之一。`python deploy.py` 推完会自动重启，新配置立即生效。

**怎么重跑一个已经抓过的任务？**
`RESET_DEDUP = True` 启动一次，或者删掉 `data/<task>_bloom.bin` 和 `data/<task>_queue.json`。
注意只删结果文件是不够的，布隆记录还在，程序会认为都抓过了。

**日志里说丢了多少条，要紧吗？**
说明写盘跟不上生产速度，通常是 `LOG_EVERY` 太小或者 `LOG_LEVEL=DEBUG` 每条 URL 都打日志。
把级别调到 `WARNING`，或者调大 `LOG_EVERY`。

**结果文件越来越大怎么办？**
`file` 模式是纯追加，没有切分。数据量大就切到 `kafka` 或 `mongo`，或者在 `output.py` 的 `FileOutput` 里按天分文件。

**部署时报「端口连不上」/「认证失败」？**
端口连不上检查 IP、`port`、安全组；认证失败检查 `user` / `password` / `key_file`。
远程没写权限时换 `REMOTE_DIR`（比如 `/home/<user>/` 下面），不要往 `/root` 或 `/opt` 推。

**怎么强制停止？**
`bash run.sh stop`，或者前台运行时 `Ctrl+C`。两种都会走收尾：关闭输出、保存去重记录、保存未抓队列。
