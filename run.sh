#!/usr/bin/env bash
#
# 服务器上的启动脚本：准备 .venv、装依赖、nohup 后台运行。
# 不需要 root，环境、缓存、依赖全在脚本所在目录里。
#
#   bash run.sh             启 crawler_demo.py（已在跑先停旧的）
#   bash run.sh xxx.py      启指定文件
#   bash run.sh stop        停止
#
# 可用环境变量：PYTHON=python3.11  PIP_INDEX=https://...

set -u
cd "$(dirname "$0")" || exit 1

ENTRY="${1:-crawler_demo.py}"
PIP_INDEX="${PIP_INDEX:-https://mirrors.aliyun.com/pypi/simple/}"
VENV=".venv"
UV_DIR="$PWD/.uv"
MARK="$VENV/.installer"
PID_FILE=".crawler.pid"

stop() {
  local quiet="${1:-}"

  if [ ! -f "$PID_FILE" ]; then
    [ -n "$quiet" ] || echo "没有在运行"
    return
  fi

  pid="$(cat "$PID_FILE" 2>/dev/null)"
  # 服务器重启后 pid 可能被别的进程占了，核对一下命令行再动手
  if [ -n "$pid" ] && [ -r "/proc/$pid/cmdline" ] \
     && ! tr '\0' ' ' < "/proc/$pid/cmdline" | grep -Fq python; then
    pid=""
  fi

  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null
    # 给它 10 秒存队列和去重记录
    i=0
    while kill -0 "$pid" 2>/dev/null && [ $i -lt 10 ]; do sleep 1; i=$((i+1)); done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
    echo "已停止"
  else
    [ -n "$quiet" ] || echo "没有在运行"
  fi
  rm -f "$PID_FILE"
}

if [ "$ENTRY" = "stop" ]; then
  stop
  exit 0
fi

stop quiet

# 找 python，各发行版命名不一样
if [ -z "${PYTHON:-}" ]; then
  for c in python3 python3.13 python3.12 python3.11 python3.10 python; do
    if command -v "$c" >/dev/null 2>&1; then PYTHON="$c"; break; fi
  done
fi
[ -n "${PYTHON:-}" ] || { echo "找不到 python3"; exit 1; }

# 准备环境：优先用发行版自带的 venv，没有就用 uv（都装在当前目录，不碰系统）
MODE=""
if [ -f "$MARK" ] && [ -x "$VENV/bin/python" ]; then
  MODE="$(cat "$MARK")"
fi

if [ -z "$MODE" ]; then
  echo "创建运行环境 $VENV"
  if "$PYTHON" -m venv "$VENV" >/dev/null 2>&1 \
     && "$VENV/bin/python" -m pip --version >/dev/null 2>&1; then
    MODE="pip"
  else
    rm -rf "$VENV"
    MODE="uv"
  fi
fi

if [ "$MODE" = "uv" ]; then
  export UV_CACHE_DIR="$UV_DIR/cache"
  export UV_PYTHON_INSTALL_DIR="$UV_DIR/python"

  UV=""
  if command -v uv >/dev/null 2>&1; then
    UV="$(command -v uv)"
  elif [ -x "$UV_DIR/bin/uv" ]; then
    UV="$UV_DIR/bin/uv"
  else
    echo "下载 uv 到 $UV_DIR/bin"
    mkdir -p "$UV_DIR/bin"
    if command -v curl >/dev/null 2>&1; then
      curl -LsSf https://astral.sh/uv/install.sh \
        | env UV_INSTALL_DIR="$UV_DIR/bin" UV_NO_MODIFY_PATH=1 sh >/dev/null 2>&1 || true
    elif command -v wget >/dev/null 2>&1; then
      wget -qO- https://astral.sh/uv/install.sh \
        | env UV_INSTALL_DIR="$UV_DIR/bin" UV_NO_MODIFY_PATH=1 sh >/dev/null 2>&1 || true
    fi
    [ -x "$UV_DIR/bin/uv" ] && UV="$UV_DIR/bin/uv"
  fi

  if [ -z "$UV" ]; then
    echo "uv 下载失败（访问不了 astral.sh）"
    echo "可以本机下好 uv 传到 $UV_DIR/bin/uv，或让管理员装 python3-venv"
    exit 1
  fi

  if [ ! -x "$VENV/bin/python" ]; then
    rm -rf "$VENV"
    "$UV" venv "$VENV" --python "$PYTHON" --clear >/dev/null || { echo "创建虚拟环境失败"; exit 1; }
  fi
fi

[ -f "$MARK" ] || { mkdir -p "$VENV"; echo "$MODE" > "$MARK"; }

# 装依赖，requirements.txt 没变就跳过。安装过程不打印，失败才把最后几行抛出来
[ -f requirements.txt ] || { echo "缺少 requirements.txt"; exit 1; }

COUNT="$(grep -cv -E '^[[:space:]]*(#|$)' requirements.txt 2>/dev/null || true)"
STAMP="$VENV/.requirements"

install_deps() {
  if [ "$MODE" = "uv" ]; then
    "$UV" pip install -r requirements.txt --python "$VENV/bin/python" --index-url "$PIP_INDEX"
  else
    "$VENV/bin/python" -m pip install -r requirements.txt -i "$PIP_INDEX" \
      -q --disable-pip-version-check
  fi
}

if [ -f "$STAMP" ] && cmp -s requirements.txt "$STAMP"; then
  echo "依赖无变化，跳过安装"
elif out="$(install_deps 2>&1)"; then
  cp -f requirements.txt "$STAMP"
  echo "依赖安装完成（$COUNT 个）"
else
  printf '%s\n' "$out" | tail -n 20
  echo "依赖安装失败"
  exit 1
fi

[ -f "$ENTRY" ] || { echo "找不到入口文件 $ENTRY"; exit 1; }
mkdir -p logs data

# stdout 丢 /dev/null，日志程序自己写
nohup "$VENV/bin/python" -u "$ENTRY" </dev/null >/dev/null 2>&1 &
echo $! > "$PID_FILE"

sleep 2
if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  rm -f "$PID_FILE"
  echo "启动失败，日志最后几行："
  tail -n 20 logs/crawler.log 2>/dev/null
  exit 1
fi

echo "运行成功，PID $(cat "$PID_FILE")"
echo "停止：bash run.sh stop"
echo "日志：tail -f $PWD/logs/crawler.log"
