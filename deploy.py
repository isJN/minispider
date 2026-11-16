# -*- coding: utf-8 -*-
"""
把当前目录并发推到 config.SERVERS 里的每台服务器，然后各自执行 run.sh。

    python deploy.py

每台机器上：SSH 连上 → 递归上传（跳过 logs/data/.venv 等）→ 逐个比对大小和
SHA256 → 执行 bash run.sh <ENTRY_FILE>。多台是并发推的，失败的那台不影响其他台。
"""

import fnmatch
import hashlib
import os
import posixpath
import shlex
import socket
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import config
from utils import human_bytes, looks_like_text

LOCAL_DIR = Path(__file__).resolve().parent

SKIP_DIRS = {"__pycache__", ".git", ".idea", "logs", "data",
             "venv", ".venv", ".uv", ".uvbin", "node_modules"}
SKIP_FILES = (".crawler.pid", "crawler.log", "*.pyc", "*.pyo")

_print_lock = threading.Lock()


@dataclass(frozen=True)
class Server:
    """config.SERVERS 里的一项。"""

    host: str
    port: int = 22
    user: str = "ubuntu"
    password: str = ""
    key_file: str = ""

    @property
    def label(self) -> str:
        return f"{self.user}@{self.host}:{self.port}"


@dataclass
class LocalFile:
    """待上传文件，顺带记下实际写进去的字节数和哈希，校验时用。"""

    path: Path
    remote: str
    relative: str
    written_size: int = 0
    written_sha: str = ""


def load_servers() -> list[Server]:
    """SERVERS 里的字典转成 Server，顺手校验必填项。"""
    servers = []

    for index, item in enumerate(config.SERVERS, 1):
        host = str(item.get("host", "")).strip()
        if not host:
            raise ValueError(f"SERVERS 第 {index} 项没填 host")

        server = Server(
            host=host,
            port=int(item.get("port", 22)),
            user=str(item.get("user", "ubuntu")),
            password=str(item.get("password", "")),
            key_file=str(item.get("key_file", "")),
        )
        if not server.password and not server.key_file:
            raise ValueError(f"SERVERS 第 {index} 项（{server.host}）password 和 key_file 都没填")
        servers.append(server)

    return servers


class Deployer:
    """一台服务器的部署流程。"""

    def __init__(self, server: Server):
        self.server = server
        self.prefix = f"[{server.host}]"
        self.remote_dir = config.REMOTE_DIR or f"/home/{server.user}/minispider"

        self.client = None
        self.sftp = None
        self._made_dirs = set()

    def say(self, message: str) -> None:
        """多台并发时加主机前缀，加锁保证一行不被打断。"""
        with _print_lock:
            print(f"{self.prefix} {message}")

    def run(self) -> int:
        files = self.collect_files()
        if not files:
            self.say("失败：本地没有可上传的文件")
            return 1

        total_bytes = sum(f.path.stat().st_size for f in files)

        try:
            self.connect()
            self.upload(files)
            self.verify(files)
            self.say(f"传输 {len(files)} 个文件完成（{human_bytes(total_bytes)}），校验通过")
            self.execute()
        except Exception as exc:
            self.say(f"失败：{self.explain(exc)}")
            return 1
        finally:
            self.close()

        return 0

    # ---------------------------------------------------------------- 连接

    def connect(self) -> None:
        try:
            import paramiko
        except ImportError:
            raise RuntimeError("缺少依赖：pip install paramiko") from None

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        options = {
            "hostname": self.server.host,
            "port": self.server.port,
            "username": self.server.user,
            "timeout": 15,
            "allow_agent": False,
            "look_for_keys": False,
        }
        if self.server.key_file:
            options["key_filename"] = os.path.expanduser(self.server.key_file)
        else:
            options["password"] = self.server.password

        client.connect(**options)
        self.client = client
        self.sftp = client.open_sftp()
        self.say("连接成功")

    def close(self) -> None:
        for obj in (self.sftp, self.client):
            try:
                if obj is not None:
                    obj.close()
            except Exception:
                pass
        self.sftp = None
        self.client = None

    # ---------------------------------------------------------------- 上传

    def collect_files(self) -> list[LocalFile]:
        """扫出要上传的文件，按相对路径排序。"""
        files = []

        for root, dirs, names in os.walk(LOCAL_DIR):
            dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)

            for name in sorted(names):
                if any(fnmatch.fnmatch(name, pattern) for pattern in SKIP_FILES):
                    continue
                local = Path(root) / name
                if not local.is_file():
                    continue

                relative = local.relative_to(LOCAL_DIR).as_posix()
                remote = posixpath.join(self.remote_dir, relative)
                files.append(LocalFile(path=local, remote=remote, relative=relative))

        return files

    def upload(self, files: list[LocalFile]) -> None:
        for item in files:
            data = item.path.read_bytes()

            # 文本统一成 LF，否则服务器上 bash 会报 \r: command not found
            if b"\r\n" in data and looks_like_text(data):
                data = data.replace(b"\r\n", b"\n")

            self._mkdir(posixpath.dirname(item.remote))

            with self.sftp.open(item.remote, "wb") as handle:
                handle.write(data)

            if item.path.suffix.lower() == ".sh":
                self.sftp.chmod(item.remote, 0o755)

            # 记下实际写进去的内容，校验跟这个比（CRLF 转过之后和原文件不一样长）
            item.written_size = len(data)
            item.written_sha = hashlib.sha256(data).hexdigest()

    def _mkdir(self, remote_dir: str) -> None:
        if not remote_dir or remote_dir in self._made_dirs:
            return
        self._exec(f"mkdir -p {shlex.quote(remote_dir)}")
        self._made_dirs.add(remote_dir)

    # ---------------------------------------------------------------- 校验

    def verify(self, files: list[LocalFile]) -> None:
        remote_sha = self._remote_sha256([item.remote for item in files])
        bad = []

        for item in files:
            try:
                actual_size = self.sftp.stat(item.remote).st_size
            except IOError:
                bad.append((item.relative, "远程文件不存在"))
                continue

            actual_sha = remote_sha.get(item.remote, "")

            if actual_size != item.written_size:
                bad.append((item.relative, f"大小不一致 {actual_size} != {item.written_size}"))
            elif item.written_sha and actual_sha and actual_sha != item.written_sha:
                bad.append((item.relative, "SHA256 不一致"))

        if bad:
            for relative, reason in bad:
                self.say(f"  {relative}：{reason}")
            raise RuntimeError(f"{len(bad)} 个文件校验不通过")

    def _remote_sha256(self, paths: list[str]) -> dict[str, str]:
        """一批一次算完，别一个文件发一条命令。"""
        result = {}
        chunk_size = 60

        for start in range(0, len(paths), chunk_size):
            chunk = paths[start:start + chunk_size]
            command = "sha256sum " + " ".join(shlex.quote(p) for p in chunk)
            _, stdout, _ = self.client.exec_command(command, timeout=120)
            output = stdout.read().decode("utf-8", "replace")

            for line in output.splitlines():
                parts = line.split(None, 1)
                if len(parts) == 2:
                    digest, path = parts
                    result[path.strip().lstrip("*")] = digest.lower()

        return result

    # ---------------------------------------------------------------- 跑 run.sh

    def execute(self) -> None:
        # run.sh 自己会先停旧的、装依赖、再起新的
        command = (
            f"cd {shlex.quote(self.remote_dir)} && "
            f"bash ./run.sh {shlex.quote(config.ENTRY_FILE)}"
        )

        # get_pty=True 把 stderr 并进 stdout，输出顺序不会乱
        _, stdout, _ = self.client.exec_command(command, get_pty=True)
        for line in iter(stdout.readline, ""):
            text = line.rstrip("\r\n")
            if text:
                self.say(text)

        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            raise RuntimeError(f"run.sh 返回 {exit_code}")

    # ---------------------------------------------------------------- 杂项

    def _exec(self, command: str) -> str:
        _, stdout, _ = self.client.exec_command(command, timeout=60)
        return stdout.read().decode("utf-8", "replace")

    @staticmethod
    def explain(exc: Exception) -> str:
        import paramiko

        if isinstance(exc, paramiko.AuthenticationException):
            return "认证失败，检查 SERVERS 里的 user / password / key_file"
        if isinstance(exc, paramiko.ssh_exception.NoValidConnectionsError):
            return "端口连不上，检查 IP、port 和安全组"
        if isinstance(exc, paramiko.SSHException):
            return f"SSH 握手失败：{exc}"
        if isinstance(exc, (TimeoutError, socket.timeout)):
            return "连接超时，检查服务器是否开机、22 端口是否放行"
        if isinstance(exc, FileNotFoundError):
            return f"本地文件缺失：{exc}"
        if isinstance(exc, PermissionError):
            return f"远程没有写权限：{exc}，换个 REMOTE_DIR"
        return f"{type(exc).__name__}: {exc}"


def main() -> int:
    try:
        servers = load_servers()
    except ValueError as exc:
        print(f"失败：{exc}")
        return 1

    if not servers:
        print("失败：请先在 config.py 的 SERVERS 里填服务器")
        return 1

    if not (LOCAL_DIR / "run.sh").is_file():
        print("失败：本地缺少 run.sh")
        return 1

    print(f"[部署] {len(servers)} 台，入口 {config.ENTRY_FILE}")
    for server in servers:
        remote = config.REMOTE_DIR or f"/home/{server.user}/minispider"
        print(f"       {server.label:<26} -> {remote}")
    print()

    results: dict[str, int] = {}

    with ThreadPoolExecutor(max_workers=len(servers)) as pool:
        futures = {pool.submit(Deployer(server).run): server for server in servers}
        for future in as_completed(futures):
            server = futures[future]
            try:
                results[server.host] = future.result()
            except Exception as exc:
                print(f"[{server.host}] 异常：{exc}")
                results[server.host] = 1

    ok = [host for host, code in results.items() if code == 0]
    bad = [host for host, code in results.items() if code != 0]

    print()
    print(f"[汇总] 成功 {len(ok)} 台 / 共 {len(results)} 台")
    for host in bad:
        print(f"       {host} 失败，看上面 [host] 开头的日志")

    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
