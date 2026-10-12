#!/usr/bin/env python3
"""Project-local single-node HDFS; starts daemons directly, without ssh/sshd."""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[1]
URI = "hdfs://127.0.0.1:19000/retail_project"


def environment():
    runtime = ROOT / ".runtime"
    hadoop = runtime / "hadoop-3.4.2"
    if not hadoop.exists():
        raise SystemExit("Run bash scripts/setup.sh first (Hadoop missing)")
    conf = runtime / "hdfs-conf"
    conf.mkdir(parents=True, exist_ok=True)
    for filename in ["log4j.properties", "hadoop-env.sh"]:
        source = hadoop / "etc/hadoop" / filename
        if source.exists():
            shutil.copy2(source, conf / filename)
    ident = hashlib.sha256(str(ROOT).encode()).hexdigest()[:10]
    # Hadoop shell classpath scripts are sensitive to spaces. Temporary aliases are local only.
    alias_root = Path(tempfile.gettempdir()) / f"retail-hdfs-{os.getuid()}-{ident}"
    alias_root.mkdir(exist_ok=True)
    java = Path(os.environ.get("JAVA_HOME", runtime / "java_home"))
    for folder in ["logs", "pids", "hdfs/tmp"]:
        (runtime / folder).mkdir(parents=True, exist_ok=True)
    for name, target in [("hadoop", hadoop), ("java", java), ("conf", conf),
                         ("logs", runtime / "logs"), ("pids", runtime / "pids"), ("runtime", runtime)]:
        link = alias_root / name
        if not link.is_symlink():
            link.symlink_to(target)
    props = {
        "core-site.xml": {"fs.defaultFS": "hdfs://127.0.0.1:19000", "hadoop.tmp.dir": str(alias_root / "runtime/hdfs/tmp")},
        "hdfs-site.xml": {
            "dfs.replication": "1", "dfs.namenode.name.dir": str(alias_root / "runtime/hdfs/name"),
            "dfs.datanode.data.dir": str(alias_root / "runtime/hdfs/data"),
            "dfs.namenode.rpc-address": "127.0.0.1:19000", "dfs.namenode.http-address": "127.0.0.1:19870",
            "dfs.datanode.address": "127.0.0.1:19866", "dfs.datanode.ipc.address": "127.0.0.1:19867",
            "dfs.datanode.http.address": "127.0.0.1:19864", "dfs.datanode.hostname": "127.0.0.1",
            "dfs.client.use.datanode.hostname": "true", "dfs.permissions.enabled": "false"}}
    for filename, values in props.items():
        text = "<configuration>" + "".join(f"<property><name>{k}</name><value>{escape(v)}</value></property>" for k, v in values.items()) + "</configuration>"
        (conf / filename).write_text(text)
    for folder in ["logs", "pids", "hdfs/tmp"]:
        (runtime / folder).mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, HADOOP_HOME=str(alias_root / "hadoop"), JAVA_HOME=str(alias_root / "java"),
               HADOOP_CONF_DIR=str(alias_root / "conf"), HADOOP_LOG_DIR=str(alias_root / "logs"),
               HADOOP_PID_DIR=str(alias_root / "pids"), HADOOP_HEAPSIZE_MAX="512")
    return alias_root / "hadoop/bin/hdfs", env


def execute(arguments, capture=False, check=True):
    command, env = environment()
    return subprocess.run([str(command), *arguments], env=env, check=check, text=True,
                          capture_output=capture)


def start():
    if not (ROOT / ".runtime/hdfs/name/current/VERSION").exists():
        execute(["namenode", "-format", "-nonInteractive"])
    execute(["--daemon", "start", "namenode"])
    execute(["--daemon", "start", "datanode"])
    for _ in range(30):
        result = execute(["dfsadmin", "-report"], capture=True, check=False)
        if result.returncode == 0 and "Live datanodes (1)" in result.stdout:
            # On restart HDFS waits for previous block reports before permitting overwrites.
            execute(["dfsadmin", "-safemode", "wait"])
            execute(["dfs", "-mkdir", "-p", URI])
            print("HDFS ready: " + URI)
            return
        time.sleep(1)
    raise RuntimeError("HDFS failed to become ready; inspect .runtime/logs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["start", "stop", "status", "snapshot"])
    args = parser.parse_args()
    if args.action == "start":
        start()
    elif args.action == "stop":
        execute(["--daemon", "stop", "datanode"], check=False)
        execute(["--daemon", "stop", "namenode"], check=False)
    elif args.action == "status":
        execute(["dfsadmin", "-report"])
    else:
        report = execute(["dfsadmin", "-report"], capture=True).stdout
        listing = execute(["dfs", "-ls", "-R", URI], capture=True).stdout
        fsck = execute(["fsck", "/retail_project"], capture=True).stdout
        target = ROOT / "results/hdfs_evidence.json"
        target.parent.mkdir(exist_ok=True)
        target.write_text(json.dumps({"uri": URI, "deployment": "single NameNode / single DataNode, local Spark",
                                     "report": report, "listing": listing, "fsck": fsck}, indent=2))
