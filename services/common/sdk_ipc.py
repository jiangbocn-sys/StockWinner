"""
SDK 子进程 IPC 协议定义

协议格式：
  [4 bytes: 消息长度, big-endian] [N bytes: JSON 数据]

请求格式：
  {"method": "query_kline", "args": {...}, "request_id": "uuid"}

响应格式：
  {"request_id": "uuid", "success": true/false, "result": <base64-encoded-pickle>, "error": "msg"}
  {"request_id": "uuid", "success": true/false, "result": {"__none__": true}}  # None 值

控制消息（子进程 → 主进程）：
  {"event": "sdk_ready", "pid": 12345}
  {"event": "sdk_error", "message": "..."}
  {"event": "sdk_shutdown"}
"""

import struct
import json
import base64
import pickle
import os
import uuid
from typing import Any, Dict

SOCKET_PATH = "/tmp/stockwinner_sdk.sock"

# 消息最大大小 50MB
MAX_MESSAGE_SIZE = 50 * 1024 * 1024


def encode_response(result: Any) -> str:
    """将 Python 对象（含 DataFrame）序列化为可传输字符串"""
    if result is None:
        return json.dumps({"__none__": True})
    try:
        pickled = pickle.dumps(result)
        encoded = base64.b64encode(pickled).decode("ascii")
        return encoded
    except Exception as e:
        # 如果 pickle 失败，尝试返回 repr
        return json.dumps({"__repr__": repr(result)})


def decode_response(data: str) -> Any:
    """将传输字符串反序列化为 Python 对象"""
    # 先尝试 base64-encoded pickle（最常见）
    try:
        pickled = base64.b64decode(data.encode("ascii"))
        return pickle.loads(pickled)
    except Exception:
        pass
    # 再尝试 JSON
    try:
        obj = json.loads(data)
        if isinstance(obj, dict) and obj.get("__none__"):
            return None
        if isinstance(obj, dict) and obj.get("__repr__"):
            return obj["__repr__"]
        return obj
    except Exception:
        return data


def send_message(sock, data: dict):
    """发送消息到 Unix socket（带长度前缀）"""
    msg = json.dumps(data).encode("utf-8")
    header = struct.pack("!I", len(msg))  # 4 bytes, big-endian unsigned int
    sock.sendall(header + msg)


def recv_message(sock) -> dict:
    """从 Unix socket 接收消息（带长度前缀）"""
    # 读取 4 字节长度
    header = _recv_exact(sock, 4)
    if not header:
        raise ConnectionError("连接已关闭")
    msg_len = struct.unpack("!I", header)[0]
    if msg_len > MAX_MESSAGE_SIZE:
        raise ValueError(f"消息过大: {msg_len} bytes")
    # 读取消息体
    body = _recv_exact(sock, msg_len)
    if not body:
        raise ConnectionError("连接已关闭（消息体不完整）")
    return json.loads(body.decode("utf-8"))


def _recv_exact(sock, n_bytes: int) -> bytes:
    """精确读取 n_bytes 数据"""
    data = bytearray()
    while len(data) < n_bytes:
        chunk = sock.recv(n_bytes - len(data))
        if not chunk:
            return bytes(data) if data else b""
        data.extend(chunk)
    return bytes(data)
