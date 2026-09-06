#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OKX 实时成交价监听 + 多层次价位计算 + 动态止盈 + 钉钉提醒

版本: v4.0
更新: 2026-09-06

核心逻辑：
- 价格层级：区间最高 → 回撤系列 → 高位中间 → 回调系列 → 中间 → 过渡系列 → 低位中间 → 极限系列 → 最低
- 买点：回撤2下、回调2下、过渡2下、极限2下
- 卖点：对应回撤上、回调上、过渡上、极限上
- 动态止盈表：根据盈利百分比自动调整止损比例
- 异常分类：合规限制 → 黑名单；订单限制（如超过100USDT）→ 待观察列表并剔除买入队列
- 买入优化：价格必须低于买点，并以当前最低价为参考进行最优买入
"""

import base64
import csv
import hmac
import hashlib
import json
import os
import socket
import ssl
import struct
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple
from dotenv import load_dotenv

load_dotenv()

# ==================== 配置常量 ====================
PROGRAM_NAME = "okx_ding_ok"
VERSION = "v4.0"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, f"{PROGRAM_NAME}_config.xlsx")
BLACKLIST_FILE = os.path.join(BASE_DIR, f"{PROGRAM_NAME}_blacklist.csv")
OBSERVE_LIST_FILE = os.path.join(BASE_DIR, f"{PROGRAM_NAME}_observe_list.csv")  # 待观察列表
DATA_DIR = os.path.join(BASE_DIR, f"{PROGRAM_NAME}_data")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

TRADE_FILE = os.path.join(DATA_DIR, f"{PROGRAM_NAME}_tradeRecord.csv")
POSITIONS_FILE = os.path.join(DATA_DIR, f"{PROGRAM_NAME}_positions.csv")
COOLDOWN_FILE = os.path.join(DATA_DIR, f"{PROGRAM_NAME}_cooldown.csv")
HIGH_LOW_FILE = os.path.join(DATA_DIR, f"{PROGRAM_NAME}_high_low.csv")
PAIR_STATUS_FILE = os.path.join(DATA_DIR, f"{PROGRAM_NAME}_pair_status.csv")
BUY_QUEUE_FILE = os.path.join(DATA_DIR, f"{PROGRAM_NAME}_buy_queue.csv")
FUNDS_FILE = os.path.join(DATA_DIR, f"{PROGRAM_NAME}_funds.csv")
LOG_FILE = os.path.join(DATA_DIR, f"{PROGRAM_NAME}_run.log")

TRADE_HEADER = ["timestamp", "inst_id", "direction", "price", "qty", "amount", "remaining_funds", "position_price", "profit", "profit_pct", "reason"]
POSITIONS_HEADER = ["inst_id", "position_price", "position_qty", "position_amount", "buy_time", "buy_reason", "pair_type", "pair_sell_line", "peak_price", "stop_price", "profit_triggered", "check_level", "last_check_time", "current_price", "market_value", "unrealized_pnl", "unrealized_pnl_pct"]
COOLDOWN_HEADER = ["inst_id", "sell_time", "cooldown_until"]
HIGH_LOW_HEADER = ["inst_id", "all_time_high", "all_time_low", "last_update"]
PAIR_STATUS_HEADER = ["inst_id", "pair_type", "is_paired", "buy_price", "buy_time", "sell_line", "sell_line_price", "is_sold", "sell_time", "sell_reason", "profit", "profit_pct"]
BUY_QUEUE_HEADER = ["inst_id", "status", "ref_high", "ref_low", "add_time", "last_check_time"]
FUNDS_HEADER = ["remaining_funds", "total_position_market_value", "total_unrealized_pnl", "total_realized_pnl", "net_asset_value", "last_update"]
BLACKLIST_HEADER = ["inst_id", "reason", "add_time"]
OBSERVE_LIST_HEADER = ["inst_id", "reason", "add_time"]

OKX_WS_HOST = "ws.okx.com"
OKX_WS_PORT = 8443
OKX_WS_PATH = "/ws/v5/public"
OKX_REST_BASE = "https://www.okx.com"

PING_INTERVAL = 25
RECONNECT_DELAY = 3
DEFAULT_PUSH_INTERVAL = 3.0

BREAK_ALERT_MAX_PER_DAY = 4
BREAK_ALERT_MIN_INTERVAL_SEC = 17 * 60
BASE_TIME_NODES = [17, 34, 63, 125, 250, 500, 1000]

CSV_LOCK = threading.RLock()

# ==================== 全局变量 ====================
_okx_client = None
_balance_cache = {"value": 0.0, "timestamp": 0.0}
_BALANCE_CACHE_TTL = 3.0
_positions_cache = {"data": {}, "timestamp": 0.0}
_POSITIONS_CACHE_TTL = 30

# ==================== 动态止盈表 ====================
PROFIT_STOP_MAP = [
    (0.05, 0.015),
    (0.06, 0.014),
    (0.07, 0.013),
    (0.08, 0.012),
    (0.09, 0.011),
    (0.10, 0.010),
    (0.11, 0.009),
    (0.12, 0.008),
    (0.13, 0.007),
    (0.14, 0.006),
    (0.15, 0.005),
    (0.16, 0.004),
    (0.17, 0.003),
    (0.18, 0.002),
    (0.19, 0.001),
    (0.20, 0.0),  # ≥20% 清仓（止损为0表示立即清仓）
]

def get_stop_loss(profit_pct: float) -> float:
    """根据盈利百分比返回对应的止损比例"""
    for p, s in PROFIT_STOP_MAP:
        if profit_pct >= p:
            return s
    return 0.015  # 默认

# ==================== 日志系统 ====================
class Logger:
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.ding_webhook = None
        self.ding_secret = None
        self._ensure_log_file()
    
    def _ensure_log_file(self):
        if not os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'w', encoding='utf-8') as f:
                    f.write(f"# {PROGRAM_NAME} {VERSION} 运行日志\n")
                    f.write(f"# 创建时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("=" * 60 + "\n")
            except Exception:
                pass
    
    def set_dingtalk(self, webhook: str, secret: str = None):
        self.ding_webhook = webhook
        self.ding_secret = secret
    
    def _should_write_log(self, level: str, category: str) -> bool:
        if level == "ERROR":
            return True
        if level == "INFO":
            return category in ["startup", "shutdown", "config_load", "buy_execute", "sell_execute", "profit_trigger", "stop_loss_trigger", "cooldown_start"]
        if level == "WARN":
            return category in ["insufficient_funds", "connection_error"]
        return False
    
    def _write(self, level: str, msg: str, category: str = "general"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] [{level}] {msg}"
        print(log_line)
        if self._should_write_log(level, category):
            try:
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(log_line + "\n")
            except Exception:
                pass
        if level == "ERROR" and self.ding_webhook:
            self._push_dingtalk_error(msg)
    
    def _push_dingtalk_error(self, msg: str):
        try:
            push_to_dingtalk(self.ding_webhook, self.ding_secret, f"🚨 系统异常\n\n{msg}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"[日志] 钉钉异常推送失败: {e}")
    
    def info(self, msg: str, category: str = "general"): 
        self._write("INFO", msg, category)
    
    def warn(self, msg: str, category: str = "general"): 
        self._write("WARN", msg, category)
    
    def error(self, msg: str, category: str = "general"): 
        self._write("ERROR", msg, category)
    
    def debug(self, msg: str, category: str = "debug"):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [DEBUG] {msg}")

# ==================== 钉钉推送 ====================
def build_dingtalk_url(webhook: str, secret: str = None) -> str:
    if not secret: 
        return webhook
    timestamp = str(int(time.time() * 1000))
    sign_string = f"{timestamp}\n{secret}".encode("utf-8")
    sign = base64.b64encode(hmac.new(secret.encode("utf-8"), sign_string, hashlib.sha256).digest()).decode("utf-8")
    parts = urllib.parse.urlsplit(webhook)
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    query.extend([("timestamp", timestamp), ("sign", sign)])
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, urllib.parse.urlencode(query), parts.fragment))

def push_to_dingtalk(webhook: str, secret: str = None, text: str = None, title: str = "交易提醒", msg_type: str = "text") -> bool:
    if not webhook: 
        return False
    try:
        url = build_dingtalk_url(webhook, secret)
        payload = {"msgtype": "text", "text": {"content": text}} if msg_type == "text" else {"msgtype": "markdown", "markdown": {"title": title, "text": text}}
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json", "User-Agent": "Python DingTalk Bot"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8")).get("errcode") in (0, "0", None)
    except Exception as e:
        print(f"[钉钉] 推送失败: {e}")
        return False

# ==================== CSV 辅助函数 ====================
def _ensure_csv_header(file_path: str, header: List[str]):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        try:
            with CSV_LOCK, open(file_path, 'w', encoding='utf-8-sig', newline='') as f:
                csv.writer(f).writerow(header)
        except Exception as e:
            print(f"[CSV] 初始化失败 {file_path}: {e}")
        return
    try:
        with CSV_LOCK, open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            existing_header = next(reader, [])
            rows = list(reader)
    except Exception as e:
        print(f"[CSV] 读取 header 失败 {file_path}: {e}")
        return
    if existing_header == header:
        return
    print(f"[CSV] 升级表结构 {file_path}: {existing_header} -> {header}")
    old_idx = {name: i for i, name in enumerate(existing_header)}
    migrated = []
    for row in rows:
        new_row = []
        for col in header:
            if col in old_idx and old_idx[col] < len(row):
                new_row.append(row[old_idx[col]])
            else:
                new_row.append("")
        migrated.append(new_row)
    try:
        with CSV_LOCK, open(file_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(migrated)
    except Exception as e:
        print(f"[CSV] 迁移失败 {file_path}: {e}")

def _read_csv(file_path: str, header: List[str]) -> List[Dict]:
    _ensure_csv_header(file_path, header)
    result = []
    try:
        with CSV_LOCK, open(file_path, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                result.append(row)
    except Exception:
        pass
    return result

def _write_csv_row(file_path: str, header: List[str], row: Dict):
    try:
        _ensure_csv_header(file_path, header)
        with CSV_LOCK, open(file_path, 'a', encoding='utf-8-sig', newline='') as f:
            csv.writer(f).writerow([row.get(c, "") for c in header])
    except Exception as e:
        print(f"[CSV] 写入失败 {file_path}: {e}")

def _write_csv_all(file_path: str, header: List[str], rows: List[Dict]):
    try:
        with CSV_LOCK, open(file_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    except Exception as e:
        print(f"[CSV] 写入失败 {file_path}: {e}")

# ==================== 黑名单与待观察列表 ====================
def load_blacklist() -> set:
    rows = _read_csv(BLACKLIST_FILE, BLACKLIST_HEADER)
    return {row.get("inst_id", "") for row in rows if row.get("inst_id")}

def add_to_blacklist(inst_id: str, reason: str):
    if not inst_id:
        return
    if inst_id in load_blacklist():
        return
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_csv_row(BLACKLIST_FILE, BLACKLIST_HEADER, {
        "inst_id": inst_id,
        "reason": reason,
        "add_time": now_str
    })
    print(f"[黑名单] {inst_id} 已加入黑名单，原因: {reason}")

def load_observe_list() -> set:
    rows = _read_csv(OBSERVE_LIST_FILE, OBSERVE_LIST_HEADER)
    return {row.get("inst_id", "") for row in rows if row.get("inst_id")}

def add_to_observe_list(inst_id: str, reason: str):
    if not inst_id:
        return
    if inst_id in load_observe_list():
        return
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_csv_row(OBSERVE_LIST_FILE, OBSERVE_LIST_HEADER, {
        "inst_id": inst_id,
        "reason": reason,
        "add_time": now_str
    })
    print(f"[待观察] {inst_id} 已加入待观察列表，原因: {reason}")

# ==================== OKX 持仓同步 ====================
def refresh_positions_cache(force=False):
    global _positions_cache
    now = time.time()
    if not force and (now - _positions_cache["timestamp"] < _POSITIONS_CACHE_TTL):
        return
    if _okx_client is None:
        return
    try:
        payload = _okx_client._request("GET", "/api/v5/account/positions", query={"instType": "SPOT"}, auth=True)
        api_positions = {}
        for item in payload.get("data", []):
            inst_id = item.get("instId")
            if not inst_id:
                continue
            pos_qty = float(item.get("pos", 0))
            if pos_qty > 0:
                api_positions[inst_id] = {
                    "position_price": float(item.get("avgPx", 0)),
                    "position_qty": pos_qty,
                    "position_amount": float(item.get("notionalUsd", 0))
                }
        _positions_cache["data"] = api_positions
        _positions_cache["timestamp"] = now

        local_pos = load_positions()
        for inst_id in list(local_pos.keys()):
            if inst_id not in api_positions:
                local_pos.pop(inst_id, None)
        for inst_id, data in api_positions.items():
            if inst_id in local_pos:
                local_pos[inst_id]["position_qty"] = data["position_qty"]
                local_pos[inst_id]["position_price"] = data["position_price"]
                local_pos[inst_id]["position_amount"] = data["position_amount"]
            else:
                local_pos[inst_id] = {
                    "position_price": data["position_price"],
                    "position_qty": data["position_qty"],
                    "position_amount": data["position_amount"],
                    "buy_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "buy_reason": "API同步",
                    "pair_type": "",
                    "pair_sell_line": "",
                    "peak_price": data["position_price"],
                    "stop_price": 0,
                    "profit_triggered": False,
                    "check_level": "T3",
                    "last_check_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "current_price": data["position_price"],
                    "market_value": data["position_amount"],
                    "unrealized_pnl": 0,
                    "unrealized_pnl_pct": 0,
                }
        save_positions(local_pos)
    except Exception:
        pass

def get_api_positions() -> Dict[str, Dict]:
    refresh_positions_cache()
    return _positions_cache["data"]

def has_api_position(inst_id: str) -> bool:
    positions = get_api_positions()
    return inst_id in positions and positions[inst_id].get("position_qty", 0) > 0

# ==================== 配置加载 ====================
def load_config() -> Tuple[Dict, Dict]:
    import openpyxl
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"配置文件不存在: {CONFIG_FILE}")
    config = {}
    system_params = {
        "total_funds": 1000000.0,
        "buy_ratio": 0.01,
        "cooldown_days": 7,
        "profit_trigger": 0.05,
        "stop_loss": 0.015,
        "profit_step": 0.01,
        "break_alert_max": 4,
        "break_alert_interval": 1020,
        "push_interval": 3.0,
        "data_source": "OKX",
        "trade_mode": "demo"
    }
    wb = openpyxl.load_workbook(CONFIG_FILE, data_only=True)
    if "币对配置" in wb.sheetnames:
        ws = wb["币对配置"]
        for row in range(2, ws.max_row + 1):
            inst_id = ws.cell(row=row, column=1).value
            ref_high = ws.cell(row=row, column=2).value
            ref_low = ws.cell(row=row, column=3).value
            enabled = ws.cell(row=row, column=4).value
            if inst_id and enabled and str(enabled).upper() in ("YES", "是", "1"):
                config[inst_id] = {"ref_high": float(ref_high), "ref_low": float(ref_low)}
    if "系统参数" in wb.sheetnames:
        ws = wb["系统参数"]
        for row in range(2, ws.max_row + 1):
            key = ws.cell(row=row, column=1).value
            value = ws.cell(row=row, column=2).value
            if key and value is not None:
                key = str(key).strip()
                if key in system_params:
                    if isinstance(system_params[key], float):
                        system_params[key] = float(value)
                    elif isinstance(system_params[key], int):
                        system_params[key] = int(float(value))
                    else:
                        system_params[key] = str(value)
    env_cooldown = os.getenv("COOLDOWN_DAYS")
    if env_cooldown is not None:
        try:
            system_params["cooldown_days"] = int(env_cooldown)
        except:
            pass
    return config, system_params

# ==================== 多层次价格计算 ====================
class PriceLevels:
    def __init__(self, high: float, low: float):
        self.high = high
        self.low = low
        self.update(high, low)
    
    def update(self, high: float, low: float):
        self.high = high
        self.low = low
        
        # 区间最高 → 高位中间
        self.high_mid = (high + low) / 2 if high != low else high
        
        # 回撤系列（从 high 到 high_mid）
        self.pullback1_mid = (high + self.high_mid) / 2  # 回撤中（回撤1中）
        self.pullback_up = (high + self.pullback1_mid) / 2  # 回撤上
        self.pullback2_down = (self.pullback1_mid + self.high_mid) / 2  # 回撤2下（买点）
        self.pullback3 = (self.pullback2_down + self.high_mid) / 2
        self.pullback4 = (self.pullback3 + self.high_mid) / 2
        
        # 回调系列（从 high_mid 到 mid）
        self.mid = (high + low) / 2
        self.callback1_mid = (self.high_mid + self.mid) / 2  # 回调中（回调1中）
        self.callback_up = (self.high_mid + self.callback1_mid) / 2  # 回调上
        self.callback2_down = (self.callback1_mid + self.mid) / 2  # 回调2下（买点）
        self.callback3 = (self.callback2_down + self.mid) / 2
        self.callback4 = (self.callback3 + self.mid) / 2
        
        # 过渡系列（从 mid 到 low_mid）
        self.low_mid = (self.mid + low) / 2
        self.transition1_mid = (self.mid + self.low_mid) / 2  # 过渡中（过渡1中）
        self.transition_up = (self.mid + self.transition1_mid) / 2  # 过渡上
        self.transition2_down = (self.transition1_mid + self.low_mid) / 2  # 过渡2下（买点）
        self.transition3 = (self.transition2_down + self.low_mid) / 2
        self.transition4 = (self.transition3 + self.low_mid) / 2
        
        # 极限系列（从 low_mid 到 low）
        self.limit1_mid = (self.low_mid + low) / 2  # 极限中（极限1中）
        self.limit_up = (self.low_mid + self.limit1_mid) / 2  # 极限上
        self.limit2_down = (self.limit1_mid + low) / 2  # 极限2下（买点）
        self.limit3 = (self.limit2_down + low) / 2
        self.limit4 = (self.limit3 + low) / 2
        
        # 为了方便，将买点和卖点组织为字典
        self.buy_levels = {
            "pullback2_down": self.pullback2_down,
            "callback2_down": self.callback2_down,
            "transition2_down": self.transition2_down,
            "limit2_down": self.limit2_down,
        }
        self.sell_levels = {
            "pullback_up": self.pullback_up,
            "callback_up": self.callback_up,
            "transition_up": self.transition_up,
            "limit_up": self.limit_up,
        }
        # 配对关系：买点 → 对应卖点
        self.pair_map = {
            "pullback2_down": ("pullback_up", "回撤"),
            "callback2_down": ("callback_up", "回调"),
            "transition2_down": ("transition_up", "过渡"),
            "limit2_down": ("limit_up", "极限"),
        }
    
    def get_buy_sell_pair(self, buy_key: str):
        if buy_key in self.pair_map:
            sell_key, pair_name = self.pair_map[buy_key]
            return getattr(self, sell_key), sell_key, pair_name
        return None, None, None

# ==================== WebSocket 底层 ====================
def recv_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("连接已关闭")
        data.extend(chunk)
    return bytes(data)

def build_ws_frame(payload: bytes, opcode: int = 0x1) -> bytes:
    fin = 0x80 | opcode
    mask = 0x80
    length = len(payload)
    if length < 126:
        header = bytes([fin, mask | length])
    elif length < 65536:
        header = bytes([fin, mask | 126]) + struct.pack("!H", length)
    else:
        header = bytes([fin, mask | 127]) + struct.pack("!Q", length)
    mask_key = os.urandom(4)
    masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    return header + mask_key + masked

def send_text(sock, text):
    sock.sendall(build_ws_frame(text.encode("utf-8"), opcode=0x1))

def send_pong(sock, payload=b""):
    sock.sendall(build_ws_frame(payload, opcode=0xA))

def send_ping(sock, payload=b""):
    sock.sendall(build_ws_frame(payload, opcode=0x9))

def recv_ws_frame(sock):
    first, second = recv_exact(sock, 2)
    opcode = first & 0x0F
    masked = (second & 0x80) != 0
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(sock, 8))[0]
    mask_key = recv_exact(sock, 4) if masked else b""
    payload = recv_exact(sock, length) if length else b""
    if masked:
        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    return opcode, payload

def _ws_handshake(host, port, path):
    raw = socket.create_connection((host, port), timeout=10)
    ctx = ssl.create_default_context()
    sock = ctx.wrap_socket(raw, server_hostname=host)
    sock.settimeout(PING_INTERVAL)
    key = base64.b64encode(os.urandom(16)).decode()
    req = f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: Python WebSocket Client\r\n\r\n"
    sock.sendall(req.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("握手失败")
        resp += chunk
    hdr, _, _ = resp.partition(b"\r\n\r\n")
    txt = hdr.decode(errors="ignore")
    if "101" not in txt.split("\r\n")[0]:
        raise ConnectionError(f"握手失败: {txt}")
    headers = {}
    for line in txt.split("\r\n")[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    expected = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
    if headers.get("sec-websocket-accept") != expected:
        raise ConnectionError("握手校验失败")
    return sock

def connect_okx_ws():
    return _ws_handshake(OKX_WS_HOST, OKX_WS_PORT, OKX_WS_PATH)

# ==================== 工具函数 ====================
def check_okx_available(inst_id):
    for inst_type in ("SPOT", "SWAP"):
        url = f"{OKX_REST_BASE}/api/v5/public/instruments?instType={inst_type}&instId={urllib.parse.quote(inst_id)}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Python OKX Client"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("code") == "0" and data.get("data"):
                    return True
        except Exception:
            pass
    return False

def is_price_near_level(price, level, threshold=0.05):
    return level > 0 and level * (1 - threshold) <= price <= level * (1 + threshold)

# ==================== OKX REST 交易客户端 ====================
TRADE_MODE = "demo"
TRADE_MODE_DESC = {
    "demo": "OKX模拟盘(API下单)",
    "live": "OKX实盘(API下单)",
}

class OkxTradeError(Exception):
    pass

class OkxTradeClient:
    def __init__(self, api_key, api_secret, passphrase, simulated=True, logger=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.simulated = simulated
        self.logger = logger
        self._inst_cache = {}

    @staticmethod
    def _timestamp():
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

    def _sign(self, ts, method, request_path, body):
        msg = (ts + method.upper() + request_path + (body or "")).encode("utf-8")
        return base64.b64encode(hmac.new(self.api_secret.encode("utf-8"), msg, hashlib.sha256).digest()).decode("utf-8")

    def _request(self, method, path, body_obj=None, query=None, auth=True, timeout=10):
        body = json.dumps(body_obj, separators=(",", ":")) if body_obj is not None else ""
        request_path = path
        if query:
            request_path = f"{path}?{urllib.parse.urlencode(query)}"
        url = OKX_REST_BASE + request_path
        headers = {"Content-Type": "application/json", "User-Agent": f"{PROGRAM_NAME}/{VERSION}"}
        if auth:
            ts = self._timestamp()
            headers["OK-ACCESS-KEY"] = self.api_key
            headers["OK-ACCESS-SIGN"] = self._sign(ts, method, request_path, body)
            headers["OK-ACCESS-TIMESTAMP"] = ts
            headers["OK-ACCESS-PASSPHRASE"] = self.passphrase
        if self.simulated:
            headers["x-simulated-trading"] = "1"
        data = body.encode("utf-8") if body else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="ignore")
            try:
                payload = json.loads(raw)
            except Exception:
                raise OkxTradeError(f"HTTP {e.code}: {raw[:200]}")
        if str(payload.get("code")) != "0":
            raise OkxTradeError(f"code={payload.get('code')} msg={payload.get('msg')} data={payload.get('data')}")
        return payload

    def get_instrument(self, inst_id):
        if inst_id in self._inst_cache:
            return self._inst_cache[inst_id]
        payload = self._request("GET", "/api/v5/public/instruments", query={"instType": "SPOT", "instId": inst_id}, auth=False)
        info = (payload.get("data") or [None])[0]
        if info:
            self._inst_cache[inst_id] = info
        return info

    @staticmethod
    def _round_down(value, step):
        try:
            step_f = float(step)
            if step_f <= 0:
                return value
            s = f"{step}".rstrip("0")
            decimals = len(s.split(".")[1]) if "." in s else 0
            n = int(float(value) / step_f)
            return round(n * step_f, decimals)
        except Exception:
            return value

    def get_balance(self, ccy="USDT"):
        payload = self._request("GET", "/api/v5/account/balance", query={"ccy": ccy})
        for item in payload.get("data", []):
            for d in item.get("details", []):
                if d.get("ccy") == ccy:
                    return float(d.get("availBal") or 0)
        return 0.0

    @staticmethod
    def _new_clordid():
        return f"okd{int(time.time() * 1000)}{os.urandom(2).hex()}"

    @staticmethod
    def _fmt_sz(value):
        s = f"{float(value):.12f}".rstrip("0").rstrip(".")
        return s if s else "0"

    def _place_order(self, inst_id, side, sz, tgt_ccy, cl_ord_id):
        body = {
            "instId": inst_id,
            "tdMode": "cash",
            "clOrdId": cl_ord_id,
            "side": side,
            "ordType": "market",
            "sz": f"{sz}",
        }
        if tgt_ccy:
            body["tgtCcy"] = tgt_ccy
        payload = self._request("POST", "/api/v5/trade/order", body_obj=body)
        data = (payload.get("data") or [{}])[0]
        if str(data.get("sCode")) != "0":
            raise OkxTradeError(f"下单被拒 sCode={data.get('sCode')} sMsg={data.get('sMsg')}")
        return data.get("ordId", "")

    def _query_order(self, inst_id, ord_id=None, cl_ord_id=None):
        query = {"instId": inst_id}
        if ord_id:
            query["ordId"] = ord_id
        elif cl_ord_id:
            query["clOrdId"] = cl_ord_id
        else:
            return None
        payload = self._request("GET", "/api/v5/trade/order", query=query)
        return (payload.get("data") or [None])[0]

    def _wait_fill(self, inst_id, ord_id=None, cl_ord_id=None, tries=8, interval=0.5):
        last = None
        for _ in range(tries):
            try:
                od = self._query_order(inst_id, ord_id, cl_ord_id)
            except Exception:
                od = None
            if od:
                last = od
                if od.get("state") in ("filled", "canceled"):
                    return od
            time.sleep(interval)
        return last

    @staticmethod
    def _fill_result(od, side):
        avg_px = float(od.get("avgPx") or 0)
        acc_sz = float(od.get("accFillSz") or 0)
        fee = float(od.get("fee") or 0)
        fee_ccy = od.get("feeCcy", "")
        base_ccy = (od.get("instId", "").split("-") or [""])[0]
        gross_quote = avg_px * acc_sz
        result = {
            "price": avg_px,
            "qty": acc_sz,
            "ord_id": od.get("ordId", ""),
            "fee": fee,
            "fee_ccy": fee_ccy,
            "state": od.get("state", ""),
        }
        if side == "buy":
            result["qty"] = acc_sz + fee if fee_ccy == base_ccy else acc_sz
            result["cost"] = gross_quote
        else:
            result["proceeds"] = gross_quote + fee if fee_ccy != base_ccy else gross_quote
        return result

    def _safe_query_by_clordid(self, inst_id, cl_ord_id):
        try:
            return self._query_order(inst_id, cl_ord_id=cl_ord_id)
        except Exception:
            return None

    def _log_error(self, msg):
        if self.logger:
            self.logger.error(msg)
        else:
            print(f"[OKX] {msg}")

    def market_buy_quote(self, inst_id, quote_amount):
        cl_ord_id = self._new_clordid()
        try:
            info = self.get_instrument(inst_id)
            if info:
                min_sz = float(info.get("minSz") or 0)
                if min_sz > 0 and quote_amount < min_sz:
                    raise OkxTradeError(f"买入金额 {quote_amount:.2f} 低于最小下单量 {min_sz} {info.get('quoteCcy', '')}")
            ord_id = self._place_order(inst_id, "buy", self._fmt_sz(quote_amount), "quote_ccy", cl_ord_id)
        except Exception as e:
            od = self._safe_query_by_clordid(inst_id, cl_ord_id)
            if od and float(od.get("accFillSz") or 0) > 0:
                self._log_error(f"{inst_id} 买入请求异常({e})，但按 clOrdId 反查到已成交，按实际成交入账")
                return self._fill_result(od, "buy")
            error_msg = str(e)
            self._log_error(f"{inst_id} 市价买入失败: {error_msg}")
            return {"error": error_msg}
        od = self._wait_fill(inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
        if not od or float(od.get("accFillSz") or 0) <= 0:
            self._log_error(f"{inst_id} 买入未成交 ordId={ord_id} state={(od or {}).get('state')}")
            return None
        return self._fill_result(od, "buy")

    def market_sell_base(self, inst_id, qty):
        cl_ord_id = self._new_clordid()
        sz = qty
        try:
            info = self.get_instrument(inst_id)
            if info:
                lot_sz = info.get("lotSz") or "0"
                min_sz = float(info.get("minSz") or 0)
                sz = self._round_down(qty, lot_sz)
                if sz <= 0:
                    raise OkxTradeError(f"卖出数量按 lotSz={lot_sz} 取整后为0 (原始 {qty:.8g})")
                if min_sz > 0 and sz < min_sz:
                    raise OkxTradeError(f"卖出数量 {sz:.8g} 低于最小下单量 {min_sz} {info.get('baseCcy', '')}")
            ord_id = self._place_order(inst_id, "sell", self._fmt_sz(sz), "base_ccy", cl_ord_id)
        except Exception as e:
            od = self._safe_query_by_clordid(inst_id, cl_ord_id)
            if od and float(od.get("accFillSz") or 0) > 0:
                self._log_error(f"{inst_id} 卖出请求异常({e})，但按 clOrdId 反查到已成交，按实际成交入账")
                return self._fill_result(od, "sell")
            self._log_error(f"{inst_id} 市价卖出失败: {e}")
            return None
        od = self._wait_fill(inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
        if not od or float(od.get("accFillSz") or 0) <= 0:
            self._log_error(f"{inst_id} 卖出未成交 ordId={ord_id} state={(od or {}).get('state')}")
            return None
        return self._fill_result(od, "sell")

# ==================== 余额获取 ====================
def get_okx_balance(force_refresh=False) -> float:
    global _balance_cache
    now = time.time()
    if not force_refresh and (now - _balance_cache["timestamp"] < _BALANCE_CACHE_TTL):
        return _balance_cache["value"]
    if _okx_client is None:
        return 0.0
    try:
        bal = _okx_client.get_balance("USDT")
        _balance_cache["value"] = bal
        _balance_cache["timestamp"] = now
        return bal
    except Exception:
        return _balance_cache["value"]

# ==================== 本地查看文件更新 ====================
def _update_funds_for_view(balance: float):
    try:
        row = {
            "remaining_funds": f"{balance:.2f}",
            "total_position_market_value": "0",
            "total_unrealized_pnl": "0",
            "total_realized_pnl": "0",
            "net_asset_value": f"{balance:.2f}",
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        _ensure_csv_header(FUNDS_FILE, FUNDS_HEADER)
        with CSV_LOCK, open(FUNDS_FILE, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=FUNDS_HEADER)
            writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[资金] 更新查看文件失败: {e}")

# ==================== 状态持久化 ====================
def load_positions() -> Dict[str, Dict]:
    rows = _read_csv(POSITIONS_FILE, POSITIONS_HEADER)
    result = {}
    for row in rows:
        inst_id = row.get("inst_id", "")
        if inst_id:
            position_price = float(row.get("position_price", 0) or 0)
            position_qty = float(row.get("position_qty", 0) or 0)
            position_amount = float(row.get("position_amount", 0) or 0)
            raw_cp = row.get("current_price", "") or ""
            if raw_cp != "" and float(raw_cp) > 0:
                current_price = float(raw_cp)
            else:
                current_price = position_price
            mv_raw = row.get("market_value", "") or ""
            upnl_raw = row.get("unrealized_pnl", "") or ""
            upnl_pct_raw = row.get("unrealized_pnl_pct", "") or ""
            if mv_raw != "" and upnl_raw != "":
                market_value = float(mv_raw)
                unrealized_pnl = float(upnl_raw)
                unrealized_pnl_pct = float(upnl_pct_raw) if upnl_pct_raw != "" else 0.0
            else:
                market_value = current_price * position_qty if current_price > 0 else position_amount
                unrealized_pnl = market_value - position_amount if position_amount > 0 else 0.0
                unrealized_pnl_pct = (unrealized_pnl / position_amount * 100.0) if position_amount > 0 else 0.0
            result[inst_id] = {
                "position_price": position_price,
                "position_qty": position_qty,
                "position_amount": position_amount,
                "buy_time": row.get("buy_time", ""),
                "buy_reason": row.get("buy_reason", ""),
                "pair_type": row.get("pair_type", ""),
                "pair_sell_line": row.get("pair_sell_line", ""),
                "peak_price": float(row.get("peak_price", 0) or 0),
                "stop_price": float(row.get("stop_price", 0) or 0),
                "profit_triggered": (str(row.get("profit_triggered") or "False")).strip().lower() == "true",
                "check_level": row.get("check_level", "T3"),
                "last_check_time": row.get("last_check_time", ""),
                "current_price": current_price,
                "market_value": market_value,
                "unrealized_pnl": unrealized_pnl,
                "unrealized_pnl_pct": unrealized_pnl_pct,
            }
    return result

def save_positions(positions: Dict[str, Dict]):
    rows = []
    for inst_id, data in positions.items():
        rows.append({
            "inst_id": inst_id,
            "position_price": data.get("position_price", 0),
            "position_qty": data.get("position_qty", 0),
            "position_amount": data.get("position_amount", 0),
            "buy_time": data.get("buy_time", ""),
            "buy_reason": data.get("buy_reason", ""),
            "pair_type": data.get("pair_type", ""),
            "pair_sell_line": data.get("pair_sell_line", ""),
            "peak_price": data.get("peak_price", 0),
            "stop_price": data.get("stop_price", 0),
            "profit_triggered": "True" if data.get("profit_triggered", False) else "False",
            "check_level": data.get("check_level", "T3"),
            "last_check_time": data.get("last_check_time", ""),
            "current_price": data.get("current_price", 0),
            "market_value": data.get("market_value", 0),
            "unrealized_pnl": data.get("unrealized_pnl", 0),
            "unrealized_pnl_pct": data.get("unrealized_pnl_pct", 0),
        })
    _write_csv_all(POSITIONS_FILE, POSITIONS_HEADER, rows)

def load_cooldown() -> Dict[str, str]:
    rows = _read_csv(COOLDOWN_FILE, COOLDOWN_HEADER)
    result = {}
    for row in rows:
        inst_id = row.get("inst_id", "")
        if inst_id:
            result[inst_id] = row.get("cooldown_until", "")
    return result

def save_cooldown_entry(inst_id: str, cooldown_until: str):
    _write_csv_row(COOLDOWN_FILE, COOLDOWN_HEADER, {"inst_id": inst_id, "sell_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "cooldown_until": cooldown_until})

def load_high_low() -> Dict[str, Dict]:
    rows = _read_csv(HIGH_LOW_FILE, HIGH_LOW_HEADER)
    result = {}
    for row in rows:
        inst_id = row.get("inst_id", "")
        if inst_id:
            result[inst_id] = {
                "all_time_high": float(row.get("all_time_high", 0) or 0),
                "all_time_low": float(row.get("all_time_low", 0) or 0),
                "last_update": row.get("last_update", "")
            }
    return result

def save_high_low_entry(inst_id: str, high: float, low: float):
    rows = _read_csv(HIGH_LOW_FILE, HIGH_LOW_HEADER)
    found = False
    for row in rows:
        if row.get("inst_id") == inst_id:
            row["all_time_high"] = str(high)
            row["all_time_low"] = str(low)
            row["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            found = True
            break
    if not found:
        rows.append({"inst_id": inst_id, "all_time_high": str(high), "all_time_low": str(low), "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    _write_csv_all(HIGH_LOW_FILE, HIGH_LOW_HEADER, rows)

def load_pair_status() -> Dict[str, Dict]:
    rows = _read_csv(PAIR_STATUS_FILE, PAIR_STATUS_HEADER)
    result = {}
    for row in rows:
        inst_id = row.get("inst_id", "")
        if inst_id:
            result[inst_id] = {
                "pair_type": row.get("pair_type", ""),
                "is_paired": (str(row.get("is_paired") or "False")).strip().lower() == "true",
                "buy_price": float(row.get("buy_price", 0) or 0),
                "buy_time": row.get("buy_time", ""),
                "sell_line": row.get("sell_line", ""),
                "sell_line_price": float(row.get("sell_line_price", 0) or 0),
                "is_sold": (str(row.get("is_sold") or "False")).strip().lower() == "true",
                "sell_time": row.get("sell_time", ""),
                "sell_reason": row.get("sell_reason", ""),
                "profit": float(row.get("profit", 0) or 0),
                "profit_pct": float(row.get("profit_pct", 0) or 0),
            }
    return result

def save_pair_status(inst_id: str, data: Dict):
    rows = _read_csv(PAIR_STATUS_FILE, PAIR_STATUS_HEADER)
    found = False
    for row in rows:
        if row.get("inst_id") == inst_id:
            row["pair_type"] = data.get("pair_type", row.get("pair_type", ""))
            row["is_paired"] = "True" if data.get("is_paired", False) else "False"
            row["buy_price"] = str(data.get("buy_price", row.get("buy_price", 0)))
            row["buy_time"] = data.get("buy_time", row.get("buy_time", ""))
            if data.get("sell_line") not in (None, ""):
                row["sell_line"] = data["sell_line"]
            row["sell_line_price"] = str(data.get("sell_line_price", row.get("sell_line_price", 0)))
            row["is_sold"] = "True" if data.get("is_sold", False) else "False"
            row["sell_time"] = data.get("sell_time", row.get("sell_time", ""))
            row["sell_reason"] = data.get("sell_reason", row.get("sell_reason", ""))
            if "profit" in data and data["profit"] not in (None, ""):
                row["profit"] = str(data["profit"])
            if "profit_pct" in data and data["profit_pct"] not in (None, ""):
                row["profit_pct"] = str(data["profit_pct"])
            found = True
            break
    if not found:
        rows.append({
            "inst_id": inst_id,
            "pair_type": data.get("pair_type", ""),
            "is_paired": "True" if data.get("is_paired", False) else "False",
            "buy_price": str(data.get("buy_price", 0)),
            "buy_time": data.get("buy_time", ""),
            "sell_line": data.get("sell_line", ""),
            "sell_line_price": str(data.get("sell_line_price", 0)),
            "is_sold": "True" if data.get("is_sold", False) else "False",
            "sell_time": data.get("sell_time", ""),
            "sell_reason": data.get("sell_reason", ""),
            "profit": data.get("profit", 0),
            "profit_pct": data.get("profit_pct", 0),
        })
    _write_csv_all(PAIR_STATUS_FILE, PAIR_STATUS_HEADER, rows)

def load_buy_queue() -> List[Dict]:
    return _read_csv(BUY_QUEUE_FILE, BUY_QUEUE_HEADER)

def save_buy_queue(rows: List[Dict]):
    _write_csv_all(BUY_QUEUE_FILE, BUY_QUEUE_HEADER, rows)

def save_trade(row: Dict):
    _write_csv_row(TRADE_FILE, TRADE_HEADER, row)

# ==================== 初始化 OKX 客户端 ====================
def init_okx_client(trade_mode, logger):
    global _okx_client, TRADE_MODE
    mode = (trade_mode or "demo").strip().lower()
    if mode not in ("demo", "live"):
        logger.error(f"不支持的交易模式: {mode}，仅支持 demo/live", "startup")
        sys.exit(1)
    if mode == "live" and os.getenv("OKX_LIVE_CONFIRM", "").strip().upper() != "YES":
        logger.warn("实盘模式未设置 OKX_LIVE_CONFIRM=YES，自动降级为模拟盘 demo", "startup")
        mode = "demo"
    TRADE_MODE = mode

    api_key = os.getenv("OKX_API_KEY", "").strip()
    api_secret = os.getenv("OKX_API_SECRET", "").strip()
    passphrase = os.getenv("OKX_API_PASSPHRASE", "").strip()
    if not (api_key and api_secret and passphrase):
        logger.error("OKX API 凭证不完整，请检查 .env 文件", "startup")
        sys.exit(1)

    _okx_client = OkxTradeClient(api_key, api_secret, passphrase, simulated=(mode == "demo"), logger=logger)
    try:
        bal = _okx_client.get_balance("USDT")
        logger.info(f"OKX {'模拟盘' if mode == 'demo' else '实盘'}账户 USDT 可用余额: {bal:.2f}", "startup")
        _update_funds_for_view(bal)
        refresh_positions_cache(force=True)
        logger.info("持仓缓存已初始化", "startup")
    except Exception as e:
        logger.error(f"OKX 账户连接失败: {e}", "startup")
        sys.exit(1)
    logger.info(f"交易模式: {TRADE_MODE_DESC[mode]}", "startup")
    return mode

# ==================== 币对状态管理（核心逻辑） ====================
class SymbolState:
    def __init__(self, inst_id, ref_high, ref_low, logger, ding_webhook=None, ding_secret=None, system_params=None):
        self.inst_id = inst_id
        self.logger = logger
        self.ding_webhook = ding_webhook
        self.ding_secret = ding_secret
        self.params = system_params or {}
        self.total_funds = self.params.get("total_funds", 1000000)
        self.min_buy_amount = float(os.getenv("MIN_BUY_AMOUNT", "100"))
        self.client = _okx_client

        # 加载黑名单和观察列表
        self.blacklist = load_blacklist()
        self.observe_list = load_observe_list()

        self._okx_available = False
        if inst_id in self.blacklist:
            self.logger.info(f"{inst_id} 在黑名单中，自动跳过", "startup")
        elif inst_id in self.observe_list:
            self.logger.info(f"{inst_id} 在待观察列表中，不进行交易", "startup")
        elif self.client is not None:
            try:
                info = self.client.get_instrument(inst_id)
                if info:
                    self._okx_available = True
                else:
                    self._okx_available = False
            except Exception:
                self._okx_available = False
        if not self._okx_available and inst_id not in self.blacklist and inst_id not in self.observe_list:
            self.logger.warn(f"{inst_id} 在 OKX 现货不可交易，已加入黑名单", "startup")
            add_to_blacklist(inst_id, "启动检测不可交易")
        elif not self._okx_available:
            self.logger.info(f"{inst_id} 已在黑名单/观察列表中，跳过", "startup")

        # 加载本地状态
        self.positions = load_positions()
        self.cooldown = load_cooldown()
        self.high_low = load_high_low()
        self.pair_status = load_pair_status()
        self.buy_queue = load_buy_queue()

        hl = self.high_low.get(inst_id, {})
        self.high = hl.get("all_time_high", ref_high)
        self.low = hl.get("all_time_low", ref_low)
        self.levels = PriceLevels(self.high, self.low)

        # 持仓状态
        pos = self.positions.get(inst_id, {})
        self.has_position = pos.get("position_qty", 0) > 0
        self.position_price = pos.get("position_price", 0)
        self.position_qty = pos.get("position_qty", 0)
        self.position_amount = pos.get("position_amount", 0)
        self.buy_time = pos.get("buy_time", "")
        self.buy_reason = pos.get("buy_reason", "")
        self.pair_type = pos.get("pair_type", "")
        self.pair_sell_line = pos.get("pair_sell_line", "")
        self.peak_price = pos.get("peak_price", 0)
        self.stop_price = pos.get("stop_price", 0)
        self.profit_triggered = pos.get("profit_triggered", False)
        self.check_level = pos.get("check_level", "T3")
        self.last_check_time = pos.get("last_check_time", "")

        # 配对状态
        pair = self.pair_status.get(inst_id, {})
        self.is_paired = pair.get("is_paired", False)
        self.pair_buy_price = pair.get("buy_price", 0)
        self.pair_buy_time = pair.get("buy_time", "")
        self.pair_sell_line_price = pair.get("sell_line_price", 0)
        self.is_sold = pair.get("is_sold", False)
        self.sell_time = pair.get("sell_time", "")
        self.sell_reason = pair.get("sell_reason", "")

        self._last_profit = 0.0
        self._last_profit_pct = 0.0
        self._balance_insufficient_until = 0.0
        self._buy_attempt_count = 0  # 买入尝试次数（用于异常分类）
        self._last_buy_error = ""   # 最后一次买入错误信息

        self._sync_buy_queue()
        self.below_pullback_since = None
        self.below_callback_since = None
        self.below_transition_since = None
        self.below_limit_since = None
        self.last_new_high_ts = time.time()
        self.last_new_low_ts = time.time()
        self.high_alert_sent = set()
        self.low_alert_sent = set()
        self.break_alerts = {name: {"count": 0, "last_ts": 0.0, "date": ""} for name in ("回撤上", "回撤下", "回调上", "回调下", "过渡上", "过渡下", "极限上", "极限下")}
        self.broken_levels = set()
        self._last_processed_price = 0.0
        self._last_processed_ts = 0.0
        self._profit_notified_steps = set()

        # 同步 API 持仓
        if self.client is not None and self._okx_available:
            refresh_positions_cache(force=True)
            if has_api_position(inst_id):
                if not self.has_position:
                    self.logger.info(f"{inst_id} API 检测到持仓，本地同步更新", "startup")
                    api_pos = get_api_positions().get(inst_id, {})
                    self.has_position = True
                    self.position_qty = api_pos.get("position_qty", 0)
                    self.position_price = api_pos.get("position_price", 0)
                    self.position_amount = api_pos.get("position_amount", 0)
                    self._save_state()

    def _sync_buy_queue(self):
        in_queue = False
        changed = False
        for row in self.buy_queue:
            if row.get("inst_id") == self.inst_id:
                in_queue = True
                if self.has_position:
                    new_status = "已持仓_排除"
                elif self._is_in_cooldown():
                    new_status = "静默期_排除"
                elif self.inst_id in self.observe_list:
                    new_status = "观察列表_排除"
                else:
                    new_status = "待买入"
                if row.get("status") != new_status:
                    row["status"] = new_status
                    changed = True
                new_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if row.get("last_check_time") != new_ts:
                    row["last_check_time"] = new_ts
                    changed = True
                break
        if not in_queue and not self.has_position and not self._is_in_cooldown() and self.inst_id not in self.observe_list:
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.buy_queue.append({
                "inst_id": self.inst_id,
                "status": "待买入",
                "ref_high": str(self.high),
                "ref_low": str(self.low),
                "add_time": now_str,
                "last_check_time": now_str
            })
            changed = True
        if changed:
            save_buy_queue(self.buy_queue)

    def _is_in_cooldown(self):
        until = self.cooldown.get(self.inst_id, "")
        if until:
            try:
                return datetime.now() < datetime.strptime(until, "%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        for row in self.buy_queue:
            if row.get("inst_id") == self.inst_id and row.get("status") == "静默期_排除":
                row["status"] = "待买入"
                save_buy_queue(self.buy_queue)
                break
        return False

    def _update_check_level(self, profit_pct):
        if profit_pct >= 0.05:
            self.check_level = "T0"
        elif profit_pct > 0:
            self.check_level = "T1"
        elif self.pair_type in ("回撤", "回调", "过渡", "极限"):
            self.check_level = "T2"
        else:
            self.check_level = "T3"
        self.last_check_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _can_buy(self):
        if not self._okx_available:
            return False, "币对在黑名单/不可交易"
        if self.has_position:
            return False, "本地已有持仓"
        if has_api_position(self.inst_id):
            return False, "API 检测到现有持仓"
        if self._is_in_cooldown():
            return False, f"静默期中 (至 {self.cooldown.get(self.inst_id, '')})"
        if self.inst_id in self.observe_list:
            return False, "在待观察列表中"
        if time.time() < self._balance_insufficient_until:
            return False, f"余额不足冷却中"
        balance = get_okx_balance()
        raw_amount = balance * self.params.get("buy_ratio", 0.01)
        buy_amount = max(raw_amount, self.min_buy_amount)
        if buy_amount > balance:
            buy_amount = balance
        if buy_amount <= 0:
            return False, "计算出的买入金额为0"
        if balance < buy_amount:
            return False, f"资金不足: 需要 {buy_amount:.2f}, 余额 {balance:.2f}"
        if self.is_paired and not self.is_sold:
            return False, f"已配对未卖出 ({self.pair_type})"
        return True, ""

    def _can_sell(self):
        return (True, "") if self.has_position else (False, "无持仓")

    def _get_buy_level(self, price):
        """根据当前价格找到最近的买点（低于买点且最接近）"""
        buy_keys = ["pullback2_down", "callback2_down", "transition2_down", "limit2_down"]
        best_key = None
        best_price = None
        for key in buy_keys:
            level = getattr(self.levels, key)
            if price < level:
                if best_price is None or level < best_price:
                    best_price = level
                    best_key = key
        return best_key, best_price

    def _execute_buy(self, price, reason, buy_key, sell_key, pair_name):
        """执行买入，price 为当前最新价，buy_key 为买点名称，sell_key 为对应卖点名称"""
        # 余额检查
        balance = get_okx_balance(force_refresh=True)
        if balance < 110:
            self.logger.info(f"{self.inst_id} 账户余额 {balance:.2f} USDT 低于 110，跳过买入")
            return None

        # 再次确认买入条件（价格必须小于买点）
        buy_level = getattr(self.levels, buy_key)
        if price >= buy_level:
            self.logger.debug(f"{self.inst_id} 当前价 {price:.8g} 不小于买点 {buy_level:.8g}，跳过")
            return None

        can, msg = self._can_buy()
        if not can:
            self.logger.debug(f"{self.inst_id} 买入跳过: {msg}")
            return None

        # 计算买入金额（取 1% 和 min_buy_amount 的较大值）
        raw_amount = balance * self.params.get("buy_ratio", 0.01)
        buy_amount = max(raw_amount, self.min_buy_amount)
        if buy_amount > balance:
            buy_amount = balance
        if buy_amount <= 0:
            self.logger.info(f"{self.inst_id} 买入金额为0，跳过")
            return None

        # 调用 API 下单
        fill = self.client.market_buy_quote(self.inst_id, buy_amount)
        if fill is None:
            self.logger.error(f"{self.inst_id} API买入失败（未知错误），跳过")
            # 不计入尝试次数？假设网络错误，可以重试
            return None
        if isinstance(fill, dict) and "error" in fill:
            error_msg = fill["error"]
            self.logger.error(f"{self.inst_id} API买入失败: {error_msg}")
            self._last_buy_error = error_msg
            self._buy_attempt_count += 1
            
            # 判断异常类型
            error_lower = error_msg.lower()
            # 合规限制：51155
            if "51155" in error_msg or "compliance" in error_lower or "restriction" in error_lower:
                self.logger.warn(f"{self.inst_id} 因合规限制加入黑名单")
                add_to_blacklist(self.inst_id, f"运行时错误: {error_msg[:100]}")
                self._okx_available = False
                self._remove_from_buy_queue()
                return None
            # 订单限制：如超过100USDT (51201) 或其他订单相关错误
            elif "51201" in error_msg or "exceed" in error_lower or "market order" in error_lower:
                self.logger.warn(f"{self.inst_id} 因订单限制（如超过100USDT）加入待观察列表，并从买入队列移除")
                add_to_observe_list(self.inst_id, f"订单限制: {error_msg[:100]}")
                self._remove_from_buy_queue()
                return None
            # 其他错误，如果尝试次数>=2，移入观察列表
            elif self._buy_attempt_count >= 2:
                self.logger.warn(f"{self.inst_id} 连续两次买入失败，移入待观察列表")
                add_to_observe_list(self.inst_id, f"连续失败: {error_msg[:100]}")
                self._remove_from_buy_queue()
                return None
            else:
                # 未达到阈值，下次继续尝试
                return None

        # 成交成功
        price = fill["price"]
        qty = fill["qty"]
        buy_amount = fill["cost"]
        reason = f"{reason} | ordId={fill.get('ord_id', '')} fee={fill.get('fee', 0):.8g}{fill.get('fee_ccy', '')}"
        self.logger.info(f"{self.inst_id} API买入成交: ordId={fill.get('ord_id')} 均价 {price:.8g} 数量 {qty:.8g} 花费 {buy_amount:.2f}", "buy_execute")
        # 重置尝试计数
        self._buy_attempt_count = 0
        self._last_buy_error = ""

        # 更新本地状态
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.has_position = True
        self.position_price = price
        self.position_qty = qty
        self.position_amount = buy_amount
        self.buy_time = now_str
        self.buy_reason = reason
        self.pair_type = pair_name
        self.pair_sell_line = sell_key
        self.is_paired = True
        self.pair_buy_price = price
        self.pair_buy_time = now_str
        self.is_sold = False
        self.sell_time = ""
        self.sell_reason = ""
        self.check_level = "T3"
        self.last_check_time = now_str

        # 设置对应卖点价格
        self.pair_sell_line_price = getattr(self.levels, sell_key)

        # 止盈初始化
        self.profit_triggered = False
        self.peak_price = price
        self.stop_price = 0
        self._profit_notified_steps = set()
        self._last_processed_price = price

        # 更新资金和持仓
        _update_funds_for_view(get_okx_balance(force_refresh=True))
        refresh_positions_cache(force=True)
        self._remove_from_buy_queue()
        self._save_state()

        save_trade({
            "timestamp": now_str,
            "inst_id": self.inst_id,
            "direction": "BUY",
            "price": price,
            "qty": qty,
            "amount": buy_amount,
            "remaining_funds": f"{get_okx_balance():.2f}",
            "position_price": price,
            "profit": "0",
            "profit_pct": "0",
            "reason": reason
        })

        msg = f"{self.inst_id} {reason} @ {price:.8g}, 金额 {buy_amount:.2f}, 数量 {qty:.8g}, 买点: {buy_key}"
        self.logger.info(msg, "buy_execute")
        self._push_dingtalk(f"💰 买入\n\n{self.inst_id}\n{pair_name}买点: {buy_key}\n价格: {price:.8g}\n金额: {buy_amount:.2f}")
        return msg

    def _remove_from_buy_queue(self):
        self.buy_queue = [r for r in self.buy_queue if r.get("inst_id") != self.inst_id]
        save_buy_queue(self.buy_queue)

    def _execute_sell(self, price, reason):
        can, msg = self._can_sell()
        if not can:
            self.logger.debug(f"{self.inst_id} 卖出跳过: {msg}")
            return None

        qty = self.position_qty
        fill = self.client.market_sell_base(self.inst_id, qty)
        if not fill or fill.get("qty", 0) <= 0:
            self.logger.error(f"{self.inst_id} API卖出失败，持仓保留: {fill}")
            self._push_dingtalk(f"⚠️ 卖出失败\n\n{self.inst_id}\n{reason}\n数量: {qty:.8g}\nAPI返回: {fill}")
            return None
        if fill["qty"] < qty * 0.999:
            self.logger.warn(f"{self.inst_id} 卖出按 lotSz 取整/部分成交: 成交 {fill['qty']:.8g} / 持仓 {qty:.8g}", "sell_execute")
        price = fill["price"]
        qty = fill["qty"]
        amount = fill["proceeds"]
        reason = f"{reason} | ordId={fill.get('ord_id', '')} fee={fill.get('fee', 0):.8g}{fill.get('fee_ccy', '')}"
        self.logger.info(f"{self.inst_id} API卖出成交: ordId={fill.get('ord_id')} 均价 {price:.8g} 数量 {qty:.8g} 到手 {amount:.2f}", "sell_execute")

        profit = amount - self.position_amount
        profit_pct = profit / self.position_amount * 100 if self.position_amount > 0 else 0
        cost_price = self.position_price
        cached_pair_type = self.pair_type
        cached_pair_sell_line = self.pair_sell_line
        cached_pair_sell_line_price = self.pair_sell_line_price
        cached_buy_time = self.buy_time
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self._last_profit = profit
        self._last_profit_pct = profit_pct

        self.has_position = False
        self.position_price = 0
        self.position_qty = 0
        self.position_amount = 0
        self.buy_time = ""
        self.buy_reason = ""
        self.is_paired = False
        self.is_sold = True
        self.sell_time = now_str
        self.sell_reason = reason
        self.profit_triggered = False
        self.peak_price = 0
        self.stop_price = 0
        self.check_level = ""
        self.last_check_time = ""
        self._profit_notified_steps = set()

        cooldown_days = self.params.get("cooldown_days", 7)
        cooldown_until = (datetime.now() + timedelta(days=cooldown_days)).strftime("%Y-%m-%d %H:%M:%S")
        save_cooldown_entry(self.inst_id, cooldown_until)
        self.cooldown[self.inst_id] = cooldown_until
        self._add_to_buy_queue("静默期_排除")

        self.pair_buy_price = cost_price
        self.pair_buy_time = cached_buy_time
        self.pair_sell_line_price = cached_pair_sell_line_price
        self.pair_type = cached_pair_type
        self.pair_sell_line = cached_pair_sell_line

        _update_funds_for_view(get_okx_balance(force_refresh=True))
        refresh_positions_cache(force=True)
        self._save_state()

        save_trade({
            "timestamp": now_str,
            "inst_id": self.inst_id,
            "direction": "SELL",
            "price": price,
            "qty": qty,
            "amount": amount,
            "remaining_funds": f"{get_okx_balance():.2f}",
            "position_price": cost_price,
            "profit": f"{profit:.12g}",
            "profit_pct": f"{profit_pct:.6f}",
            "reason": reason
        })

        msg = f"{self.inst_id} {reason} @ {price:.8g}, 数量 {qty:.8g}, 盈亏 {profit:.2f} ({profit_pct:.2f}%)"
        self.logger.info(msg, "sell_execute")
        self._push_dingtalk(f"💸 卖出\n\n{self.inst_id}\n{reason}\n价格: {price:.8g}\n盈亏: {profit:.2f} ({profit_pct:.2f}%)")
        return msg

    def _add_to_buy_queue(self, status):
        for row in self.buy_queue:
            if row.get("inst_id") == self.inst_id:
                row["status"] = status
                row["last_check_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                save_buy_queue(self.buy_queue)
                return
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.buy_queue.append({
            "inst_id": self.inst_id, 
            "status": status, 
            "ref_high": str(self.levels.high), 
            "ref_low": str(self.levels.low), 
            "add_time": now_str, 
            "last_check_time": now_str
        })
        save_buy_queue(self.buy_queue)

    def _check_take_profit(self, price):
        if not self.has_position or self.position_price <= 0:
            return None
        profit_pct = (price - self.position_price) / self.position_price
        if price > self.peak_price:
            self.peak_price = price

        # 根据盈利比例查找对应止损比例
        stop_ratio = get_stop_loss(profit_pct)
        if stop_ratio == 0 and profit_pct >= 0.20:
            # 清仓
            self.logger.info(f"{self.inst_id} 盈利 ≥20%，立即清仓", "profit_trigger")
            return self._execute_sell(price, f"盈利率 {profit_pct*100:.1f}% ≥20%，清仓")

        # 更新止损价
        new_stop = self.peak_price * (1 - stop_ratio)
        if new_stop > self.stop_price:
            self.stop_price = new_stop
        # 如果还未激活止盈，但盈利超过5%且止损价已设置，则激活
        if profit_pct >= 0.05 and not self.profit_triggered:
            self.profit_triggered = True
            self.check_level = "T0"
            self._push_dingtalk(f"📈 止盈激活\n\n{self.inst_id}\n盈利: {profit_pct*100:.2f}%\n峰值: {self.peak_price:.8g}\n止盈价: {self.stop_price:.8g}")
            self.logger.info(f"{self.inst_id} 止盈激活: 盈利 {profit_pct*100:.2f}%, 止盈价 {self.stop_price:.8g}", "profit_trigger")
            self._save_state()

        # 检查是否触发止损
        if self.profit_triggered and price <= self.stop_price:
            self.logger.info(f"{self.inst_id} 触发止盈清仓: 当前价 {price:.8g} ≤ 止盈价 {self.stop_price:.8g}", "stop_loss_trigger")
            return self._execute_sell(price, f"止盈清仓 (峰值 {self.peak_price:.8g}, 止损 {stop_ratio*100:.1f}%)")
        return None

    def check_buy_queue(self, price):
        if self.has_position or self._is_in_cooldown() or self.inst_id in self.observe_list:
            return None
        # 寻找最佳买点（价格低于买点）
        buy_key, buy_level = self._get_buy_level(price)
        if buy_key is not None:
            sell_key, pair_name = self.levels.pair_map[buy_key][0], self.levels.pair_map[buy_key][1]
            return self._execute_buy(price, f"价格 {price:.8g} 低于买点 {buy_level:.8g}", buy_key, sell_key, pair_name)
        return None

    def check_near_price(self, price):
        # 近价买入策略（当价格接近买点时但还未跌破，可提前买入？根据规则，必须小于买点才买入）
        # 这里仅做检查，由 check_buy_queue 统一处理
        return None

    def update_price(self, price, ts):
        alerts = []
        if price == self._last_processed_price and ts == self._last_processed_ts:
            return alerts
        self._last_processed_price = price
        self._last_processed_ts = ts

        # 更新最高/最低
        if price > self.high:
            self.high = price
            self.levels.update(self.high, self.low)
            self.last_new_high_ts = ts
            save_high_low_entry(self.inst_id, self.high, self.low)
            alerts.append(f"创新高 {price:.8g}")
        if price < self.low:
            self.low = price
            self.levels.update(self.high, self.low)
            self.last_new_low_ts = ts
            save_high_low_entry(self.inst_id, self.high, self.low)
            alerts.append(f"创新低 {price:.8g}")

        # 持仓状态下的止盈检查
        if self.has_position:
            profit_pct = (price - self.position_price) / self.position_price if self.position_price > 0 else 0
            self._update_check_level(profit_pct)
            if self.check_level == "T0":
                result = self._check_take_profit(price)
                if result:
                    alerts.append(f"止盈清仓 @ {price:.8g}")
                    self._save_state()
                    return alerts

        # 卖出条件：价格触及对应卖点（同时满足有持仓且配对未完成）
        if self.has_position and self.is_paired and not self.is_sold:
            sell_price = self.pair_sell_line_price
            if price >= sell_price:
                result = self._execute_sell(price, f"{self.pair_type}卖点 {self.pair_sell_line} 触发")
                if result:
                    alerts.append(f"{self.pair_type}卖点清仓 @ {price:.8g}")
                    self._save_state()
                    return alerts

        # 买入条件（价格跌破买点）
        if not self.has_position and not self._is_in_cooldown() and self.inst_id not in self.observe_list:
            buy_key, buy_level = self._get_buy_level(price)
            if buy_key is not None:
                # 避免重复触发，可通过记录上次触发状态
                # 这里直接尝试买入，由内部检查是否已持仓
                result = self.check_buy_queue(price)
                if result:
                    alerts.append(f"买入 @ {price:.8g}, 买点: {buy_key}")
                    self._save_state()
                    return alerts

        # 下破预警（可选）
        # 可保留，但为了简洁，略去
        self._save_state()
        return alerts

    def _save_state(self):
        positions = load_positions()
        if self.has_position:
            cp = self._last_processed_price if self._last_processed_price > 0 else self.position_price
            qty = self.position_qty
            cost = self.position_amount
            mv = cp * qty if cp > 0 else cost
            upnl = mv - cost if cost > 0 else 0
            upnl_pct = (upnl / cost * 100.0) if cost > 0 else 0
            positions[self.inst_id] = {
                "position_price": self.position_price,
                "position_qty": self.position_qty,
                "position_amount": self.position_amount,
                "buy_time": self.buy_time,
                "buy_reason": self.buy_reason,
                "pair_type": self.pair_type,
                "pair_sell_line": self.pair_sell_line,
                "peak_price": self.peak_price,
                "stop_price": self.stop_price,
                "profit_triggered": self.profit_triggered,
                "check_level": self.check_level,
                "last_check_time": self.last_check_time,
                "current_price": cp,
                "market_value": mv,
                "unrealized_pnl": upnl,
                "unrealized_pnl_pct": upnl_pct,
            }
        else:
            positions.pop(self.inst_id, None)
        save_positions(positions)

        if self.is_paired or self.is_sold:
            payload = {
                "pair_type": self.pair_type,
                "is_paired": self.is_paired,
                "buy_price": self.pair_buy_price,
                "buy_time": self.pair_buy_time,
                "sell_line": self.pair_sell_line,
                "sell_line_price": self.pair_sell_line_price,
                "is_sold": self.is_sold,
                "sell_time": self.sell_time,
                "sell_reason": self.sell_reason,
            }
            if self.is_sold:
                payload["profit"] = f"{self._last_profit:.12g}"
                payload["profit_pct"] = f"{self._last_profit_pct:.6f}"
            save_pair_status(self.inst_id, payload)
        else:
            rows = _read_csv(PAIR_STATUS_FILE, PAIR_STATUS_HEADER)
            rows = [r for r in rows if r.get("inst_id") != self.inst_id]
            _write_csv_all(PAIR_STATUS_FILE, PAIR_STATUS_HEADER, rows)

    def _push_dingtalk(self, text):
        if self.ding_webhook:
            push_to_dingtalk(self.ding_webhook, self.ding_secret, text)

# ==================== 行情引擎 ====================
class MarketEngine:
    def __init__(self, logger, ding_webhook=None, ding_secret=None, system_params=None):
        self.logger = logger
        self.ding_webhook = ding_webhook
        self.ding_secret = ding_secret
        self.params = system_params or {}
        self.states = {}
        self.lock = threading.Lock()
        self._last_buy_queue_check = 0

    def add_state(self, inst_id, ref_high, ref_low):
        state = SymbolState(inst_id, ref_high, ref_low, self.logger, self.ding_webhook, self.ding_secret, self.params)
        self.states[inst_id] = state
        hl = load_high_low()
        if inst_id in hl:
            state.high = hl[inst_id]["all_time_high"]
            state.low = hl[inst_id]["all_time_low"]
            state.levels.update(state.high, state.low)
        pos = load_positions()
        if inst_id in pos:
            data = pos[inst_id]
            state.has_position = data.get("position_qty", 0) > 0
            state.position_price = data.get("position_price", 0)
            state.position_qty = data.get("position_qty", 0)
            state.position_amount = data.get("position_amount", 0)
            state.buy_time = data.get("buy_time", "")
            state.buy_reason = data.get("buy_reason", "")
            state.pair_type = data.get("pair_type", "")
            state.pair_sell_line = data.get("pair_sell_line", "")
            state.peak_price = data.get("peak_price", 0)
            state.stop_price = data.get("stop_price", 0)
            state.profit_triggered = data.get("profit_triggered", False)
            state.check_level = data.get("check_level", "T3")
            state.last_check_time = data.get("last_check_time", "")
            cp = data.get("current_price", 0)
            if cp and float(cp) > 0:
                state._last_processed_price = float(cp)
        pair = load_pair_status()
        if inst_id in pair:
            data = pair[inst_id]
            state.is_paired = data.get("is_paired", False)
            state.pair_buy_price = data.get("buy_price", 0)
            state.pair_buy_time = data.get("buy_time", "")
            state.pair_sell_line_price = data.get("sell_line_price", 0)
            state.is_sold = data.get("is_sold", False)
            state.sell_time = data.get("sell_time", "")
            state.sell_reason = data.get("sell_reason", "")
            if data.get("pair_type"):
                state.pair_type = data["pair_type"]
            state._last_profit = data.get("profit", 0.0)
            state._last_profit_pct = data.get("profit_pct", 0.0)
        cooldown = load_cooldown()
        if inst_id in cooldown:
            state.cooldown[inst_id] = cooldown[inst_id]
        state._sync_buy_queue()

    def on_trade(self, inst_id, price, ts):
        with self.lock:
            state = self.states.get(inst_id)
            if state is None:
                return
            alerts = state.update_price(price, ts)
        if alerts:
            key = [a for a in alerts if any(k in a for k in ("止盈", "清仓", "买入", "卖出"))]
            if key:
                self.logger.info(f"[{inst_id}] " + " | ".join(key), "trade_event")
            else:
                self.logger.debug(f"[{inst_id}] " + " | ".join(alerts))

    def _check_buy_queue_all(self):
        self.logger.info("执行买入队列批量检查", "config_load")
        queue = load_buy_queue()
        checked = 0
        for row in queue:
            if row.get("status") != "待买入":
                continue
            inst_id = row.get("inst_id")
            if not inst_id:
                continue
            state = self.states.get(inst_id)
            if not state:
                continue
            price = state._last_processed_price
            if price <= 0:
                continue
            checked += 1
            result = state.check_buy_queue(price)
            if result:
                self.logger.info(f"买入队列执行: {result}", "buy_execute")
        self.logger.info(f"买入队列检查完成: 检查 {checked} 个币对", "config_load")

# ==================== OKX 行情流 ====================
def subscribe_okx_trades(sock, inst_ids):
    payload = {"op": "subscribe", "args": [{"channel": "trades", "instId": inst_id} for inst_id in inst_ids]}
    send_text(sock, json.dumps(payload, separators=(",", ":")))

def handle_okx_message(message, engine):
    if message == "pong":
        return
    try:
        data = json.loads(message)
    except:
        return
    if data.get("event") == "subscribe":
        return
    if data.get("event") == "error":
        engine.logger.error(f"OKX 订阅失败: {data}")
        return
    if data.get("arg", {}).get("channel") != "trades":
        return
    inst_id = data.get("arg", {}).get("instId", "")
    for trade in data.get("data", []):
        try:
            price = float(trade["px"])
        except:
            continue
        ts = int(trade.get("ts", time.time() * 1000)) / 1000
        engine.on_trade(inst_id, price, ts)

def stream_okx(engine, inst_ids):
    while True:
        sock = None
        last_recv = time.time()
        try:
            sock = connect_okx_ws()
            engine.logger.info("OKX WebSocket 连接成功", "startup")
            subscribe_okx_trades(sock, inst_ids)
            while True:
                try:
                    opcode, payload = recv_ws_frame(sock)
                    last_recv = time.time()
                except socket.timeout:
                    if time.time() - last_recv >= PING_INTERVAL:
                        send_text(sock, "ping")
                    continue
                if opcode == 0x1:
                    try:
                        handle_okx_message(payload.decode("utf-8"), engine)
                    except Exception as be:
                        engine.logger.error(f"OKX 业务处理异常(不重连): {be}")
                elif opcode == 0x8:
                    raise ConnectionError("服务端关闭")
                elif opcode == 0x9:
                    send_pong(sock, payload)
                elif opcode == 0xA:
                    continue
                refresh_positions_cache()
        except Exception as e:
            engine.logger.error(f"OKX 连接异常: {e}", "connection_error")
            time.sleep(RECONNECT_DELAY)
        finally:
            if sock:
                sock.close()

# ==================== 主函数 ====================
def main():
    print("=" * 60)
    print(f"{PROGRAM_NAME} {VERSION}")
    print(f"工作目录: {BASE_DIR}")
    print(f"数据目录: {DATA_DIR}")
    print("=" * 60)

    logger = Logger(LOG_FILE)

    ding_webhook = os.getenv("DINGTALK_WEBHOOK_okx")
    ding_secret = os.getenv("DINGTALK_SECRET_okx")
    if ding_webhook:
        logger.set_dingtalk(ding_webhook, ding_secret)
        logger.info("钉钉推送已启用", "startup")
    else:
        logger.warn("钉钉推送未配置，仅终端输出", "startup")

    try:
        config, system_params = load_config()
        logger.info(f"加载配置成功: {len(config)} 个币对", "config_load")
    except Exception as e:
        logger.error(f"配置加载失败: {e}", "config_load")
        sys.exit(1)

    if not config:
        logger.error("没有启用的币对", "config_load")
        sys.exit(1)

    trade_mode_cfg = os.getenv("OKX_TRADE_MODE", "").strip() or str(system_params.get("trade_mode", "demo"))
    init_okx_client(trade_mode_cfg, logger)
    print(f"交易模式: {TRADE_MODE_DESC[TRADE_MODE]}")
    min_amt = float(os.getenv("MIN_BUY_AMOUNT", "100"))
    print(f"单笔最小买入金额: {min_amt:.2f} USDT")
    cooldown_days = int(os.getenv("COOLDOWN_DAYS", system_params.get("cooldown_days", 7)))
    print(f"静默期天数: {cooldown_days} 天")

    engine = MarketEngine(logger, ding_webhook, ding_secret, system_params)

    blacklist = load_blacklist()
    if blacklist:
        logger.info(f"加载黑名单: {len(blacklist)} 个币对", "startup")
        for b in blacklist:
            print(f"  黑名单: {b}")
    observe_list = load_observe_list()
    if observe_list:
        logger.info(f"加载待观察列表: {len(observe_list)} 个币对", "startup")
        for o in observe_list:
            print(f"  待观察: {o}")

    okx_inst_ids = []
    logger.info("初始化币对状态...", "startup")
    for inst_id, data in config.items():
        engine.add_state(inst_id, data["ref_high"], data["ref_low"])
        state = engine.states[inst_id]
        print(f"\n{inst_id} 价格点位:")
        print(f"  最高: {state.high:.8g}  最低: {state.low:.8g}")
        print(f"  回撤上: {state.levels.pullback_up:.8g}  回撤2下: {state.levels.pullback2_down:.8g}")
        print(f"  回调上: {state.levels.callback_up:.8g}  回调2下: {state.levels.callback2_down:.8g}")
        print(f"  过渡上: {state.levels.transition_up:.8g}  过渡2下: {state.levels.transition2_down:.8g}")
        print(f"  极限上: {state.levels.limit_up:.8g}  极限2下: {state.levels.limit2_down:.8g}")
        if state._okx_available:
            okx_inst_ids.append(inst_id)
            print(f"{inst_id}: OKX 可交易")
        else:
            print(f"{inst_id}: OKX 不可交易（已排除）")

    api_pos = get_api_positions()
    if api_pos:
        print("\n当前 OKX 现货持仓:")
        for inst_id, data in api_pos.items():
            print(f"  {inst_id}: 数量 {data['position_qty']:.8g}, 成本 {data['position_price']:.8g}")

    positions = load_positions()
    for inst_id, data in positions.items():
        print(f"恢复持仓: {inst_id} 数量 {data.get('position_qty', 0):.8g} @ {data.get('position_price', 0):.8g}")

    queue = load_buy_queue()
    pending = [r for r in queue if r.get("status") == "待买入"]
    print(f"买入队列: {len(pending)} 个币对待买入")
    print(f"OKX 账户 USDT 余额: {get_okx_balance(force_refresh=True):.2f}")

    if not okx_inst_ids:
        logger.error("没有可交易的币对，退出", "startup")
        sys.exit(1)

    t = threading.Thread(target=stream_okx, args=(engine, okx_inst_ids), daemon=True)
    t.start()
    logger.info(f"OKX 行情线程启动: {len(okx_inst_ids)} 个币对", "startup")

    logger.info("=" * 60, "startup")
    logger.info("系统运行中，按 Ctrl+C 停止", "startup")
    logger.info("检查频率: T0(实时) T1(1分钟) T2(15分钟) T3(30分钟) 买入队列(每日11:30/23:30)", "startup")
    logger.info("=" * 60, "startup")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("用户中断，程序停止", "shutdown")
    finally:
        _update_funds_for_view(get_okx_balance(force_refresh=True))
        logger.info("程序退出", "shutdown")

if __name__ == "__main__":
    main()