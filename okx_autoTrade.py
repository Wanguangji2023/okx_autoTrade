#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OKX / Binance 实时成交价监听 + 价格点位计算 + 模拟交易 + 钉钉提醒

版本: v2.7
更新: 2026-09-04

核心功能：
1. Excel 配置文件输入（无需人工交互）
2. 基于参考最高/最低实时计算各价位点位，新高新低刷新
3. 成对买卖：回撤下→回撤上，回调下→回调上，次2→中间
4. 通用清仓规则：任何有持仓，价格到达回撤上即清仓
5. 止盈机制：盈利≥5%启动，回撤1.5%清仓
6. 同一币对只持有一次，卖出后7天静默期
7. 分层轮询：T0实时/T1每分钟/T2每15分/T3每30分
8. 买入信号修复：以最低触及的触发线为准
9. 资金管理修复：remaining_funds 正确累加/扣减
10. 完整的运行日志（精简） + 钉钉异常推送
v2.7 修复：
- 【账单盈亏】tradeRecord 增加 profit/profit_pct 列，SELL 写入实际盈亏、position_price 填买入成本，个币盈亏直接可读
- 【持仓浮盈】positions 增加 current_price/market_value/unrealized_pnl/unrealized_pnl_pct 列
- 【资金总览】funds 增加 总持仓市值/总浮盈/总已实现盈亏/净资产(NAV) 列，总持仓一目了然
- 【资金Bug修复】买入判断与金额计算使用全局 get_remaining_funds()，避免跨币对实例缓存不同步导致资金被超额扣减为负数
- 【买卖追溯】卖出后 pair_status 保留 pair_type/sell_line 历史值 + 该笔 profit/profit_pct，可按单币审计盈亏
- 【buy_queue一致性】_sync_buy_queue 修改status后立即持久化，已持仓/静默期状态不再误显示"待买入"
- 【CSV升级迁移】_ensure_csv_header 自动检测 header 变化并按列名迁移旧数据，结构升级无历史丢失
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
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from dotenv import load_dotenv

# ==================== 加载环境变量 ====================
load_dotenv()

# ==================== 配置常量 ====================
PROGRAM_NAME = "okx_ding_ok"
VERSION = "v2.7"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, f"{PROGRAM_NAME}_config.xlsx")
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

# v2.7: 增加 profit/profit_pct 列（个币盈亏直接写入账单），position_price 作为买入成本对账
TRADE_HEADER = ["timestamp", "inst_id", "direction", "price", "qty", "amount", "remaining_funds", "position_price", "profit", "profit_pct", "reason"]
# v2.7: 增加当前价/持仓市值/浮盈/浮盈比例，个币盈亏与总持仓盈亏直接体现
POSITIONS_HEADER = ["inst_id", "position_price", "position_qty", "position_amount", "buy_time", "buy_reason", "pair_type", "pair_sell_line", "peak_price", "stop_price", "profit_triggered", "check_level", "last_check_time", "current_price", "market_value", "unrealized_pnl", "unrealized_pnl_pct"]
COOLDOWN_HEADER = ["inst_id", "sell_time", "cooldown_until"]
HIGH_LOW_HEADER = ["inst_id", "all_time_high", "all_time_low", "last_update"]
PAIR_STATUS_HEADER = ["inst_id", "pair_type", "is_paired", "buy_price", "buy_time", "sell_line", "sell_line_price", "is_sold", "sell_time", "sell_reason", "profit", "profit_pct"]
BUY_QUEUE_HEADER = ["inst_id", "status", "ref_high", "ref_low", "add_time", "last_check_time"]
FUNDS_HEADER = ["remaining_funds", "total_position_market_value", "total_unrealized_pnl", "total_realized_pnl", "net_asset_value", "last_update"]

OKX_WS_HOST = "ws.okx.com"
OKX_WS_PORT = 8443
OKX_WS_PATH = "/ws/v5/public"
OKX_REST_BASE = "https://www.okx.com"
BINANCE_WS_HOST = "stream.binance.com"
BINANCE_WS_PORT = 9443

PING_INTERVAL = 25
RECONNECT_DELAY = 3
DEFAULT_PUSH_INTERVAL = 3.0

BREAK_ALERT_MAX_PER_DAY = 4
BREAK_ALERT_MIN_INTERVAL_SEC = 17 * 60
BASE_TIME_NODES = [17, 34, 63, 125, 250, 500, 1000]

# RLock: save_funds 内部会 load_positions 再次加锁，避免同线程死锁
CSV_LOCK = threading.RLock()

# ==================== 资金管理（全局） ====================
# 全局资金变量，程序启动时加载，运行时在内存中维护
_global_remaining_funds = 1000000.0
_global_total_realized_pnl = 0.0   # 已实现盈亏累计（所有已平仓交易的 profit 之和）
_funds_loaded = False


def load_funds() -> float:
    """加载剩余资金 + 已实现盈亏（程序启动时调用）"""
    global _global_remaining_funds, _global_total_realized_pnl, _funds_loaded
    if _funds_loaded:
        return _global_remaining_funds

    _ensure_csv_header(FUNDS_FILE, FUNDS_HEADER)

    if not os.path.exists(FUNDS_FILE):
        _global_remaining_funds = 1000000.0
        _global_total_realized_pnl = 0.0
        _funds_loaded = True
        return _global_remaining_funds

    try:
        with CSV_LOCK, open(FUNDS_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                _global_remaining_funds = float(row.get("remaining_funds") or 1000000)
                # 若旧文件没有已实现盈亏，则用 tradeRecord 的历史 SELL 记录重建
                raw_pnl = row.get("total_realized_pnl", "")
                if raw_pnl not in ("", None):
                    _global_total_realized_pnl = float(raw_pnl)
                else:
                    _global_total_realized_pnl = _rebuild_realized_pnl_from_trades()
                break
    except Exception:
        _global_remaining_funds = 1000000.0
        _global_total_realized_pnl = 0.0

    _funds_loaded = True
    return _global_remaining_funds


def _rebuild_realized_pnl_from_trades() -> float:
    """从 tradeRecord 的历史 SELL 记录重建累计已实现盈亏。
    优先使用 profit 列；否则若 SELL.position_price 非空则用 qty*position_price 算成本；
    若 SELL.position_price 为空（老数据 v2.6），则按 inst_id 查找最近 BUY 记录的 position_price/qty 匹配推导。"""
    total = 0.0
    if not os.path.exists(TRADE_FILE):
        return total
    try:
        # 读取全部交易（数据量通常数百条，加载无压力）
        rows = []
        with CSV_LOCK, open(TRADE_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception:
        return 0.0
    # 先累积当前看到的 BUY 持仓成本（同一币对多笔：新的覆盖成本，考虑本策略每币对只买一次）
    buy_cost_by_inst = {}  # inst_id -> (price, qty, amount)
    for row in rows:
        direction = row.get("direction", "")
        inst_id = row.get("inst_id", "")
        if direction == "BUY":
            try:
                price = float(row.get("price") or 0)
                qty = float(row.get("qty") or 0)
                amount = float(row.get("amount") or 0)
                if qty > 0 and amount > 0:
                    buy_cost_by_inst[inst_id] = (price, qty, amount)
            except Exception:
                pass
            continue
        if direction == "SELL":
            # 1) 优先 profit 列
            raw_profit = row.get("profit", "")
            if raw_profit not in ("", None):
                try:
                    total += float(raw_profit)
                    continue
                except Exception:
                    pass
            # 2) SELL 自己的 position_price 列
            try:
                sell_amount = float(row.get("amount") or 0)
                qty = float(row.get("qty") or 0)
                pos_price = row.get("position_price", "")
                if pos_price not in ("", None) and float(pos_price) > 0 and qty > 0:
                    total += (sell_amount - float(pos_price) * qty)
                    continue
            except Exception:
                pass
            # 3) 用同 inst_id BUY 成本推导
            if inst_id in buy_cost_by_inst and qty > 0:
                buy_price, buy_qty, buy_amount = buy_cost_by_inst[inst_id]
                # 如果 qty 接近匹配，用 buy_amount 直接当成本；否则按比例
                ratio = qty / buy_qty if buy_qty > 0 else 1.0
                cost = buy_amount * ratio
                total += (sell_amount - cost)
    return total


def save_funds(extra: Optional[Dict] = None):
    """保存资金总览（剩余资金/总持仓市值/总浮盈/已实现/净资产）"""
    global _global_remaining_funds, _global_total_realized_pnl
    try:
        # 汇总持仓层的浮盈
        total_mv = 0.0
        total_upnl = 0.0
        try:
            positions = load_positions()
            for _, p in positions.items():
                mv = float(p.get("market_value", 0) or 0)
                upnl = float(p.get("unrealized_pnl", 0) or 0)
                total_mv += mv
                total_upnl += upnl
        except Exception:
            pass

        nav = _global_remaining_funds + total_mv
        row = {
            "remaining_funds": f"{_global_remaining_funds:.12g}",
            "total_position_market_value": f"{total_mv:.12g}",
            "total_unrealized_pnl": f"{total_upnl:.12g}",
            "total_realized_pnl": f"{_global_total_realized_pnl:.12g}",
            "net_asset_value": f"{nav:.12g}",
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        if extra:
            row.update(extra)
        with CSV_LOCK, open(FUNDS_FILE, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=FUNDS_HEADER)
            writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[资金] 保存失败: {e}")


def get_remaining_funds() -> float:
    """获取当前剩余资金"""
    global _global_remaining_funds
    return _global_remaining_funds


def get_total_realized_pnl() -> float:
    global _global_total_realized_pnl
    return _global_total_realized_pnl


def add_realized_pnl(profit: float):
    """平仓时累计已实现盈亏"""
    global _global_total_realized_pnl
    _global_total_realized_pnl += profit


def update_remaining_funds(amount: float):
    """更新剩余资金（正数增加，负数减少）"""
    global _global_remaining_funds
    _global_remaining_funds += amount
    save_funds()


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


# ==================== CSV 数据操作 ====================
def _ensure_csv_header(file_path: str, header: List[str]):
    """确保 CSV 文件使用最新 header，若旧文件缺少列则自动迁移（补空值）。"""
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        try:
            with CSV_LOCK, open(file_path, 'w', encoding='utf-8-sig', newline='') as f:
                csv.writer(f).writerow(header)
        except Exception as e:
            print(f"[CSV] 初始化失败 {file_path}: {e}")
        return
    # 读取现有 header 做对比
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
    # header 变动：迁移
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
            # 老数据无 current_price 列时，用 position_price 暂代，避免浮盈被错误显示为 -100%
            # 首次 update_price 到来后会刷新成真实价格
            raw_cp = row.get("current_price", "") or ""
            if raw_cp != "" and float(raw_cp) > 0:
                current_price = float(raw_cp)
            else:
                current_price = position_price
            # 老数据兼容：如果 market_value / unrealized_pnl 为空则用 current_price * qty 推导
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
                # 修复 NoneType.lower() 报错：row.get 在列缺失+某些边缘CSV写入场景可能返回 None/空，统一归一化
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
                # 修复 NoneType.lower()：列缺失/边缘写入场景下 row.get(...) 可能返回 None，统一归一化
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
            # 卖出时也保留 pair_type/sell_line/sell_line_price 作为历史痕迹（is_paired=false 但信息可追溯）
            if data.get("sell_line") not in (None, ""):
                row["sell_line"] = data["sell_line"]
            elif data.get("is_sold", False) and row.get("sell_line", "") in (None, ""):
                # 保持旧值
                pass
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


# ==================== 配置加载 ====================
def load_config() -> Tuple[Dict, Dict]:
    import openpyxl
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"配置文件不存在: {CONFIG_FILE}")
    config = {}
    system_params = {
        "total_funds": 1000000.0, 
        "buy_ratio": 0.02, 
        "cooldown_days": 7, 
        "profit_trigger": 0.05, 
        "stop_loss": 0.015, 
        "profit_step": 0.01, 
        "break_alert_max": 4, 
        "break_alert_interval": 1020, 
        "push_interval": 3.0, 
        "data_source": "OKX,Binance"
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
    return config, system_params


# ==================== 价位计算 ====================
class PriceLevels:
    def __init__(self, high: float, low: float): 
        self.update(high, low)
    
    def update(self, high: float, low: float):
        self.high = high
        self.low = low
        self.mid = (high + low) / 2
        self.high_mid = (high + self.mid) / 2
        self.low_mid = (self.mid + low) / 2
        self.pullback_mid = (high + self.high_mid) / 2
        self.pullback_up = (high + self.pullback_mid) / 2
        self.pullback_down = (self.pullback_mid + self.high_mid) / 2
        self.callback_mid = (self.high_mid + self.mid) / 2
        self.callback_up = (self.high_mid + self.callback_mid) / 2
        self.callback_down = (self.callback_mid + self.mid) / 2
        self.sub1 = (self.low_mid + low) / 2
        self.sub2 = (self.sub1 + low) / 2
        self.sub3 = (self.sub2 + low) / 2
        self.sub4 = (self.sub3 + low) / 2
    
    def describe(self) -> str:
        return (
            f"最高={self.high:.8g} 最低={self.low:.8g}\n"
            f"中间={self.mid:.8g} 高位中间={self.high_mid:.8g} 低位中间={self.low_mid:.8g}\n"
            f"回撤上={self.pullback_up:.8g} 回撤中={self.pullback_mid:.8g} 回撤下={self.pullback_down:.8g}\n"
            f"回调上={self.callback_up:.8g} 回调中={self.callback_mid:.8g} 回调下={self.callback_down:.8g}\n"
            f"次1={self.sub1:.8g} 次2={self.sub2:.8g} 次3={self.sub3:.8g} 次4={self.sub4:.8g}"
        )


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


def connect_binance_ws(): 
    return _ws_handshake(BINANCE_WS_HOST, BINANCE_WS_PORT, "/ws")


# ==================== 工具函数 ====================
def to_binance_symbol(inst_id): 
    return inst_id.replace("-", "").upper()


def to_okx_inst_id(symbol):
    s = symbol.upper()
    if "-" in s: 
        return s
    for q in ("USDT", "USD", "BTC", "ETH"):
        if s.endswith(q) and len(s) > len(q):
            return f"{s[:-len(q)]}-{q}"
    return s


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


def get_time_nodes(elapsed_min):
    nodes = [n for n in BASE_TIME_NODES if elapsed_min >= n]
    if elapsed_min >= 2000:
        k = int(elapsed_min // 1000)
        for i in range(2, k + 1): 
            nodes.append(i * 1000)
    return nodes


def is_price_near_level(price, level, threshold=0.05):
    return level > 0 and level * (1 - threshold) <= price <= level * (1 + threshold)


# ==================== 币对状态管理 ====================
class SymbolState:
    def __init__(self, inst_id, ref_high, ref_low, logger, ding_webhook=None, ding_secret=None, system_params=None):
        self.inst_id = inst_id
        self.logger = logger
        self.ding_webhook = ding_webhook
        self.ding_secret = ding_secret
        self.params = system_params or {}
        self.total_funds = self.params.get("total_funds", 1000000)
        
        # 加载持久化状态
        self.positions = load_positions()
        self.cooldown = load_cooldown()
        self.high_low = load_high_low()
        self.pair_status = load_pair_status()
        self.buy_queue = load_buy_queue()
        
        # 从全局资金读取
        self.remaining_funds = get_remaining_funds()
        
        hl = self.high_low.get(inst_id, {})
        self.high = hl.get("all_time_high", ref_high)
        self.low = hl.get("all_time_low", ref_low)
        self.levels = PriceLevels(self.high, self.low)
        
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
        
        pair = self.pair_status.get(inst_id, {})
        self.is_paired = pair.get("is_paired", False)
        self.pair_buy_price = pair.get("buy_price", 0)
        self.pair_buy_time = pair.get("buy_time", "")
        self.pair_sell_line_price = pair.get("sell_line_price", 0)
        self.is_sold = pair.get("is_sold", False)
        self.sell_time = pair.get("sell_time", "")
        self.sell_reason = pair.get("sell_reason", "")
        
        self._sync_buy_queue()
        self.below_pullback_since = None
        self.below_callback_since = None
        self.below_sub2_since = None
        self.last_new_high_ts = time.time()
        self.last_new_low_ts = time.time()
        self.high_alert_sent = set()
        self.low_alert_sent = set()
        self.break_alerts = {name: {"count": 0, "last_ts": 0.0, "date": ""} for name in ("回撤上", "回撤下", "回调上", "回调下", "次2", "中间")}
        self.broken_levels = set()
        self._last_processed_price = 0.0
        self._last_processed_ts = 0.0
        self._profit_notified_steps = set()

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
        if not in_queue and not self.has_position and not self._is_in_cooldown():
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
        # 【修复3】只要有改动就持久化，否则持仓币对会一直显示"待买入"
        if changed:
            save_buy_queue(self.buy_queue)

    def _is_in_cooldown(self):
        until = self.cooldown.get(self.inst_id, "")
        if until:
            try:
                return datetime.now() < datetime.strptime(until, "%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        return False

    def _update_check_level(self, profit_pct):
        if profit_pct >= self.params.get("profit_trigger", 0.05):
            self.check_level = "T0"
        elif profit_pct > 0:
            self.check_level = "T1"
        elif self.pair_type in ("回撤下", "回调下"):
            self.check_level = "T2"
        else:
            self.check_level = "T3"
        self.last_check_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _can_buy(self):
        if self.has_position:
            return False, "已有持仓"
        if self._is_in_cooldown():
            return False, f"静默期中 (至 {self.cooldown.get(self.inst_id, '')})"
        # 【修复2】始终使用全局 get_remaining_funds() 做判断，避免 SymbolState 实例间缓存不同步把资金花成负数
        live_funds = get_remaining_funds()
        buy_amount = self.total_funds * self.params.get("buy_ratio", 0.02)
        if live_funds < buy_amount:
            return False, f"资金不足: 需要 {buy_amount:.2f}, 剩余 {live_funds:.2f}"
        if self.is_paired and not self.is_sold:
            return False, f"已配对未卖出 ({self.pair_type})"
        return True, ""

    def _can_sell(self):
        return (True, "") if self.has_position else (False, "无持仓")

    # ========== 买入执行（修复版） ==========
    def _determine_buy_level(self, price):
        """根据价格确定买入级别（从低到高优先级）"""
        if price < self.levels.sub2:
            return "次2-买入", "次2", "中间"
        elif price < self.levels.callback_down:
            return "回调下-买入", "回调下", "回调上"
        elif price < self.levels.pullback_down:
            return "回撤下-买入", "回撤下", "回撤上"
        else:
            return None, None, None

    def _execute_buy(self, price, reason, pair_type, sell_line):
        can, msg = self._can_buy()
        if not can:
            self.logger.debug(f"{self.inst_id} 买入跳过: {msg}")
            return None
        # 【修复2】使用全局最新资金，而不是 self.remaining_funds（可能跨实例陈旧）
        live_funds = get_remaining_funds()
        buy_amount = min(self.total_funds * self.params.get("buy_ratio", 0.02), live_funds)
        if buy_amount <= 0:
            self.logger.warn(f"{self.inst_id} 买入跳过: 买入金额为0 (剩余 {live_funds:.2f})", "insufficient_funds")
            return None
        qty = buy_amount / price
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 更新持仓状态
        self.has_position = True
        self.position_price = price
        self.position_qty = qty
        self.position_amount = buy_amount
        self.buy_time = now_str
        self.buy_reason = reason
        self.pair_type = pair_type
        self.pair_sell_line = sell_line
        self.is_paired = True
        self.pair_buy_price = price
        self.pair_buy_time = now_str
        self.is_sold = False
        self.sell_time = ""
        self.sell_reason = ""
        self.check_level = "T3"
        self.last_check_time = now_str

        # 计算配对卖出触发价
        if sell_line == "回撤上":
            self.pair_sell_line_price = self.levels.pullback_up
        elif sell_line == "回调上":
            self.pair_sell_line_price = self.levels.callback_up
        elif sell_line == "中间":
            self.pair_sell_line_price = self.levels.mid
        else:
            self.pair_sell_line_price = 0

        # 重置止盈状态
        self.profit_triggered = False
        self.peak_price = price
        self.stop_price = 0
        self._profit_notified_steps = set()
        # 更新最新处理价 -> 用于浮盈计算（买入时刻 current_price=price）
        self._last_processed_price = price

        # 扣减资金 - 使用全局资金
        update_remaining_funds(-buy_amount)
        self.remaining_funds = get_remaining_funds()

        # 从买入队列移除
        self._remove_from_buy_queue()

        # 保存状态（包含市值/浮盈计算与资金总览）
        self._save_state()

        # 记录交易：BUY 的 profit/profit_pct 固定为 0；position_price=买入价
        save_trade({
            "timestamp": now_str,
            "inst_id": self.inst_id,
            "direction": "BUY",
            "price": price,
            "qty": qty,
            "amount": buy_amount,
            "remaining_funds": f"{self.remaining_funds:.12g}",
            "position_price": price,
            "profit": "0",
            "profit_pct": "0",
            "reason": reason
        })

        msg = f"{self.inst_id} {reason} @ {price:.8g}, 金额 {buy_amount:.2f}, 数量 {qty:.8g}, 剩余 {self.remaining_funds:.2f}"
        self.logger.info(msg, "buy_execute")
        self._push_dingtalk(f"💰 买入\n\n{self.inst_id}\n{reason}\n价格: {price:.8g}\n金额: {buy_amount:.2f}")
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
        amount = qty * price
        profit = amount - self.position_amount
        profit_pct = profit / self.position_amount * 100 if self.position_amount > 0 else 0
        cost_price = self.position_price   # 保存成本价（后面清空后仍可用于写账单/pair_status）
        cached_pair_type = self.pair_type
        cached_pair_sell_line = self.pair_sell_line
        cached_pair_sell_line_price = self.pair_sell_line_price
        cached_buy_time = self.buy_time
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 增加资金 + 累计已实现盈亏
        update_remaining_funds(amount)
        add_realized_pnl(profit)
        self.remaining_funds = get_remaining_funds()

        # 清空持仓状态（【修复4】不清空 pair_type/sell_line，保留买入时的类型用于 pair_status 追溯）
        self.has_position = False
        self.position_price = 0
        self.position_qty = 0
        self.position_amount = 0
        self.buy_time = ""
        self.buy_reason = ""
        # 保留 pair_type / pair_sell_line / pair_sell_line_price 在实例变量中，
        # 因为下面 _save_state -> save_pair_status 会使用最新值继续写入
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

        # 加入静默期
        cooldown_days = self.params.get("cooldown_days", 7)
        cooldown_until = (datetime.now() + timedelta(days=cooldown_days)).strftime("%Y-%m-%d %H:%M:%S")
        save_cooldown_entry(self.inst_id, cooldown_until)
        self.cooldown[self.inst_id] = cooldown_until
        self._add_to_buy_queue("静默期_排除")

        # save_state 里会写 positions（会把该币移除）和 pair_status（保留历史）
        # 为了 pair_status 保留完整的买卖信息，这里先喂好值
        self.pair_buy_price = cost_price
        self.pair_buy_time = cached_buy_time
        self.pair_sell_line_price = cached_pair_sell_line_price
        # pair_type 和 sell_line 保持原值不清空（供 save_pair_status 写入）
        self.pair_type = cached_pair_type
        self.pair_sell_line = cached_pair_sell_line

        self._save_state()

        # 显式写入 pair_status 的 profit/profit_pct（_save_state 里 is_sold=True 分支不覆盖，这里补齐）
        save_pair_status(self.inst_id, {
            "pair_type": cached_pair_type,
            "is_paired": False,
            "buy_price": cost_price,
            "buy_time": cached_buy_time,
            "sell_line": cached_pair_sell_line,
            "sell_line_price": cached_pair_sell_line_price,
            "is_sold": True,
            "sell_time": now_str,
            "sell_reason": reason,
            "profit": f"{profit:.12g}",
            "profit_pct": f"{profit_pct:.6f}",
        })
        # pair_status 已写入，现在刷新 funds.csv（已实现盈亏累计 + 持仓市值变化）
        save_funds()

        # 记录交易：SELL 时 position_price=买入成本价，profit/profit_pct=实际盈亏
        save_trade({
            "timestamp": now_str,
            "inst_id": self.inst_id,
            "direction": "SELL",
            "price": price,
            "qty": qty,
            "amount": amount,
            "remaining_funds": f"{self.remaining_funds:.12g}",
            "position_price": cost_price,
            "profit": f"{profit:.12g}",
            "profit_pct": f"{profit_pct:.6f}",
            "reason": reason
        })

        msg = f"{self.inst_id} {reason} @ {price:.8g}, 数量 {qty:.8g}, 盈亏 {profit:.2f} ({profit_pct:.2f}%), 剩余 {self.remaining_funds:.2f}"
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
        trigger = self.params.get("profit_trigger", 0.05)
        if profit_pct >= trigger:
            if not self.profit_triggered:
                self.profit_triggered = True
                self.stop_price = self.peak_price * (1 - self.params.get("stop_loss", 0.015))
                self._push_dingtalk(f"📈 止盈激活\n\n{self.inst_id}\n盈利: {profit_pct*100:.2f}%\n峰值: {self.peak_price:.8g}\n止盈价: {self.stop_price:.8g}")
                self.logger.info(f"{self.inst_id} 止盈激活: 盈利 {profit_pct*100:.2f}%, 止盈价 {self.stop_price:.8g}", "profit_trigger")
                self.check_level = "T0"
                self.last_check_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._save_state()
            step = self.params.get("profit_step", 0.01)
            current_step = int(profit_pct / step)
            if current_step > 5 and current_step not in self._profit_notified_steps:
                self._profit_notified_steps.add(current_step)
                self._push_dingtalk(f"📈 盈利更新\n\n{self.inst_id}\n盈利: {profit_pct*100:.2f}%\n峰值: {self.peak_price:.8g}\n止盈价: {self.stop_price:.8g}")
                self.logger.info(f"{self.inst_id} 盈利更新: {profit_pct*100:.2f}%", "profit_trigger")
            new_stop = self.peak_price * (1 - self.params.get("stop_loss", 0.015))
            if new_stop > self.stop_price: 
                self.stop_price = new_stop
            if price <= self.stop_price:
                self.logger.info(f"{self.inst_id} 触发止盈清仓: 当前价 {price:.8g} ≤ 止盈价 {self.stop_price:.8g}", "stop_loss_trigger")
                return self._execute_sell(price, f"止盈清仓 (峰值 {self.peak_price:.8g}, 回撤1.5%)")
        self._update_check_level(profit_pct)
        self._save_state()
        return None

    def check_buy_queue(self, price):
        if self.has_position or self._is_in_cooldown(): 
            return None
        reason, pair_type, sell_line = self._determine_buy_level(price)
        if reason:
            return self._execute_buy(price, reason, pair_type, sell_line)
        return None

    def check_near_price(self, price):
        near = is_price_near_level(price, self.levels.pullback_down) or is_price_near_level(price, self.levels.callback_down) or is_price_near_level(price, self.levels.sub2)
        if near and not self.has_position and not self._is_in_cooldown():
            reason, pair_type, sell_line = self._determine_buy_level(price)
            if reason:
                return self._execute_buy(price, reason + "(近)", pair_type, sell_line)
        return None

    def update_price(self, price, ts):
        alerts = []
        if price == self._last_processed_price and ts == self._last_processed_ts:
            return alerts
        self._last_processed_price = price
        self._last_processed_ts = ts
        if price > self.high:
            self.high = price
            self.levels.update(self.high, self.low)
            self.last_new_high_ts = ts
            self.high_alert_sent.clear()
            save_high_low_entry(self.inst_id, self.high, self.low)
            alerts.append(f"创新高 {price:.8g}")
        if price < self.low:
            self.low = price
            self.levels.update(self.high, self.low)
            self.last_new_low_ts = ts
            self.low_alert_sent.clear()
            save_high_low_entry(self.inst_id, self.high, self.low)
            alerts.append(f"创新低 {price:.8g}")
        if self.has_position:
            profit_pct = (price - self.position_price) / self.position_price if self.position_price > 0 else 0
            self._update_check_level(profit_pct)
            if self.check_level == "T0":
                result = self._check_take_profit(price)
                if result:
                    alerts.append(f"止盈清仓 @ {price:.8g}")
                    self._save_state()
                    return alerts
        if self.has_position and price >= self.levels.pullback_up:
            result = self._execute_sell(price, "回撤上-清仓（通用规则）")
            if result:
                alerts.append(f"回撤上-清仓 @ {price:.8g}")
                self._save_state()
                return alerts
        if self.is_paired and not self.is_sold and self.has_position:
            if self.pair_sell_line == "回调上" and price >= self.levels.callback_up:
                result = self._execute_sell(price, "回调上-清仓")
                if result:
                    alerts.append(f"回调上-清仓 @ {price:.8g}")
                    self._save_state()
                    return alerts
            if self.pair_sell_line == "中间" and price >= self.levels.mid:
                result = self._execute_sell(price, "中间-清仓")
                if result:
                    alerts.append(f"中间-清仓 @ {price:.8g}")
                    self._save_state()
                    return alerts
        if not self.has_position and not self._is_in_cooldown():
            near = self.check_near_price(price)
            if near:
                alerts.append(f"近价买入 @ {price:.8g}")
                self._save_state()
                return alerts
        for name, threshold in (("回撤上", self.levels.pullback_up), ("回撤下", self.levels.pullback_down), ("回调上", self.levels.callback_up), ("回调下", self.levels.callback_down), ("次2", self.levels.sub2), ("中间", self.levels.mid)):
            if price < threshold:
                if name not in self.broken_levels:
                    self.broken_levels.add(name)
                    if self._can_send_break_alert(name, ts):
                        self._record_break_alert(name, ts)
                        alerts.append(f"价格 {price:.8g} 下破[{name}] ({threshold:.8g})")
            else:
                self.broken_levels.discard(name)
        self._save_state()
        return alerts

    def _can_send_break_alert(self, name, now):
        info = self.break_alerts[name]
        today = datetime.fromtimestamp(now).strftime("%Y-%m-%d")
        if info["date"] != today:
            info["date"] = today
            info["count"] = 0
        if info["count"] >= self.params.get("break_alert_max", 4):
            return False
        if now - info["last_ts"] < self.params.get("break_alert_interval", 1020):
            return False
        return True

    def _record_break_alert(self, name, now):
        info = self.break_alerts[name]
        info["count"] += 1
        info["last_ts"] = now

    def _push_dingtalk(self, text):
        if self.ding_webhook:
            push_to_dingtalk(self.ding_webhook, self.ding_secret, text)

    def _calc_unrealized_pnl(self) -> Tuple[float, float, float, float]:
        """基于最近成交价计算当前持仓的 (current_price, market_value, unrealized_pnl, unrealized_pnl_pct)"""
        cp = float(self._last_processed_price or 0.0)
        qty = float(self.position_qty or 0.0)
        cost = float(self.position_amount or 0.0)
        mv = cp * qty if cp > 0 and qty > 0 else 0.0
        pnl = mv - cost if cost > 0 else 0.0
        pnl_pct = (pnl / cost * 100.0) if cost > 0 else 0.0
        return cp, mv, pnl, pnl_pct

    def _save_state(self):
        positions = load_positions()
        if self.has_position:
            cp, mv, upnl, upnl_pct = self._calc_unrealized_pnl()
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

        # pair_status：保留完整历史买卖数据；已卖出时附带 profit/profit_pct
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
                # 计算实际盈亏：pair_status 里 profit 保留该笔交易的已实现盈亏
                buy_amt = float(self.pair_buy_price) * float(self.position_qty) if (float(self.pair_buy_price or 0) > 0) else 0.0
                # 如果 is_sold=True 且是刚卖出，则 position_qty 已清零；用 buy_price 重算成本不可靠，
                # 因此读取 pair_status 原 profit（若有）或留空让 save_pair_status 不覆盖原值；
                # 实际 profit 在 _execute_sell 后会被调用一次，此处简单地以 0 占位避免误覆盖，
                # 更精确的做法是 SELL 时调用 _save_state 之后我们会 save_trade；
                # 为了避免此处覆盖真实值：只有 is_paired=True 时才在 payload 中写 profit=0
                pass
            save_pair_status(self.inst_id, payload)
        else:
            # 既未配对也未卖出 -> 无需保留
            rows = _read_csv(PAIR_STATUS_FILE, PAIR_STATUS_HEADER)
            rows = [r for r in rows if r.get("inst_id") != self.inst_id]
            _write_csv_all(PAIR_STATUS_FILE, PAIR_STATUS_HEADER, rows)

        # 持仓状态变更后，刷新资金总览（汇总 positions 的市值/浮盈到 funds.csv）
        save_funds()


# ==================== 行情引擎 ====================
class MarketEngine:
    def __init__(self, logger, ding_webhook=None, ding_secret=None, system_params=None):
        self.logger = logger
        self.ding_webhook = ding_webhook
        self.ding_secret = ding_secret
        self.params = system_params or {}
        self.states = {}
        self.lock = threading.Lock()
        self._last_push_ts = 0.0
        self._last_t1_check = 0
        self._last_t2_check = 0
        self._last_t3_check = 0
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
            # 若恢复的持仓 current_price 有值，则同步 _last_processed_price 用于浮盈计算
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
            # 恢复 pair_type 历史（卖出后的记录 pair_type 仍保留原值）
            if data.get("pair_type"):
                state.pair_type = data["pair_type"]
        cooldown = load_cooldown()
        if inst_id in cooldown:
            state.cooldown[inst_id] = cooldown[inst_id]
        # 【修复2】恢复完成后，同步全局最新资金（避免构造期间其他实例更新后缓存过时）
        state.remaining_funds = get_remaining_funds()
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

    def run_timed_checks(self):
        now = time.time()
        now_min = int(now / 60)
        if now_min != self._last_t1_check:
            self._last_t1_check = now_min
        if now_min % 15 == 0 and now_min != self._last_t2_check:
            self._last_t2_check = now_min
        if now_min % 30 == 0 and now_min != self._last_t3_check:
            self._last_t3_check = now_min
        dt = datetime.now()
        if (dt.hour == 11 and dt.minute == 30) or (dt.hour == 23 and dt.minute == 30):
            if int(now) != self._last_buy_queue_check:
                self._last_buy_queue_check = int(now)
                self._check_buy_queue_all()

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
                    # 【修复B】业务层异常独立捕获，避免 None.lower() 等 CSV 问题被错报为"连接异常"并触发整连接重连
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
                try:
                    engine.run_timed_checks()
                except Exception as te:
                    engine.logger.error(f"OKX 定时检查异常(不重连): {te}")
        except Exception as e:
            engine.logger.error(f"OKX 连接异常: {e}", "connection_error")
            time.sleep(RECONNECT_DELAY)
        finally:
            if sock:
                sock.close()


# ==================== Binance 行情流 ====================
def subscribe_binance_trades(sock, symbols):
    payload = {"method": "SUBSCRIBE", "params": [f"{s.lower()}@trade" for s in symbols], "id": 1}
    send_text(sock, json.dumps(payload, separators=(",", ":")))


def handle_binance_message(message, engine, symbol_to_inst):
    try: 
        data = json.loads(message)
    except: 
        return
    if "id" in data and "result" in data: 
        return
    if data.get("e") == "error":
        engine.logger.error(f"Binance 订阅失败: {data}")
        return
    if data.get("e") == "trade":
        symbol = data.get("s", "")
        inst_id = symbol_to_inst.get(symbol, to_okx_inst_id(symbol))
        try: 
            price = float(data["p"])
        except: 
            return
        ts = int(data.get("T", time.time() * 1000)) / 1000
        engine.on_trade(inst_id, price, ts)


def stream_binance(engine, symbols, symbol_to_inst):
    while True:
        sock = None
        last_recv = time.time()
        try:
            sock = connect_binance_ws()
            engine.logger.info("Binance WebSocket 连接成功", "startup")
            subscribe_binance_trades(sock, symbols)
            while True:
                try:
                    opcode, payload = recv_ws_frame(sock)
                    last_recv = time.time()
                except socket.timeout:
                    if time.time() - last_recv >= PING_INTERVAL:
                        send_ping(sock)
                    continue
                if opcode == 0x1:
                    # 【修复B】Binance 线程同步做业务异常分层，不触发无谓重连
                    try:
                        handle_binance_message(payload.decode("utf-8"), engine, symbol_to_inst)
                    except Exception as be:
                        engine.logger.error(f"Binance 业务处理异常(不重连): {be}")
                elif opcode == 0x8:
                    raise ConnectionError("服务端关闭")
                elif opcode == 0x9:
                    send_pong(sock, payload)
                elif opcode == 0xA:
                    continue
                try:
                    engine.run_timed_checks()
                except Exception as te:
                    engine.logger.error(f"Binance 定时检查异常(不重连): {te}")
        except Exception as e:
            engine.logger.error(f"Binance 连接异常: {e}", "connection_error")
            time.sleep(RECONNECT_DELAY)
        finally:
            if sock:
                sock.close()


# ==================== 主函数 ====================
def main():
    global _global_remaining_funds
    
    print("=" * 60)
    print(f"{PROGRAM_NAME} {VERSION}")
    print(f"工作目录: {BASE_DIR}")
    print(f"数据目录: {DATA_DIR}")
    print("=" * 60)

    logger = Logger(LOG_FILE)

    # 加载资金
    initial_funds = load_funds()
    logger.info(f"加载资金: {initial_funds:.2f}", "startup")

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

    engine = MarketEngine(logger, ding_webhook, ding_secret, system_params)

    okx_inst_ids = []
    binance_symbols = []
    symbol_to_inst = {}

    logger.info("初始化币对状态...", "startup")
    for inst_id, data in config.items():
        engine.add_state(inst_id, data["ref_high"], data["ref_low"])
        state = engine.states[inst_id]
        print(f"\n{inst_id} 价格点位:")
        print(state.levels.describe())
        if check_okx_available(inst_id):
            okx_inst_ids.append(inst_id)
            print(f"{inst_id}: 使用 OKX")
        else:
            sym = to_binance_symbol(inst_id)
            binance_symbols.append(sym)
            symbol_to_inst[sym] = inst_id
            print(f"{inst_id}: OKX 无数据，改用 Binance ({sym})")

    positions = load_positions()
    for inst_id, data in positions.items():
        print(f"恢复持仓: {inst_id} 数量 {data.get('position_qty', 0):.8g} @ {data.get('position_price', 0):.8g}")

    queue = load_buy_queue()
    pending = [r for r in queue if r.get("status") == "待买入"]
    print(f"买入队列: {len(pending)} 个币对待买入")
    print(f"剩余资金: {get_remaining_funds():.2f}")

    threads = []
    if okx_inst_ids:
        t = threading.Thread(target=stream_okx, args=(engine, okx_inst_ids), daemon=True)
        t.start()
        threads.append(t)
        logger.info(f"OKX 行情线程启动: {len(okx_inst_ids)} 个币对", "startup")
    if binance_symbols:
        t = threading.Thread(target=stream_binance, args=(engine, binance_symbols, symbol_to_inst), daemon=True)
        t.start()
        threads.append(t)
        logger.info(f"Binance 行情线程启动: {len(binance_symbols)} 个币对", "startup")

    logger.info("=" * 60, "startup")
    logger.info("系统运行中，按 Ctrl+C 停止", "startup")
    logger.info(f"检查频率: T0(实时) T1(1分钟) T2(15分钟) T3(30分钟) 买入队列(每日11:30/23:30)", "startup")
    logger.info("=" * 60, "startup")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("用户中断，程序停止", "shutdown")
    finally:
        save_funds()
        logger.info("程序退出", "shutdown")


if __name__ == "__main__":
    main()