#!/usr/bin/env python3
"""
moonbot_final_ready.py
Final MoonBot Auto BUY-SELL (OKX spot) — Scalping / full features

Features:
 - Uses OKX REST primary orders (with timestamp taken from OKX public/time when possible)
 - Fallback to ccxt market orders
 - Polls order status, cancels if not filled in timeout
 - Confirmation of fills before considering position open
 - Take Profit and optional Stop Loss
 - Persistent last_buy.json to resume after restart
 - DRY_RUN mode
 - Safe retry logic, auto-stop on repeated insufficient funds
 - Detect STOP file (STOP.txt) to gracefully exit
"""

import os
import time
import json
import math
import hmac
import base64
import hashlib
import argparse
from datetime import datetime
from typing import Optional, Tuple

import requests
import ccxt
from dotenv import load_dotenv

# -----------------------
# CONFIG / ENV
# -----------------------
HERE = os.path.dirname(__file__) or "."
load_dotenv(os.path.join(HERE, "config.env"))

API_KEY = os.getenv("OKX_API_KEY")
SECRET_KEY = os.getenv("OKX_SECRET_KEY")
PASSPHRASE = os.getenv("OKX_PASSPHRASE")

BASE = "https://www.okx.com"
PAIR_INST = os.getenv("MOONBOT_PAIR_INST", "DOGE-USDT")  # OKX instId
PAIR_CCXT = os.getenv("MOONBOT_PAIR_CCXT", "DOGE/USDT")  # ccxt symbol

LOGFILE = os.path.join(HERE, "moonbot_final.log")
LAST_BUY_FILE = os.path.join(HERE, "last_buy.json")
STOP_FILE = os.path.join(HERE, "STOP.txt")

# Defaults (can override via env or CLI)
DEFAULT_BUY_USDT = float(os.getenv("MOONBOT_BUY_USDT", "5"))
DEFAULT_TP_MULT = float(os.getenv("MOONBOT_TP_MULT", "1.005"))   # +0.5% default scalping
DEFAULT_SL_MULT = float(os.getenv("MOONBOT_SL_MULT", "0.995"))   # -0.5% stop-loss (optional)
CHECK_DELAY = float(os.getenv("MOONBOT_CHECK_DELAY", "2"))
TS_RETRIES = int(os.getenv("MOONBOT_TS_RETRIES", "6"))
TS_SLEEP = float(os.getenv("MOONBOT_TS_SLEEP", "0.6"))
ORDER_POLL_INTERVAL = float(os.getenv("MOONBOT_ORDER_POLL_INTERVAL", "1"))
ORDER_FILL_TIMEOUT = int(os.getenv("MOONBOT_FILL_TIMEOUT", "12"))
INSUFFICIENT_LIMIT = int(os.getenv("MOONBOT_INSUFFICIENT_LIMIT", "3"))

# -----------------------
# CLI
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true", help="Do not execute real orders")
parser.add_argument("--buy-usdt", type=float, help="Override buy USDT")
parser.add_argument("--tp", type=float, help="Override take profit multiplier")
parser.add_argument("--sl", type=float, help="Override stop loss multiplier (optional)")
args = parser.parse_args()

DRY_RUN = bool(args.dry_run)
BUY_USDT = args.buy_usdt if args.buy_usdt else DEFAULT_BUY_USDT
TP_MULT = args.tp if args.tp else DEFAULT_TP_MULT
SL_MULT = args.sl if args.sl else DEFAULT_SL_MULT

# -----------------------
# Basic checks
# -----------------------
if not API_KEY or not SECRET_KEY or not PASSPHRASE:
    print("❌ Missing OKX credentials in config.env (OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE).")
    raise SystemExit(1)

# -----------------------
# Logging helper
# -----------------------
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOGFILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# -----------------------
# Timestamp & sign helpers
# -----------------------
def get_okx_server_ts() -> str:
    """Try fetch OKX public time (ms) and return seconds.float string; fallback to local time"""
    try:
        r = requests.get(BASE + "/api/v5/public/time", timeout=3)
        ts_ms = int(r.json()["data"][0]["ts"])
        # return seconds with millisecond precision
        return f"{ts_ms / 1000:.3f}"
    except Exception:
        return f"{time.time():.3f}"

def sign_okx(ts: str, method: str, path: str, body: str = "") -> str:
    prehash = f"{ts}{method}{path}{body}"
    mac = hmac.new(SECRET_KEY.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def request_signed(method: str, path: str, body: Optional[dict] = None, use_okx_time: bool = True, retries: int = TS_RETRIES) -> dict:
    url = BASE + path
    payload = json.dumps(body) if body else ""
    attempt = 0
    while attempt < retries:
        ts = get_okx_server_ts() if use_okx_time else f"{time.time():.3f}"
        headers = {
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": API_KEY,
            "OK-ACCESS-PASSPHRASE": PASSPHRASE,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-SIGN": sign_okx(ts, method, path, payload),
        }
        try:
            r = requests.request(method, url, headers=headers, data=payload, timeout=10)
        except Exception as e:
            log(f"Network error calling {path}: {e} (retry {attempt+1}/{retries})")
            attempt += 1
            time.sleep(TS_SLEEP)
            continue

        try:
            js = r.json()
        except Exception:
            log(f"Non-JSON response from OKX {path}: {r.text}")
            return {"error": "nonjson", "raw": r.text}

        # handle timestamp errors
        code = js.get("code", "")
        msg = js.get("msg", "") or ""
        if code in ("50112", "50102") or "Timestamp request expired" in msg or "Invalid OK-ACCESS-TIMESTAMP" in msg:
            log(f"TS error from OKX (code={code}): {msg} — retry {attempt+1}/{retries}")
            attempt += 1
            time.sleep(TS_SLEEP)
            continue

        return js
    return {"error": "timestamp_failed", "msg": "timestamp retries exhausted"}

# -----------------------
# CCXT helper
# -----------------------
def create_ccxt_okx():
    ex = ccxt.okx({
        "apiKey": API_KEY,
        "secret": SECRET_KEY,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    })
    return ex

# -----------------------
# Account & market helpers
# -----------------------
def get_price() -> float:
    r = requests.get(f"{BASE}/api/v5/market/ticker?instId={PAIR_INST}", timeout=6)
    return float(r.json()["data"][0]["last"])

def get_usdt_balance() -> float:
    js = request_signed("GET", "/api/v5/account/balance?ccy=USDT")
    if isinstance(js, dict) and js.get("code") == "0":
        try:
            for entry in js.get("data", []):
                for d in entry.get("details", []):
                    if d.get("ccy") == "USDT":
                        return float(d.get("availBal") or d.get("cashBal") or 0)
        except Exception:
            pass
    js2 = request_signed("GET", "/api/v5/asset/balances")
    if isinstance(js2, dict) and js2.get("code") == "0":
        for it in js2.get("data", []):
            if it.get("ccy") == "USDT":
                return float(it.get("availBal") or 0)
    return 0.0

def get_doge_balance() -> float:
    js = request_signed("GET", "/api/v5/asset/balances")
    if isinstance(js, dict) and js.get("code") == "0":
        for it in js.get("data", []):
            if it.get("ccy") == "DOGE":
                return float(it.get("availBal") or 0)
    return 0.0

# -----------------------
# Order helpers
# -----------------------
def okx_buy_by_cost(cost_usdt: float) -> dict:
    body = {
        "instId": PAIR_INST,
        "tdMode": "cash",
        "side": "buy",
        "ordType": "market",
        "sz": str(cost_usdt),
        "tgtCcy": "quote"
    }
    return request_signed("POST", "/api/v5/trade/order", body)

def okx_sell_by_amount(amount_base: float) -> dict:
    body = {
        "instId": PAIR_INST,
        "tdMode": "cash",
        "side": "sell",
        "ordType": "market",
        "sz": str(amount_base)
    }
    return request_signed("POST", "/api/v5/trade/order", body)

def okx_get_order_status(ordId: Optional[str] = None, clOrdId: Optional[str] = None, instId: Optional[str] = None) -> dict:
    path = "/api/v5/trade/order?"
    params = []
    if instId:
        params.append(f"instId={instId}")
    if ordId:
        params.append(f"ordId={ordId}")
    if clOrdId:
        params.append(f"clOrdId={clOrdId}")
    if not params:
        raise ValueError("ordId or clOrdId or instId required")
    path += "&".join(params)
    return request_signed("GET", path)

def okx_cancel_order(ordId: Optional[str] = None, clOrdId: Optional[str] = None, instId: Optional[str] = None) -> dict:
    body = {"instId": instId or PAIR_INST}
    if ordId:
        body["ordId"] = ordId
    if clOrdId:
        body["clOrdId"] = clOrdId
    return request_signed("POST", "/api/v5/trade/cancel-order", body)

# -----------------------
# Polling / persistence
# -----------------------
def poll_order_until_filled(ordId: Optional[str], clOrdId: Optional[str], timeout_sec: int = ORDER_FILL_TIMEOUT) -> Tuple[float, float, dict]:
    start = time.time()
    last_resp = {}
    while True:
        try:
            resp = okx_get_order_status(ordId=ordId, clOrdId=clOrdId, instId=PAIR_INST)
            last_resp = resp
            if isinstance(resp, dict) and resp.get("code") == "0":
                data = resp.get("data", [{}])[0]
                filled = float(data.get("fillSz") or data.get("accFillSz") or 0)
                avg = float(data.get("avgPx") or 0)
                state = (data.get("state") or "").lower()
                if filled > 0:
                    return filled, avg or 0.0, resp
                if state in ("filled", "canceled", "cancelled"):
                    return filled, avg or 0.0, resp
        except Exception as e:
            log(f"Error polling order: {e}")
        if time.time() - start > timeout_sec:
            return 0.0, 0.0, last_resp
        time.sleep(ORDER_POLL_INTERVAL)

def save_last_buy(price: float, amount: float):
    try:
        with open(LAST_BUY_FILE, "w", encoding="utf-8") as f:
            json.dump({"price": price, "amount": amount, "ts": time.time()}, f)
    except Exception:
        pass

def load_last_buy():
    try:
        with open(LAST_BUY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

# -----------------------
# Fallback ccxt methods
# -----------------------
def ccxt_market_buy_by_amount(amount_base: float):
    ex = create_ccxt_okx()
    ex.load_markets()
    m = ex.market(PAIR_CCXT)
    min_amt = m.get("limits", {}).get("amount", {}).get("min") or 0
    precision = m.get("precision", {}).get("amount", 6)
    if amount_base < min_amt:
        amount_base = min_amt
    factor = 10 ** (precision if isinstance(precision, int) else 6)
    amt_q = math.floor(amount_base * factor) / factor
    if amt_q <= 0:
        return {"error": "bad_amount"}
    if DRY_RUN:
        log(f"[DRY RUN] ccxt buy {amt_q} {PAIR_CCXT}")
        return {"dummy": True, "amount": amt_q}
    try:
        order = ex.create_market_buy_order(PAIR_CCXT, amt_q)
        return {"ccxt_order": order}
    except Exception as e:
        return {"error": "ccxt_err", "msg": str(e)}

def ccxt_market_sell_by_amount(amount_base: float):
    ex = create_ccxt_okx()
    ex.load_markets()
    precision = ex.market(PAIR_CCXT).get("precision", {}).get("amount", 6)
    factor = 10 ** (precision if isinstance(precision, int) else 6)
    amt_q = math.floor(amount_base * factor) / factor
    if amt_q <= 0:
        return {"error": "bad_amount"}
    if DRY_RUN:
        log(f"[DRY RUN] ccxt sell {amt_q} {PAIR_CCXT}")
        return {"dummy": True, "amount": amt_q}
    try:
        order = ex.create_market_sell_order(PAIR_CCXT, amt_q)
        return {"ccxt_order": order}
    except Exception as e:
        return {"error": "ccxt_err", "msg": str(e)}

# -----------------------
# Main loop
# -----------------------
def main_loop():
    log("MoonBot FINAL starting...")
    insufficient_counter = 0

    # attempt to load market minimum via ccxt
    try:
        ex = create_ccxt_okx()
        ex.load_markets()
        market = ex.market(PAIR_CCXT)
        min_amount = float(market.get("limits", {}).get("amount", {}).get("min") or 10.0)
    except Exception:
        min_amount = 10.0

    log(f"Market min amount (base): {min_amount}")

    while True:
        # STOP file check
        if os.path.exists(STOP_FILE):
            log("STOP.txt detected — exiting gracefully.")
            break

        try:
            price = get_price()
            usdt_bal = get_usdt_balance()
            doge_bal = get_doge_balance()
        except Exception as e:
            log(f"Market/account read error: {e}")
            time.sleep(3)
            continue

        log(f"Balances: USDT={usdt_bal:.6f}, DOGE={doge_bal:.6f} | Price={price:.6f}")

        # If holding DOGE -> monitor and sell at TP or SL
        if doge_bal >= min_amount:
            lb = load_last_buy()
            buy_price = lb.get("price") if lb else price
            target = buy_price * TP_MULT
            stoploss = buy_price * SL_MULT if SL_MULT and SL_MULT < 1.0 else None
            log(f"Holding {doge_bal:.6f} DOGE | buy_price={buy_price:.6f} target={target:.6f} stoploss={stoploss:.6f}" if stoploss else f"Holding {doge_bal:.6f} DOGE | buy_price={buy_price:.6f} target={target:.6f}")

            while True:
                if os.path.exists(STOP_FILE):
                    log("STOP detected — stop monitoring and exit.")
                    return
                try:
                    pnow = get_price()
                except Exception as e:
                    log(f"Price fetch error: {e}")
                    time.sleep(CHECK_DELAY)
                    continue

                log(f"Monitor price={pnow:.6f} target={target:.6f}")
                if pnow >= target:
                    log("Target reached -> SELL market")
                    if DRY_RUN:
                        log("[DRY RUN] Would sell via OKX REST")
                        try:
                            os.remove(LAST_BUY_FILE)
                        except Exception:
                            pass
                        break
                    resp = okx_sell_by_amount(doge_bal)
                    log(f"OKX sell response: {json.dumps(resp)}")
                    if isinstance(resp, dict) and resp.get("code") == "0":
                        data = resp.get("data", [{}])[0]
                        ordId = data.get("ordId")
                        clOrdId = data.get("clOrdId")
                        filled, avg, last = poll_order_until_filled(ordId=ordId, clOrdId=clOrdId, timeout_sec=ORDER_FILL_TIMEOUT)
                        if filled > 0:
                            log(f"✅ SOLD {filled:.6f} DOGE @ {avg:.6f}")
                            try: os.remove(LAST_BUY_FILE)
                            except Exception: pass
                            break
                        else:
                            log("REST sell not filled -> fallback to ccxt sell")
                            res2 = ccxt_market_sell_by_amount(doge_bal)
                            log(f"ccxt sell fallback: {res2}")
                            try: os.remove(LAST_BUY_FILE)
                            except Exception: pass
                            break
                    else:
                        log("REST sell error -> fallback to ccxt sell")
                        res2 = ccxt_market_sell_by_amount(doge_bal)
                        log(f"ccxt sell fallback: {res2}")
                        try: os.remove(LAST_BUY_FILE)
                        except Exception: pass
                        break

                # optional stop-loss
                if stoploss and pnow <= stoploss:
                    log("Stop-loss triggered -> SELL market")
                    if DRY_RUN:
                        log("[DRY RUN] Would sell (stop-loss)")
                        try: os.remove(LAST_BUY_FILE)
                        except Exception: pass
                        break
                    resp = okx_sell_by_amount(doge_bal)
                    log(f"OKX sell response (SL): {json.dumps(resp)}")
                    if isinstance(resp, dict) and resp.get("code") == "0":
                        data = resp.get("data", [{}])[0]
                        ordId = data.get("ordId")
                        clOrdId = data.get("clOrdId")
                        filled, avg, last = poll_order_until_filled(ordId=ordId, clOrdId=clOrdId, timeout_sec=ORDER_FILL_TIMEOUT)
                        if filled > 0:
                            log(f"✅ STOP-LOSS SOLD {filled:.6f} DOGE @ {avg:.6f}")
                            try: os.remove(LAST_BUY_FILE)
                            except Exception: pass
                            break
                        else:
                            res2 = ccxt_market_sell_by_amount(doge_bal)
                            log(f"ccxt sell fallback (SL): {res2}")
                            try: os.remove(LAST_BUY_FILE)
                            except Exception: pass
                            break
                    else:
                        res2 = ccxt_market_sell_by_amount(doge_bal)
                        log(f"ccxt sell fallback (SL): {res2}")
                        try: os.remove(LAST_BUY_FILE)
                        except Exception: pass
                        break

                time.sleep(CHECK_DELAY)
            time.sleep(2)
            continue

        # If not holding DOGE -> attempt buy
        if usdt_bal < 1.0:
            insufficient_counter += 1
            log(f"Insufficient USDT ({usdt_bal:.6f}), attempt {insufficient_counter}/{INSUFFICIENT_LIMIT}")
            if insufficient_counter >= INSUFFICIENT_LIMIT:
                log("❌ USDT insufficient too many times — bot stopping.")
                break
            time.sleep(10)
            continue
        else:
            insufficient_counter = 0

        est_amount = BUY_USDT / price
        amount_to_buy = max(min_amount, est_amount)
        # quantize amount to market precision
        try:
            ex = create_ccxt_okx()
            ex.load_markets()
            precision = ex.market(PAIR_CCXT).get("precision", {}).get("amount", 6)
        except Exception:
            precision = 6
        factor = 10 ** (precision if isinstance(precision, int) else 6)
        amount_to_buy = math.floor(amount_to_buy * factor) / factor

        log(f"Attempt BUY: cost={BUY_USDT} USDT -> amount={amount_to_buy:.6f} DOGE (est price {price:.6f})")

        if DRY_RUN:
            log("[DRY RUN] Would send OKX buy-by-cost (tgtCcy=quote)")
            save_last_buy(price, amount_to_buy)
            time.sleep(2)
            continue

        # Try REST buy-by-cost (preferred)
        resp = okx_buy_by_cost(BUY_USDT)
        log(f"OKX buy response: {json.dumps(resp)}")

        filled = 0.0
        avg_px = 0.0

        if isinstance(resp, dict) and resp.get("code") == "0":
            data = resp.get("data", [{}])[0]
            ordId = data.get("ordId")
            clOrdId = data.get("clOrdId")
            filled, avg_px, last_resp = poll_order_until_filled(ordId=ordId, clOrdId=clOrdId, timeout_sec=ORDER_FILL_TIMEOUT)
            if filled > 0:
                log(f"✅ BUY success (REST) filled {filled:.6f} DOGE @ {avg_px:.6f}")
                save_last_buy(avg_px or price, filled)
                time.sleep(2)
                continue
            else:
                log("REST buy-by-cost not filled in time -> cancel & fallback")
                try:
                    okx_cancel_order(ordId=ordId, clOrdId=clOrdId, instId=PAIR_INST)
                    log("Cancel requested")
                except Exception as e:
                    log(f"Cancel error: {e}")
        else:
            log("REST buy failed or rejected -> fallback to ccxt buy by amount")

        # Fallback ccxt
        res = ccxt_market_buy_by_amount(amount_to_buy)
        log(f"ccxt fallback result: {res}")

        if isinstance(res, dict) and res.get("ccxt_order"):
            order = res["ccxt_order"]
            # ccxt may or may not include filled info; try poll via ordId/clOrdId if present
            filled = float(order.get("filled") or order.get("amount") or 0 or 0)
            avg_px = float(order.get("average") or order.get("price") or price)
            if filled > 0:
                log(f"✅ BUY success (ccxt) filled {filled:.6f} DOGE @ {avg_px:.6f}")
                save_last_buy(avg_px, filled)
            else:
                info = order.get("info", {})
                ordId = info.get("ordId")
                clOrdId = info.get("clientOrderId") or info.get("clOrdId")
                if ordId or clOrdId:
                    log(f"Polling ccxt-created order ordId={ordId} clOrdId={clOrdId}")
                    f2, a2, last2 = poll_order_until_filled(ordId=ordId, clOrdId=clOrdId, timeout_sec=ORDER_FILL_TIMEOUT)
                    if f2 > 0:
                        log(f"✅ BUY filled after poll: {f2:.6f} DOGE @ {a2:.6f}")
                        save_last_buy(a2 or price, f2)
                    else:
                        log("ccxt order not filled in timeout. Giving up this cycle.")
                else:
                    log("ccxt returned no ordId/clOrdId and no fill info -> cannot confirm. Giving up this cycle.")
        else:
            log("Fallback buy failed. Waiting before next attempt.")
        time.sleep(3)

    log("Bot stopped. Goodbye.")

# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        log("Interrupted by user. Exiting.")
