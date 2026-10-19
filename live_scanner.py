import sqlite3, threading, time, json, os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests, websocket
from strategy_1 import analyze, analyze_preclose, rsi14

REST = "https://api.binance.com"
WS = "wss://stream.binance.com:9443/ws"
TP_PCT = 1.0
SL_PCT = 3.0


class LiveScanner:
    def __init__(self, db):
        self.dbpath = db
        self.enabled = False  # shared Binance feed on/off
        self.method1_enabled = False  # Method 1 strategy on/off independently
        self.running = False
        self.ws = None
        self.symbols = []
        self.candles = {}
        self.lock = threading.RLock()
        self.updated = None
        self.msg = "Ready — click Start Scanner"
        self.watch = {}
        self.confirmed = {}
        self.identified = {}  # qualifying setups remain until user records a manual result
        self.active = {}
        self.invalid = {}
        self.ws_subscribed_1m = set()
        self._bootstrap_started = False

    def db(self):
        c = sqlite3.connect(self.dbpath)
        c.row_factory = sqlite3.Row
        return c

    def start(self):
        # Start only the controller; no Binance loading before the user presses Start.
        self.running = True

    def enable_feed(self):
        """Start shared Binance market data without implicitly enabling Method 1."""
        if self.enabled:
            return
        self.enabled = True
        self.msg = "Starting shared Binance feed…"
        if not self._bootstrap_started:
            self._bootstrap_started = True
            threading.Thread(target=self._bootstrap, daemon=True).start()

    def enable(self):
        self.method1_enabled = True
        self.enable_feed()
        self.msg = "Method 1 running — shared Binance feed active"

    def disable_method1(self):
        self.method1_enabled = False
        self.watch.clear()
        self.confirmed.clear()
        self.invalid.clear()
        self.msg = "ICT scanner stopped; shared feed can continue for Hidden Bullish scanner"

    def disable_feed(self):
        self.enabled = False
        ws = self.ws
        self.ws = None
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        self.msg = "Shared Binance feed stopped"

    def disable(self):
        # Backwards-compatible full stop; keep identified setups for manual results.
        self.disable_method1()
        self.disable_feed()

    def status(self):
        with self.lock:
            return {
                "running": self.method1_enabled,
                "feed_running": self.enabled,
                "connected": self.ws is not None,
                "symbols": len(self.symbols),
                "loaded": len(self.candles),
                "message": self.msg,
                "updated": self.updated,
                "active_trades": len(self.active),
                "invalid": len(self.invalid),
            }

    def _symbols(self):
        d = requests.get(REST + "/api/v3/exchangeInfo", timeout=30).json()
        return [
            x["symbol"] for x in d["symbols"]
            if x["status"] == "TRADING"
            and x["quoteAsset"] == "USDT"
            and x.get("isSpotTradingAllowed", False)
        ]

    def _history(self, s):
        # Retry transient Binance rate-limit / network errors instead of silently losing pairs.
        last_error = None
        for attempt in range(4):
            try:
                response = requests.get(REST + "/api/v3/klines", params={"symbol": s, "interval": "1h", "limit": 230}, timeout=30)
                if response.status_code in (418, 429, 500, 502, 503, 504):
                    time.sleep(min(2 ** attempt, 12))
                    continue
                response.raise_for_status()
                d = response.json()
                if not isinstance(d, list):
                    raise RuntimeError(str(d)[:180])
                now = int(time.time() * 1000)
                a = []
                for x in d:
                    a.append({
                        "open_time": int(x[0]), "open": float(x[1]), "high": float(x[2]),
                        "low": float(x[3]), "close": float(x[4]), "close_time": int(x[6]),
                        "closed": int(x[6]) < now
                    })
                return s, a
            except Exception as exc:
                last_error = exc
                time.sleep(min(1 + attempt * 2, 7))
        raise RuntimeError(f"History failed for {s}: {last_error}")

    def _bootstrap(self):
        try:
            syms = self._symbols()
            with self.lock:
                self.symbols = syms
                self.msg = f"Loading 1H history: 0/{len(syms)}"
                self.updated = datetime.now(timezone.utc).isoformat()
            loaded = failed = 0
            with ThreadPoolExecutor(max_workers=8) as pool:
                fs = [pool.submit(self._history, s) for s in syms]
                for f in as_completed(fs):
                    try:
                        s, a = f.result()
                        self.candles[s] = a
                        loaded += 1
                    except Exception:
                        failed += 1
                    if (loaded + failed) % 25 == 0 or loaded + failed == len(syms):
                        with self.lock:
                            self.msg = f"Loading 1H history: {loaded + failed}/{len(syms)} (loaded {loaded})"
                            self.updated = datetime.now(timezone.utc).isoformat()
            with self.lock:
                self.msg = f"History ready: {loaded}/{len(syms)} pairs. Connecting live feed…"
                self.updated = datetime.now(timezone.utc).isoformat()
            threading.Thread(target=self._ws_loop, daemon=True).start()
            threading.Thread(target=self._paper_poll_loop, daemon=True).start()
        except Exception as e:
            with self.lock:
                self.msg = f"Bootstrap error: {e}"
                self.updated = datetime.now(timezone.utc).isoformat()

    def _ws_loop(self):
        while self.running:
            if not self.enabled:
                time.sleep(1)
                continue
            try:
                ws = websocket.create_connection(WS, timeout=35, ping_interval=20, ping_timeout=10)
                self.ws = ws
                params = [s.lower() + "@kline_1h" for s in self.symbols]
                for st in range(0, len(params), 200):
                    ws.send(json.dumps({"method": "SUBSCRIBE", "params": params[st:st+200], "id": st // 200 + 1}))
                with self.lock:
                    self.msg = f"LIVE — monitoring {len(params)} Spot USDT pairs"
                    self.updated = datetime.now(timezone.utc).isoformat()
                while self.running:
                    raw = ws.recv()
                    if not raw:
                        break
                    self._on(raw)
            except Exception as e:
                with self.lock:
                    self.msg = f"WebSocket reconnecting: {e}"
                    self.updated = datetime.now(timezone.utc).isoformat()
                self.ws = None
                time.sleep(4)
        self.ws = None

    def _subscribe_1m(self, s):
        if not self.ws or s in self.ws_subscribed_1m:
            return
        try:
            self.ws.send(json.dumps({"method": "SUBSCRIBE", "params": [s.lower() + "@kline_1m"], "id": 100000 + len(self.ws_subscribed_1m)}))
            self.ws_subscribed_1m.add(s)
        except Exception:
            pass

    def _on(self, raw):
        try:
            d = json.loads(raw)
        except Exception:
            return
        k = d.get("k")
        if not k:
            return
        s = k["s"]
        interval = k.get("i")
        row = {
            "open_time": int(k["t"]), "open": float(k["o"]), "high": float(k["h"]),
            "low": float(k["l"]), "close": float(k["c"]), "close_time": int(k["T"]),
            "closed": bool(k.get("x"))
        }
        if interval == "1m":
            if k.get("x"):
                self._handle_1m_close(s, row)
            return
        if interval != "1h":
            return

        arr = self.candles.setdefault(s, [])
        if arr and arr[-1]["open_time"] == row["open_time"]:
            arr[-1] = row
        else:
            arr.append(row)
        self.candles[s] = arr[-230:]

        if not self.method1_enabled:
            return

        if not k.get("x"):
            left = max(0, int((int(k["T"]) - int(time.time() * 1000)) / 1000))
            if 0 < left <= 900 and len(arr) >= 30:
                pre = analyze_preclose(arr[:-1], row)
                pre["rsi"] = rsi14(arr[:-1])
                if pre.get("valid"):
                    pre.update(symbol=s, seconds_remaining=left, status="WATCH")
                    self.watch[s] = pre
                else:
                    # If this coin was previously a potential setup in the 5-minute
                    # watch and then fails a condition, keep a human-readable invalid
                    # record for 45 minutes instead of silently disappearing.
                    if s in self.watch:
                        old = self.watch.get(s, {})
                        self._add_invalid(s, old, reason=pre.get("reason", "Setup condition failed"))
                    self.watch.pop(s, None)
            return

        # Closed 1H candle: keep only qualifying setups in the identified table.
        self.watch.pop(s, None)
        if len(arr) < 30:
            return
        r = analyze(arr)
        r["rsi"] = rsi14(arr)
        if not r.get("valid"):
            return
        now = int(time.time() * 1000)
        r.update(symbol=s, status="IDENTIFIED", identified_at=now)
        self.identified[s] = r
        self._save_event(s, r, None, "IDENTIFIED", None)

    def _resolve_first_1m(self, s, r):
        target = int(r["signal_time"]) + 1
        for _ in range(8):
            try:
                d = requests.get(REST + "/api/v3/klines", params={"symbol": s, "interval": "1m", "startTime": target, "limit": 1}, timeout=10).json()
                if d:
                    x = d[0]
                    row = {"open_time": int(x[0]), "open": float(x[1]), "high": float(x[2]), "low": float(x[3]), "close": float(x[4]), "close_time": int(x[6]), "closed": int(x[6]) < int(time.time() * 1000)}
                    if row["closed"]:
                        self._handle_1m_close(s, row)
                        return
            except Exception:
                pass
            time.sleep(2)

    def _handle_1m_close(self, s, row):
        # Setups are for manual trading only; no automatic entry or trade execution.
        if s in self.active:
            self._update_trade(s, row)

    def _start_trade(self, s, r, entry):
        tp = entry * (1 + TP_PCT / 100)
        sl = entry * (1 - SL_PCT / 100)
        trade = {
            "symbol": s,
            "rsi": r.get("rsi"),
            "pump1": r.get("pump1"), "pump2": r.get("pump2"), "one_min_pump": r.get("one_min_pump"),
            "setup_time": r.get("signal_time"), "entry_time": r.get("one_min_close_time") or int(time.time() * 1000),
            "entry_price": entry, "tp_price": tp, "sl_price": sl, "max_pump": 0.0, "status": "RUNNING"
        }
        self.active[s] = trade
        self.confirmed.pop(s, None)
        c = self.db()
        c.execute("INSERT OR REPLACE INTO active_trades(symbol,trend,rsi,pump1,pump2,one_min_pump,setup_time,entry_time,entry_price,tp_price,sl_price,max_pump,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  (s, None, trade["rsi"], trade["pump1"], trade["pump2"], trade["one_min_pump"], trade["setup_time"], trade["entry_time"], trade["entry_price"], trade["tp_price"], trade["sl_price"], trade["max_pump"], trade["status"]))
        c.commit(); c.close()

    def _update_trade(self, s, row):
        t = self.active.get(s)
        if not t:
            return
        p = (row["high"] - t["entry_price"]) / t["entry_price"] * 100
        tp_hit = row["high"] >= t["tp_price"]
        sl_hit = row["low"] <= t["sl_price"]
        if tp_hit and sl_hit:
            # OHLC cannot prove which level was reached first inside this 1M candle.
            return
        if tp_hit:
            t["max_pump"] = 1.0
            result, exit_price = "TP HIT", t["tp_price"]
        elif sl_hit:
            t["max_pump"] = min(1.0, max(t.get("max_pump", 0.0), p))
            result, exit_price = "SL HIT", t["sl_price"]
        else:
            t["max_pump"] = min(1.0, max(t.get("max_pump", 0.0), p))
            return
        t["status"] = result; t["close_time"] = row["close_time"]; t["exit_price"] = exit_price
        c = self.db()
        c.execute("INSERT INTO reports(symbol,trend,rsi,pump1,pump2,one_min_pump,entry_time,close_time,entry_price,exit_price,max_pump,result) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                  (s, None, t.get("rsi"), t["pump1"], t["pump2"], t["one_min_pump"], t["entry_time"], t["close_time"], t["entry_price"], t["exit_price"], t["max_pump"], result))
        c.execute("DELETE FROM active_trades WHERE symbol=?", (s,)); c.commit(); c.close()
        self.active.pop(s, None)

    def _paper_poll_loop(self):
        while self.running:
            if self.enabled and self.active:
                for s in list(self.active):
                    try:
                        d = requests.get(REST + "/api/v3/klines", params={"symbol": s, "interval": "1m", "limit": 2}, timeout=8).json()
                        for x in d:
                            row = {"open_time": int(x[0]), "open": float(x[1]), "high": float(x[2]), "low": float(x[3]), "close": float(x[4]), "close_time": int(x[6]), "closed": int(x[6]) < int(time.time() * 1000)}
                            if row["closed"]:
                                self._update_trade(s, row)
                    except Exception:
                        pass
            time.sleep(3)

    def _add_invalid(self, s, r, trend=None, reason=None):
        # Invalid table is reserved only for genuine candidates where BOTH
        # green sections achieved strictly more than 1% pumping.
        try:
            p1 = float(r.get("pump1"))
            p2 = float(r.get("pump2"))
        except (TypeError, ValueError):
            return
        if not (p1 > 1.0 and p2 > 1.0):
            return
        now = int(time.time() * 1000)
        self.invalid[s] = {
            "symbol": s,
            "pump1": r.get("pump1"),
            "pump2": r.get("pump2"),
            "rsi": r.get("rsi"),
            "invalid_time": now,
            "expires_at": now + 45 * 60 * 1000,
            "reason": reason or r.get("reason") or "Setup condition failed",
        }

    def invalid_list(self, limit=200):
        now = int(time.time() * 1000)
        # Purge records after exactly 45 minutes.
        for s in list(self.invalid):
            if self.invalid[s].get("expires_at", 0) <= now:
                self.invalid.pop(s, None)
        out = []
        for s, r in self.invalid.items():
            x = dict(r)
            x["seconds_remaining"] = max(0, int((x["expires_at"] - now) / 1000))
            out.append(x)
        out.sort(key=lambda x: x.get("invalid_time", 0), reverse=True)
        return out[:limit]

    def _save_event(self, s, r, trend, status, p1m):
        c = self.db()
        c.execute("INSERT INTO setup_events(symbol,trend,rsi,pump1,pump2,status,one_min_pump,event_time,reason) VALUES(?,?,?,?,?,?,?,?,?)",
                  (s, trend, r.get("rsi"), r.get("pump1"), r.get("pump2"), status, p1m, int(time.time() * 1000), r.get("reason")))
        c.commit(); c.close()

    def watch_list(self, limit=200):
        now = int(time.time() * 1000); out = []
        for s, r in list(self.watch.items()):
            x = dict(r); x["symbol"] = s; x["seconds_remaining"] = max(0, int((x["signal_time"] - now) / 1000)); out.append(x)
        out.sort(key=lambda x: (x["seconds_remaining"], x["symbol"]))
        return out[:limit]

    def identified_list(self, limit=500):
        out = []
        for symbol, r in list(self.identified.items()):
            out.append(dict(r))
        out.sort(key=lambda x: x.get("identified_at", 0), reverse=True)
        return out[:limit]

    def mark_manual_result(self, symbol, result, close_time=None):
        result = str(result or "").upper().strip()
        if result not in ("TP HIT", "SL HIT"):
            return {"ok": False, "error": "Result must be TP HIT or SL HIT"}, 400
        setup = self.identified.get(symbol)
        if not setup:
            return {"ok": False, "error": "Setup not found"}, 404
        try:
            close_time = int(close_time)
            if close_time <= 0:
                raise ValueError
        except (TypeError, ValueError):
            return {"ok": False, "error": "Please enter the trade close date and time"}, 400
        c = self.db()
        c.execute("INSERT INTO reports(symbol,trend,rsi,pump1,pump2,one_min_pump,entry_time,close_time,entry_price,exit_price,max_pump,result) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                  (symbol, setup.get("trend") or setup.get("method") or "Method 1", setup.get("rsi"), setup.get("pump1"), setup.get("pump2"), None,
                   setup.get("identified_at"), close_time, setup.get("entry_price"), None, None, result))
        c.commit(); c.close()
        self._save_event(symbol, setup, None, result, None)
        self.identified.pop(symbol, None)
        return {"ok": True, "symbol": symbol, "result": result}

    def skip_identified(self, symbol):
        setup = self.identified.get(symbol)
        if not setup:
            return {"ok": False, "error": "Setup not found or already removed"}, 404
        skipped = dict(setup)
        skipped["reason"] = "User chose Skip Trade"
        self._save_event(symbol, skipped, None, "SKIPPED", None)
        self.identified.pop(symbol, None)
        return {"ok": True, "symbol": symbol, "status": "SKIPPED"}, 200

    def confirmed_list(self, limit=200):
        out = []
        for s, r in list(self.confirmed.items()):
            x = dict(r); x["symbol"] = s; out.append(x)
        out.sort(key=lambda x: x.get("confirmed_at", 0), reverse=True)
        return out[:limit]

    def active_list(self):
        return [dict(v) for v in sorted(self.active.values(), key=lambda x: x["entry_time"], reverse=True)]

    def reports(self, limit=500):
        c = self.db()
        try:
            return [dict(x) for x in c.execute("SELECT * FROM reports ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
        finally:
            c.close()

    def events(self, limit=200):
        c = self.db()
        try:
            return [dict(x) for x in c.execute("SELECT * FROM setup_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
        finally:
            c.close()

    def test_symbol(self, s):
        try:
            _, a = self._history(s)
            closed = [x for x in a if x.get("closed")]
            r = analyze(closed); r["rsi"] = rsi14(closed); r["symbol"] = s
            return r
        except Exception as e:
            return {"valid": False, "reason": str(e)}
