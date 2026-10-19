"""Hidden Bullish Divergence scanner using the shared Binance 1H candle cache."""
import threading, time, sqlite3
from datetime import datetime, timezone


def rsi_series(closes, period=14):
    out=[None]*len(closes)
    if len(closes) <= period: return out
    gains=[]; losses=[]
    for i in range(1, period+1):
        d=closes[i]-closes[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains)/period; al=sum(losses)/period
    out[period]=100 if al==0 else 100-100/(1+ag/al)
    for i in range(period+1,len(closes)):
        d=closes[i]-closes[i-1]; ag=(ag*(period-1)+max(d,0))/period; al=(al*(period-1)+max(-d,0))/period
        out[i]=100 if al==0 else 100-100/(1+ag/al)
    return out

class HiddenBullishScanner:
    def __init__(self, shared):
        self.shared=shared; self.enabled=False; self.running=False
        self.message='Ready — start Hidden Bullish scanner'; self.updated=None
        self.watch={}; self.identified={}; self.lock=threading.RLock(); self._thread=None
        self.dbpath=shared.dbpath

    def start(self):
        if self._thread and self._thread.is_alive(): return
        self.running=True; self._thread=threading.Thread(target=self._loop,daemon=True); self._thread.start()
    def enable(self):
        self.shared.enable_feed(); self.enabled=True; self.message='Scanning shared Binance Spot 1H candle cache'
    def disable(self):
        self.enabled=False; self.watch.clear(); self.message='Hidden Bullish scanner stopped'
    def status(self):
        return {'running':self.enabled,'message':self.message,'updated':self.updated,'watching':len(self.watch),'identified':len(self.identified),'loaded':len(self.shared.candles),'symbols':len(self.shared.symbols),'feed_running':self.shared.enabled}

    def _loop(self):
        while self.running:
            if self.enabled:
                try:
                    with self.shared.lock: snap={s:list(a) for s,a in self.shared.candles.items()}
                    now=int(time.time()*1000); found={}
                    for symbol, rows in snap.items():
                        # Do not generate another signal for a coin that already has an identified setup.
                        if symbol in self.identified: continue
                        if len(rows)<40: continue
                        closes=[x['close'] for x in rows]; lows=[x['low'] for x in rows]; rs=rsi_series(closes,14)
                        # Find the latest completed local swing low: low <= prior two and lower than next two.
                        closed_idx=[i for i,x in enumerate(rows) if x.get('closed')]
                        if len(closed_idx)<35: continue
                        last_closed=closed_idx[-1]
                        # First resolve a watched candle once it has closed.
                        live_idx=len(rows)-1 if rows and not rows[-1].get('closed') else None
                        prior_watch=self.watch.get(symbol)
                        if prior_watch and prior_watch.get('signal_time'):
                            closed_match=next((i for i,x in enumerate(rows) if x.get('closed') and x['close_time']==prior_watch['signal_time']),None)
                            if closed_match is not None:
                                a=next((i for i,x in enumerate(rows) if x['open_time']==prior_watch.get('first_low_time')),None)
                                if a is not None:
                                    final_rsi=rs[closed_match]
                                    if lows[closed_match] < lows[a] and final_rsi is not None and final_rsi > rs[a]:
                                        ctime=rows[closed_match]['close_time']
                                        nxt=next((x for x in rows if x['open_time']>ctime),None)
                                        self.identified[symbol]={'symbol':symbol,'price':rows[closed_match]['close'],'price_low_1':lows[a],
                                            'price_low_2':lows[closed_match],'rsi_low_1':round(rs[a],2),'rsi_low_2':round(final_rsi,2),
                                            'first_low_time':rows[a]['open_time'],'signal_time':ctime,'gap_candles':closed_match-a,
                                            'status':'IDENTIFIED','timeframe':'1H','identified_at':now,'entry_time':ctime+1,
                                            'entry_price':nxt['open'] if nxt else None,'method':'Hidden Bullish'}
                                self.watch.pop(symbol,None)
                        if live_idx is None: continue
                        # A new potential setup is only added during the final 15 minutes of a live candle.
                        candidate_idx=live_idx
                        if candidate_idx is None: continue
                        current=rows[candidate_idx]
                        seconds_left=max(0,int((current['close_time']-now)/1000))
                        if not (0 < seconds_left <= 900): continue
                        # Reference low must be a prior confirmed swing, 3–30 bars back.
                        refs=[]
                        for i in range(2, candidate_idx-1):
                            if lows[i] <= lows[i-1] and lows[i] < lows[i-2] and lows[i] <= lows[i+1] and lows[i] < lows[i+2] and rs[i] is not None:
                                gap=candidate_idx-i
                                if 3 <= gap <= 30: refs.append(i)
                        if not refs: continue
                        a=refs[-1]
                        # Only show a setup when THIS live candle is making a fresh lower low: it must break
                        # the reference low and be lower than every intervening candle since that reference.
                        intervening_lows = lows[a+1:candidate_idx]
                        if current['low'] >= lows[a]: continue
                        if intervening_lows and current['low'] >= min(intervening_lows): continue
                        item={'symbol':symbol,'price':current['close'],'price_low_1':lows[a], 'price_low_2':current['low'],
                              'rsi_low_1':round(rs[a],2),'rsi_low_2':round(rs[candidate_idx] if rs[candidate_idx] is not None else rs[last_closed],2),
                              'first_low_time':rows[a]['open_time'],'signal_time':current['close_time'],'seconds_remaining':seconds_left,
                              'gap_candles':candidate_idx-a,'status':'WATCH','timeframe':'1H'}
                        found[symbol]=item
                    with self.lock:
                        self.watch=found
                        self.message=f'Master feed: {len(snap)} loaded / {len(self.shared.symbols)} pairs · {len(found)} potential setups in final 15m'
                        self.updated=now
                except Exception as e: self.message=f'Scanner error: {e}'
            time.sleep(5)

    def watch_list(self):
        with self.lock: return sorted((dict(x) for x in self.watch.values()),key=lambda x:x.get('seconds_remaining',0))
    def identified_list(self):
        with self.lock: return sorted((dict(x) for x in self.identified.values()),key=lambda x:x.get('identified_at',0),reverse=True)
    def manual_result(self,symbol,result,close_time,tp=None,sl=None):
        result=str(result or '').upper().strip()
        if result not in ('TP HIT','SL HIT'): return {'ok':False,'error':'Result must be TP HIT or SL HIT'},400
        setup=self.identified.get(symbol)
        if not setup: return {'ok':False,'error':'Hidden Bullish setup not found'},404
        try: close_time=int(close_time); assert close_time>0
        except Exception: return {'ok':False,'error':'Please enter trade close date/time'},400
        c=sqlite3.connect(self.dbpath)
        try:
            c.execute("INSERT INTO reports(symbol,trend,rsi,pump1,pump2,one_min_pump,entry_time,close_time,entry_price,exit_price,max_pump,result,tp_manual,sl_manual) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (symbol,'Hidden Bullish',setup.get('rsi_low_2'),None,None,None,setup.get('entry_time') or setup.get('identified_at'),close_time,setup.get('entry_price'),None,None,result,None if tp is None else str(tp),None if sl is None else str(sl)))
            c.commit()
        finally: c.close()
        self.identified.pop(symbol,None)
        return {'ok':True,'symbol':symbol,'result':result},200
    def skip_trade(self,symbol):
        if symbol not in self.identified: return {'ok':False,'error':'Hidden Bullish setup not found'},404
        self.identified.pop(symbol,None); return {'ok':True,'symbol':symbol,'status':'SKIPPED'},200
