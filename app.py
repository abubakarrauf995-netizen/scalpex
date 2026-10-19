import os, sqlite3, threading
from flask import Flask, jsonify, render_template, request, redirect, url_for, session, request, redirect, url_for, session
from dotenv import load_dotenv
from live_scanner import LiveScanner
from hidden_bullish import HiddenBullishScanner

load_dotenv()
app=Flask(__name__)
app.secret_key=os.getenv("SECRET_KEY", "change-this-scalpex-secret-key")
LOGIN_USERNAME=os.getenv("SCALPEX_USERNAME", "admin")
LOGIN_PASSWORD=os.getenv("SCALPEX_PASSWORD", "Scalpex@2026")

@app.before_request
def require_login():
    if request.endpoint in ("login", "logout", "static"):
        return None
    if not session.get("scalpex_logged_in"):
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Please log in first."}), 401
        return redirect(url_for("login"))

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        if username == LOGIN_USERNAME and password == LOGIN_PASSWORD:
            session.clear()
            session['scalpex_logged_in'] = True
            session['scalpex_user'] = username
            return redirect(url_for('home'))
        error = 'Incorrect username or password. Please try again.'
    return render_template('login.html', error=error)

@app.get('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

DB=os.getenv("DB_PATH","trading.db")
scanner=LiveScanner(DB)
hidden_bullish=HiddenBullishScanner(scanner)

def conn():
    c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; return c

def init_db():
    c=conn()
    c.execute("""CREATE TABLE IF NOT EXISTS setup_events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,trend TEXT,rsi REAL,pump1 REAL,pump2 REAL,
        status TEXT,one_min_pump REAL,event_time INTEGER,reason TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS active_trades(
        symbol TEXT PRIMARY KEY,trend TEXT,rsi REAL,pump1 REAL,pump2 REAL,one_min_pump REAL,
        setup_time INTEGER, entry_time INTEGER,entry_price REAL,tp_price REAL,sl_price REAL,max_pump REAL,status TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS reports(
        id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,trend TEXT,rsi REAL,pump1 REAL,pump2 REAL,
        one_min_pump REAL,entry_time INTEGER,close_time INTEGER,entry_price REAL,exit_price REAL,
        max_pump REAL,result TEXT)""")
    # Upgrade older databases without breaking existing installs.
    for table in ("setup_events", "active_trades", "reports"):
        cols={r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()}
        if "rsi" not in cols:
            c.execute(f"ALTER TABLE {table} ADD COLUMN rsi REAL")
    report_cols={r[1] for r in c.execute("PRAGMA table_info(reports)").fetchall()}
    if "tp_manual" not in report_cols: c.execute("ALTER TABLE reports ADD COLUMN tp_manual TEXT")
    if "sl_manual" not in report_cols: c.execute("ALTER TABLE reports ADD COLUMN sl_manual TEXT")
    cols={r[1] for r in c.execute("PRAGMA table_info(active_trades)").fetchall()}
    if "setup_time" not in cols:
        c.execute("ALTER TABLE active_trades ADD COLUMN setup_time INTEGER")
    c.commit(); c.close()

@app.get('/')
def home(): return render_template('index.html')
@app.get('/api/status')
def status(): return jsonify(scanner.status())
@app.get('/api/hidden-bullish/status')
def hidden_bullish_status(): return jsonify(hidden_bullish.status())
@app.post('/api/hidden-bullish/start')
def hidden_bullish_start(): hidden_bullish.enable(); return jsonify(hidden_bullish.status())
@app.post('/api/hidden-bullish/stop')
def hidden_bullish_stop(): hidden_bullish.disable(); return jsonify(hidden_bullish.status())
@app.get('/api/hidden-bullish/watch')
def hidden_bullish_watch(): return jsonify(hidden_bullish.watch_list())
@app.get('/api/hidden-bullish/identified')
def hidden_bullish_identified(): return jsonify(hidden_bullish.identified_list())
@app.get('/api/hidden-bullish/reports')
def hidden_bullish_reports():
    c=conn()
    try:
        rows=c.execute("SELECT * FROM reports WHERE trend=? ORDER BY close_time DESC, id DESC", ('Hidden Bullish',)).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        c.close()
@app.post('/api/hidden-bullish/manual-result/<symbol>')
def hidden_bullish_manual_result(symbol):
    payload=request.get_json(silent=True) or {}
    result, code=hidden_bullish.manual_result(symbol.upper(),payload.get('result'),payload.get('close_time'),payload.get('tp'),payload.get('sl'))
    return jsonify(result), code
@app.post('/api/hidden-bullish/skip/<symbol>')
def hidden_bullish_skip(symbol):
    result, code=hidden_bullish.skip_trade(symbol.upper())
    return jsonify(result), code
@app.post('/api/start')
def start(): scanner.enable(); return jsonify(scanner.status())
@app.post('/api/stop')
def stop():
    scanner.disable_method1()
    if not hidden_bullish.enabled:
        scanner.disable_feed()
    return jsonify(scanner.status())
@app.get('/api/watch')
def watch(): return jsonify(scanner.watch_list())
@app.get('/api/identified')
def identified(): return jsonify(scanner.identified_list())
@app.post('/api/manual-result/<symbol>')
def manual_result(symbol):
    from flask import request
    payload = request.get_json(silent=True) or {}
    result, code = scanner.mark_manual_result(symbol.upper(), payload.get('result'), payload.get('close_time'))
    return jsonify(result), code
@app.post('/api/skip-trade/<symbol>')
def skip_trade(symbol):
    result, code = scanner.skip_identified(symbol.upper())
    return jsonify(result), code
@app.get('/api/confirmed')
def confirmed(): return jsonify(scanner.confirmed_list())
@app.get('/api/invalid')
def invalid(): return jsonify(scanner.invalid_list())
@app.get('/api/active')
def active(): return jsonify(scanner.active_list())
@app.get('/api/reports')
def reports(): return jsonify(scanner.reports())
@app.delete('/api/reports/<int:report_id>')
def delete_report(report_id):
    c=conn()
    try:
        cur=c.execute('DELETE FROM reports WHERE id=?',(report_id,))
        c.commit()
        if cur.rowcount == 0: return jsonify({'ok':False,'error':'Report not found'}),404
        return jsonify({'ok':True,'deleted_id':report_id})
    finally:
        c.close()
@app.get('/api/events')
def events(): return jsonify(scanner.events())
@app.get('/api/test/<symbol>')
def test(symbol): return jsonify(scanner.test_symbol(symbol.upper()))

init_db(); scanner.start(); hidden_bullish.start()
if __name__=='__main__':
    import webbrowser,time
    threading.Timer(1.2,lambda:webbrowser.open('http://127.0.0.1:5000')).start()
    app.run(host='127.0.0.1',port=5000,debug=False)
