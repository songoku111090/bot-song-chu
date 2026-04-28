import ccxt
import pandas as pd
import time
import requests
import os
import pickle
from datetime import datetime
from threading import Thread

# ==========================================
# 1. CẤU HÌNH THÔNG TIN
# ==========================================
TOKEN = "7790113864:AAF9In2hd9UKHRCzL772NC41TkIVTkDCcug"
CHAT_ID = "1562661521"
DATA_FILE = "candle_db.pkl"

exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

# Biến toàn cục
cached_top_70 = []
last_update_top_70 = -1 
candle_db = {} # Lưu trữ nến: { symbol: { '5m': df, '1h': df } }

# Load database từ file nếu có
if os.path.exists(DATA_FILE):
    try:
        with open(DATA_FILE, 'rb') as f:
            candle_db = pickle.load(f)
        print(f"[SYSTEM] Đã nạp dữ liệu từ {DATA_FILE}", flush=True)
    except:
        print("[SYSTEM] Lỗi nạp database cũ, khởi tạo mới.", flush=True)

def save_db():
    try:
        with open(DATA_FILE, 'wb') as f:
            pickle.dump(candle_db, f)
    except Exception as e:
        print(f"[!] Lỗi lưu Database: {e}", flush=True)

def send_tele(mes):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&text={mes}"
    try:
        requests.get(url, timeout=10)
    except:
        print("\n[!] Lỗi gửi Telegram", flush=True)

# ==========================================
# 2. LỌC TOP 70
# ==========================================
def get_top_70_movers():
    now_str = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{now_str}] --- Đang cập nhật mới danh sách Top 70 (30p/lần) ---", flush=True)
    try:
        tickers = exchange.fetch_tickers()
        movers = []
        for symbol, t in tickers.items():
            if symbol.endswith('/USDT:USDT') and t['percentage'] is not None:
                if t['percentage'] > 0:
                    movers.append({'symbol': symbol, 'change': t['percentage']})
        
        top_70 = sorted(movers, key=lambda x: x['change'], reverse=True)[:70]
        final_list = [item['symbol'] for item in top_70]
        print(f" Tìm thấy {len(final_list)} con thỏa mãn điều kiện tăng 24h > 0.", flush=True)
        return final_list
    except Exception as e:
        print(f"Lỗi khi lấy Top 70: {e}", flush=True)
        return []

# ==========================================
# 3. QUẢN LÝ DỮ LIỆU NẾN THÔNG MINH
# ==========================================
def get_ohlcv_smart(symbol, tf):
    global candle_db
    if symbol not in candle_db:
        candle_db[symbol] = {'5m': pd.DataFrame(), '1h': pd.DataFrame()}
    
    target_tf = '5m' if tf in ['10m', '15m', '30m'] else '1h'
    limit_needed = 995 if target_tf == '5m' else 305 # Dự phòng dư vài nến
    
    df_old = candle_db[symbol][target_tf]
    
    try:
        if df_old.empty:
            # Con mới hoàn toàn, lấy full từ đầu
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=target_tf, limit=limit_needed)
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        else:
            # Con cũ, chỉ lấy thêm nến mới dựa trên timestamp cuối cùng
            last_ts = int(df_old['ts'].iloc[-1].timestamp() * 1000)
            new_ohlcv = exchange.fetch_ohlcv(symbol, timeframe=target_tf, since=last_ts + 1)
            
            if new_ohlcv:
                new_df = pd.DataFrame(new_ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                new_df['ts'] = pd.to_datetime(new_df['ts'], unit='ms')
                df = pd.concat([df_old, new_df]).drop_duplicates('ts').tail(1000)
            else:
                df = df_old
        
        # Cập nhật lại vào DB
        candle_db[symbol][target_tf] = df
        
        # Nếu là khung ghép (10-30m), thực hiện resample
        if tf in ['10m', '15m', '30m']:
            resample_map = {'10m': '10min', '15m': '15min', '30m': '30min'}
            df_resampled = df.set_index('ts').resample(resample_map[tf], closed='left', label='left').agg({
                'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'vol': 'sum'
            }).dropna()
            return df_resampled.iloc[:-1] # Bỏ nến đang chạy
        else:
            # Khung 1h lấy trực tiếp
            return df.set_index('ts').iloc[:-1]
            
    except Exception as e:
        print(f"\n[LOG] Lỗi lấy data {symbol} ({tf}): {e}", flush=True)
        return pd.DataFrame()

# ==========================================
# 4. LOGIC SO KÈO (GIỮ NGUYÊN ĐIỀU KIỆN)
# ==========================================
def check_logic(symbol, tf):
    # Lấy data thông minh
    df = get_ohlcv_smart(symbol, tf)

    if df is None or df.empty or len(df) < 55: return False

    try:
        # Tính toán EMA trên tập dữ liệu nến đã đóng
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema34'] = df['close'].ewm(span=34, adjust=False).mean()
        df['ema55'] = df['close'].ewm(span=55, adjust=False).mean()
        
        # Lấy các nến cuối cùng để check logic (n1 là nến vừa đóng xong)
        n1 = df.iloc[-1]
        n2, n3, n4 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        
        # 1. Điều kiện xu hướng EMA
        if not (n1['ema21'] > n1['ema34'] > n1['ema55']): return False
        
        # 2. Điều kiện 15 nến trước đó nằm trên EMA34
        last_15 = df.iloc[-15:] 
        if not all(last_15['low'] > last_15['ema34']): return False

        # --- ĐIỀU KIỆN RÂU NẾN ---
        def is_good_body(r):
            return (r['high'] - r['ema21']) > (r['ema21'] - r['low'])

        def touch21(r): return r['low'] <= r['ema21'] <= r['high']

        th1 = all([touch21(x) and is_good_body(x) for x in [n1, n2, n3]])
        th2_hits = all([touch21(x) for x in [n1, n2, n3, n4]])
        th2_bodies = is_good_body(n1) and sum([is_good_body(x) for x in [n2, n3, n4]]) >= 2
        th2 = th2_hits and th2_bodies
        th3 = all([touch21(x) and is_good_body(x) for x in [n1, n2, n3, n4]])
        
        if not (th1 or th2 or th3): return False

        if not th3:
            if n1['close'] < n1['open']:
                green_count = sum([1 for x in [n2, n3, n4] if x['close'] > x['open']])
                if green_count < 2: return False
            
        current_price = n1['close']
        c_val = (n1['ema21'] - n1['ema34']) / n1['ema34']
        c_percent = c_val * 100
        display_val = 10 / c_val if c_val != 0 else 0
        tf_display = tf.replace('m', 'M').replace('1h', 'H1')
        coin_name = symbol.split('/')[0]
        
        return f"{coin_name} chạm {tf_display} - Giá: {current_price} - C {c_percent:.2f}% - {display_val:.0f}$"
    except Exception as e:
        return False

# ==========================================
# 5. VÒNG LẶP CHÍNH
# ==========================================
def main_loop():
    global cached_top_70, last_update_top_70
    
    print("------------------------------------------", flush=True)
    print("🔥 BOT SÓNG CHỦ - SMART DB & FETCH 🔥", flush=True)
    print("------------------------------------------", flush=True)
    
    last_run_key = "" 
    
    while True:
        now = datetime.now()
        minute = now.minute
        second = now.second
        
        if second == 5:
            current_run_key = f"{minute}_{now.hour}"
            
            if current_run_key != last_run_key:
                tfs_to_check = []
                if minute % 10 == 0: tfs_to_check.append('10m')
                if minute % 15 == 0: tfs_to_check.append('15m')
                if minute % 30 == 0: tfs_to_check.append('30m')
                if minute == 0: tfs_to_check.append('1h')

                if tfs_to_check:
                    if not cached_top_70 or minute in [0, 30]:
                        if last_update_top_70 != minute:
                            cached_top_70 = get_top_70_movers()
                            last_update_top_70 = minute
                            save_db() # Lưu file định kỳ khi đổi top 70

                    print(f"\n[{now.strftime('%H:%M:%S')}] Khởi động quét khung: {tfs_to_check}", flush=True)
                    
                    if cached_top_70:
                        for tf in tfs_to_check:
                            current_scan_list = cached_top_70[:50] if tf in ['10m', '15m'] else cached_top_70
                            list_name = "Top 50" if tf in ['10m', '15m'] else "Top 70"

                            print(f"--- Đang check khung {tf} cho {len(current_scan_list)} con ({list_name}) ---", flush=True)
                            for i, s in enumerate(current_scan_list):
                                print(f"[{i+1}/{len(current_scan_list)}] Soi {tf}: {s:<12}", end='\r', flush=True)
                                alert_msg = check_logic(s, tf)
                                if alert_msg:
                                    print(f"\n✅ {alert_msg}", flush=True)
                                    send_tele(alert_msg)
                                    time.sleep(0.05)
                    
                    last_run_key = current_run_key
                    print(f"\nLượt quét giây thứ 5 của phút {minute} hoàn tất.", flush=True)

        time.sleep(0.5)

# --- PHẦN LỪA RENDER (FIX LỖI 501) ---
def health_check():
    from http.server import HTTPServer, BaseHTTPRequestHandler
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
        def do_HEAD(self):
            self.send_response(200); self.end_headers()
        def log_message(self, format, *args): return 

    port = int(os.environ.get("PORT", 10000))
    print(f"--- Đang mở Port lừa Render: {port} ---", flush=True)
    try:
        HTTPServer(('0.0.0.0', port), H).serve_forever()
    except:
        pass

if __name__ == "__main__":
    Thread(target=health_check, daemon=True).start()
    main_loop()
