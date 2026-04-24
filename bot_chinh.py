import ccxt
import pandas as pd
import time
import requests
import os
from datetime import datetime
from threading import Thread

# ==========================================
# 1. CẤU HÌNH THÔNG TIN
# ==========================================
TOKEN = "7790113864:AAF9In2hd9UKHRCzL772NC41TkIVTxDCcug"
CHAT_ID = "1562661521"

exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

# Biến toàn cục để lưu danh sách Top 70 dùng chung trong 30 phút
cached_top_70 = []
last_update_top_70 = -1 

def send_tele(mes):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&text={mes}"
    try:
        requests.get(url, timeout=10)
    except:
        print("\n[!] Lỗi gửi Telegram", flush=True)

# ==========================================
# 2. TỐI ƯU SIÊU NHANH: LỌC TOP 70 (24H % > 0)
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
# 3. LOGIC GHÉP NẾN & SO KÈO (PANDAS THUẦN)
# ==========================================
def check_logic(symbol, tf):
    df = None
    # --- CƠ CHẾ THỬ LẠI KHI LẤY OHLCV (TỐI ĐA 2 LẦN THÊM) ---
    for attempt in range(3): # 0 là lần đầu, 1-2 là retry
        try:
            if tf == '10m':
                # Lấy 601 nến 5p để ghép thành 300 nến 10p hoàn chỉnh (bỏ nến đang chạy)
                ohlcv_5m = exchange.fetch_ohlcv(symbol, timeframe='5m', limit=601)
                df_raw = pd.DataFrame(ohlcv_5m, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                df_raw['ts'] = pd.to_datetime(df_raw['ts'], unit='ms')
                df_raw.set_index('ts', inplace=True)
                
                # Ghép nến 10m
                df = df_raw.resample('10min', closed='left', label='left').agg({
                    'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'vol': 'sum'
                }).dropna()
                # Bỏ nến đang chạy của khung 10m
                df = df.iloc[:-1]
            else:
                # Lấy 301 nến để có 300 nến hoàn chỉnh sau khi bỏ nến đang chạy
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=301)
                df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                # Bỏ nến đang chạy
                df = df.iloc[:-1]
            
            if not df.empty:
                break # Lấy thành công thì thoát vòng lặp retry
        except Exception as e:
            if attempt < 2:
                time.sleep(0.5) # Nghỉ ngắn trước khi thử lại
                continue
            else:
                print(f"\n[LOG] Lỗi lấy OHLCV {symbol} ({tf}) sau 3 lần thử: {e}", flush=True)
                return False

    if df is None or df.empty: return False

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

        # --- ĐIỀU KIỆN RÂU NẾN MỚI ---
        def is_good_body(r):
            # high - ema 21 > ema 21 - low
            return (r['high'] - r['ema21']) > (r['ema21'] - r['low'])

        def touch21(r): return r['low'] <= r['ema21'] <= r['high']

        # Check các trường hợp chạm EMA21
        th1 = all([touch21(x) and is_good_body(x) for x in [n1, n2, n3]])
        
        th2_hits = all([touch21(x) for x in [n1, n2, n3, n4]])
        th2_bodies = is_good_body(n1) and sum([is_good_body(x) for x in [n2, n3, n4]]) >= 2
        th2 = th2_hits and th2_bodies
        
        if not (th1 or th2): return False

        # Điều kiện nến hồi
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
# 4. VÒNG LẶP CHÍNH
# ==========================================
def main_loop():
    global cached_top_70, last_update_top_70
    
    print("------------------------------------------", flush=True)
    print("🔥 BOT SÓNG CHỦ ONLINE - CACHED 30P 🔥", flush=True)
    print("------------------------------------------", flush=True)
    
    last_run_key = "" # Dùng key để định danh mốc thời gian đã chạy
    
    while True:
        now = datetime.now()
        minute = now.minute
        second = now.second
        
        # Kiểm tra nếu đúng giây thứ 5
        if second == 5:
            # Tạo key định danh: "phút_giờ"
            current_run_key = f"{minute}_{now.hour}"
            
            if current_run_key != last_run_key:
                tfs_to_check = []
                
                # Check các mốc phút
                if minute % 10 == 0: tfs_to_check.append('10m')
                if minute % 15 == 0: tfs_to_check.append('15m')
                if minute % 30 == 0: tfs_to_check.append('30m')
                if minute == 0: tfs_to_check.append('1h')

                if tfs_to_check:
                    # Logic cập nhật Top 70 mỗi 30 phút (phút 00 và 30)
                    if not cached_top_70 or minute in [0, 30]:
                        # Chỉ lấy lại nếu chưa lấy lần nào hoặc đúng phút 0/30 và chưa lấy trong phút này
                        if last_update_top_70 != minute:
                            cached_top_70 = get_top_70_movers()
                            last_update_top_70 = minute

                    print(f"\n[{now.strftime('%H:%M:%S')}] Khởi động quét khung: {tfs_to_check}", flush=True)
                    
                    if cached_top_70:
                        for tf in tfs_to_check:
                            print(f"--- Đang check khung {tf} cho {len(cached_top_70)} con ---", flush=True)
                            for i, s in enumerate(cached_top_70):
                                print(f"[{i+1}/{len(cached_top_70)}] Soi {tf}: {s:<12}", end='\r', flush=True)
                                alert_msg = check_logic(s, tf)
                                if alert_msg:
                                    print(f"\n✅ {alert_msg}", flush=True)
                                    send_tele(alert_msg)
                                    time.sleep(0.05)
                    
                    last_run_key = current_run_key
                    print(f"\nLượt quét giây thứ 5 của phút {minute} hoàn tất.", flush=True)

        # Nghỉ 0.5s để không bỏ lỡ giây thứ 5 và không tốn CPU
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
    # Chạy Health Check ở luồng phụ
    Thread(target=health_check, daemon=True).start()
    # Chạy Bot ở luồng chính
    main_loop()
