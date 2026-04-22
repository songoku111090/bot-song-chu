import ccxt
import pandas as pd
import time
import requests
import os
from datetime import datetime
from threading import Thread
from concurrent.futures import ThreadPoolExecutor # Thư viện hỗ trợ đa luồng

# ==========================================
# 1. CẤU HÌNH THÔNG TIN
# ==========================================
TOKEN = "7790113864:AAF9In2hd9UKHRCzL772NC41TkIVTxDCcug"
CHAT_ID = "1562661521"

exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

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
    print(f"\n[{now_str}] --- Đang quét nhanh Top 70 tăng mạnh trong 24h ---", flush=True)
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
# 2b. LỌC TOP 5 TĂNG MẠNH NHẤT TRONG 4H (CHO KHUNG 5M)
# ==========================================
def get_top_5_in_4h():
    now_str = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{now_str}] --- Đang lọc Top 5 tăng mạnh nhất trong 4h gần nhất ---", flush=True)
    try:
        tickers = exchange.fetch_tickers()
        all_symbols = [s for s in tickers.keys() if s.endswith('/USDT:USDT')]
        
        change_list = []
        for s in all_symbols:
            try:
                ohlcv = exchange.fetch_ohlcv(s, timeframe='1h', limit=5)
                if len(ohlcv) < 5: continue
                price_4h_ago = ohlcv[0][1] 
                last_closed_price = ohlcv[-2][4] 
                change = (last_closed_price - price_4h_ago) / price_4h_ago
                change_list.append({'symbol': s, 'change': change})
            except:
                continue
                
        top_5 = sorted(change_list, key=lambda x: x['change'], reverse=True)[:5]
        final_list = [item['symbol'] for item in top_5]
        print(f" Tìm thấy Top 5: {final_list}", flush=True)
        return final_list
    except Exception as e:
        print(f"Lỗi khi lọc Top 5 4h: {e}", flush=True)
        return []

# ==========================================
# 3. LOGIC GHÉP NẾN & SO KÈO (GIỮ NGUYÊN)
# ==========================================
def check_logic(symbol, tf):
    try:
        if tf == '10m':
            ohlcv_5m = exchange.fetch_ohlcv(symbol, timeframe='5m', limit=601)
            df_raw = pd.DataFrame(ohlcv_5m, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            df_raw['ts'] = pd.to_datetime(df_raw['ts'], unit='ms')
            df_raw.set_index('ts', inplace=True)
            df = df_raw.resample('10min', closed='left', label='left').agg({
                'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'vol': 'sum'
            }).dropna()
            df = df.iloc[:-1]
        else:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=301)
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            df = df.iloc[:-1]
        
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema34'] = df['close'].ewm(span=34, adjust=False).mean()
        df['ema55'] = df['close'].ewm(span=55, adjust=False).mean()
        
        n1 = df.iloc[-1]
        n2, n3, n4 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        
        if not (n1['ema21'] > n1['ema34'] > n1['ema55']): return False
        
        last_15 = df.iloc[-15:] 
        if not all(last_15['low'] > last_15['ema34']): return False

        def is_good_body(r):
            return (r['high'] - r['ema21']) > (r['ema21'] - r['low'])

        def touch21(r): return r['low'] <= r['ema21'] <= r['high']

        th1 = all([touch21(x) and is_good_body(x) for x in [n1, n2, n3]])
        
        th2_hits = all([touch21(x) for x in [n1, n2, n3, n4]])
        th2_bodies = is_good_body(n1) and sum([is_good_body(x) for x in [n2, n3, n4]]) >= 2
        th2 = th2_hits and th2_bodies
        
        if not (th1 or th2): return False

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
# 4. VÒNG LẶP CHÍNH (ĐÃ NÂNG CẤP ĐA LUỒNG)
# ==========================================
def main_loop():
    print("------------------------------------------", flush=True)
    print("🔥 BOT SÓNG CHỦ ONLINE - ĐA LUỒNG 8 WORKERS 🔥", flush=True)
    print("------------------------------------------", flush=True)
    
    last_run_minute = -1
    
    while True:
        now = datetime.now()
        minute = now.minute
        
        if minute != last_run_minute and minute % 5 == 0:
            tfs_to_check = ['5m']
            if minute % 10 == 0: tfs_to_check.append('10m')
            if minute % 15 == 0: tfs_to_check.append('15m')
            if minute % 30 == 0: tfs_to_check.append('30m')
            if minute == 0: tfs_to_check.append('1h')

            if tfs_to_check:
                top_70 = get_top_70_movers()
                top_5_4h = get_top_5_in_4h()
                
                print(f"[{now.strftime('%H:%M:%S')}] Đang quét các khung: {tfs_to_check}", flush=True)
                
                for tf in tfs_to_check:
                    current_symbols = top_5_4h if tf == '5m' else top_70
                    if not current_symbols: continue
                    
                    print(f"--- Đang check khung {tf} cho {len(current_symbols)} con (Đa luồng) ---", flush=True)
                    
                    # SỬ DỤNG 8 LUỒNG ĐỂ QUÉT SONG SONG
                    with ThreadPoolExecutor(max_workers=8) as executor:
                        # Giao việc cho 8 luồng
                        future_to_symbol = {executor.submit(check_logic, s, tf): s for s in current_symbols}
                        
                        count = 0
                        for future in future_to_symbol:
                            count += 1
                            symbol = future_to_symbol[future]
                            print(f"[{count}/{len(current_symbols)}] Soi {tf}: {symbol:<12}", end='\r', flush=True)
                            
                            try:
                                alert_msg = future.result()
                                if alert_msg:
                                    print(f"\n✅ {alert_msg}", flush=True)
                                    send_tele(alert_msg)
                            except Exception as e:
                                pass # Bỏ qua lỗi nhỏ của từng luồng
            
            last_run_minute = minute
            print(f"\nLượt quét phút {minute} hoàn tất. Đang chờ mốc tiếp theo...", flush=True)
        
        time.sleep(10)

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
