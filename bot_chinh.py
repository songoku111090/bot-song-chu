import ccxt
import pandas as pd
import time
import requests
from datetime import datetime

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
        print("\n[!] Lỗi gửi Telegram")

# ==========================================
# 2. TỐI ƯU SIÊU NHANH: LỌC TOP 70 (24H % > 0)
# ==========================================
def get_top_70_movers():
    now_str = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{now_str}] --- Đang quét nhanh Top 70 con tăng mạnh trong 24h ---")
    try:
        tickers = exchange.fetch_tickers()
        movers = []
        for symbol, t in tickers.items():
            if symbol.endswith('/USDT:USDT') and t['percentage'] is not None:
                if t['percentage'] > 0:
                    movers.append({'symbol': symbol, 'change': t['percentage']})
        
        top_70 = sorted(movers, key=lambda x: x['change'], reverse=True)[:70]
        final_list = [item['symbol'] for item in top_70]
        print(f" Tìm thấy {len(final_list)} con thỏa mãn điều kiện tăng 24h > 0.")
        return final_list
    except Exception as e:
        print(f"Lỗi khi lấy Top 70: {e}")
        return []

# ==========================================
# 3. LOGIC GHÉP NẾN & SO KÈO (ĐÃ BỎ PANDAS_TA)
# ==========================================
def check_logic(symbol, tf):
    try:
        if tf == '10m':
            ohlcv_5m = exchange.fetch_ohlcv(symbol, timeframe='5m', limit=240)
            df_5m = pd.DataFrame(ohlcv_5m, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            df_5m['ts'] = pd.to_datetime(df_5m['ts'], unit='ms')
            df_5m.set_index('ts', inplace=True)
            
            df = df_5m.resample('10min', closed='left', label='left').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'vol': 'sum'
            }).dropna()
        else:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=120)
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        
        # TÍNH EMA DÙNG PANDAS THUẦN (CHÍNH XÁC & KHÔNG LỖI BUILD)
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema34'] = df['close'].ewm(span=34, adjust=False).mean()
        df['ema55'] = df['close'].ewm(span=55, adjust=False).mean()
        
        n1 = df.iloc[-2]
        n2, n3, n4 = df.iloc[-3], df.iloc[-4], df.iloc[-5]
        
        if not (n1['ema21'] > n1['ema34'] > n1['ema55']): return False
        
        last_15 = df.iloc[-16:-1] 
        if not all(last_15['low'] > last_15['ema34']): return False

        def is_good_body(r):
            upper_wick = r['high'] - max(r['open'], r['close'])
            lower_wick = min(r['open'], r['close']) - r['low']
            return upper_wick > lower_wick

        def touch21(r): return r['low'] <= r['ema21'] <= r['high']

        th1 = all([touch21(x) and is_good_body(x) for x in [n2, n3, n4]])
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
    except:
        return False

# ==========================================
# 4. VÒNG LẶP QUÉT ĐA KHUNG
# ==========================================
def main():
    print("------------------------------------------")
    print("🔥 BOT SÓNG CHỦ ONLINE - RENDER VERSION 🔥")
    print("------------------------------------------")
    
    last_run_minute = -1
    
    while True:
        now = datetime.now()
        minute = now.minute
        
        if minute != last_run_minute and minute % 5 == 0:
            tfs_to_check = []
            if minute % 10 == 0: tfs_to_check.append('10m')
            if minute % 15 == 0: tfs_to_check.append('15m')
            if minute % 30 == 0: tfs_to_check.append('30m')
            if minute == 0: tfs_to_check.append('1h')

            if tfs_to_check:
                symbols = get_top_70_movers()
                if symbols:
                    print(f"[{now.strftime('%H:%M:%S')}] Kiểm tra {len(symbols)} con cho khung: {tfs_to_check}")
                    for i, s in enumerate(symbols):
                        print(f"[{i+1}/{len(symbols)}] Soi: {s:<12}", end='\r')
                        for tf in tfs_to_check:
                            alert_msg = check_logic(s, tf)
                            if alert_msg:
                                print(f"\n✅ {alert_msg}")
                                send_tele(alert_msg)
                            time.sleep(0.05)
            
            last_run_minute = minute
            print(f"\nLượt quét phút {minute} hoàn tất. Đang chờ mốc tiếp theo...")
        
        time.sleep(30)

if __name__ == "__main__":
    main()
