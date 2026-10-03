import os
import time
import threading
import requests
import pandas as pd
from flask import Flask

# ==========================================
# 1. 기본 설정 및 사용자 정보
# ==========================================
BOT_TOKEN = "8866848171:AAH0Jjh18W-XA2eRQIsOG0WFS4YMxmp7ICc"
CHAT_ID = "5624306078"
INTERVAL = "1m"

current_candle_time = None
alerted_upper = False
alerted_lower = False

session = requests.Session()

# ==========================================
# 2. 텔레그램 발송 및 바이낸스 감시 로직
# ==========================================
def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        session.post(url, data={"chat_id": CHAT_ID, "text": text}, timeout=15)
    except Exception as e:
        print(f"텔레그램 전송 오류: {e}")

def check_bollinger_realtime():
    global current_candle_time, alerted_upper, alerted_lower
    
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol=ETHUSDT&interval={INTERVAL}&limit=60"
    res = session.get(url, timeout=10).json()
    
    df = pd.DataFrame(res, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "q_vol", "trades", "tb_base_vol", "tb_quote_vol", "ignore"
    ])
    df["close"] = df["close"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    
    period = 45
    std_mult = 2
    df["mid"] = df["close"].rolling(window=period).mean()
    df["std"] = df["close"].rolling(window=period).std()
    df["upper"] = df["mid"] + (df["std"] * std_mult)
    df["lower"] = df["mid"] - (df["std"] * std_mult)
    
    now_candle = df.iloc[-1]
    candle_time = now_candle["open_time"]
    current_high = now_candle["high"]
    current_low = now_candle["low"]
    upper_band = now_candle["upper"]
    lower_band = now_candle["lower"]
    
    if candle_time != current_candle_time:
        current_candle_time = candle_time
        alerted_upper = False
        alerted_lower = False
    
    if current_high >= upper_band and not alerted_upper:
        send_telegram("상단")
        alerted_upper = True
        print("상단 돌파 알림 전송")

    if current_low <= lower_band and not alerted_lower:
        send_telegram("하단")
        alerted_lower = True
        print("하단 돌파 알림 전송")

def bot_loop():
    print("ETH 볼린저밴드(45) 감시 시작...")
    send_telegram("시작")
    while True:
        try:
            check_bollinger_realtime()
        except Exception as e:
            print(f"조회 일시 지연: {e}")
        time.sleep(5)

# ==========================================
# 3. Render 슬립 방지용 웹 서버 (무료 유지)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running 24/7!"

def keep_alive():
    time.sleep(30)
    while True:
        try:
            render_url = os.environ.get("RENDER_EXTERNAL_URL")
            if render_url:
                requests.get(render_url, timeout=10)
        except Exception:
            pass
        time.sleep(600)

if __name__ == '__main__':
    t_bot = threading.Thread(target=bot_loop, daemon=True)
    t_bot.start()

    t_keep = threading.Thread(target=keep_alive, daemon=True)
    t_keep.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
