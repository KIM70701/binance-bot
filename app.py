import os
import time
import requests
import pandas as pd
from threading import Thread
from flask import Flask

# ----------------- 기본 설정 -----------------
SYMBOL = "ETHUSDT"
INTERVAL = "15m"   # 실시간 대응 시 "1m", 평소엔 "15m"으로 변경
BB_PERIOD = 45
BB_STD = 2

# 텔레그램 설정
TELEGRAM_BOT_TOKEN = "8866848171:AAH0Jjh18W-XA2eRQIsOG0WFS4YMxmp7ICc"
TELEGRAM_CHAT_ID = "5624306078"
# ---------------------------------------------

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!"

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"텔레그램 발송 오류: {e}")

def get_klines():
    # 실시간 미완성봉 포함 45개 데이터 조회
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": BB_PERIOD
    }
    try:
        res = requests.get(url, params=params, timeout=5)
        data = res.json()
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "q_vol", "trades", "tb_base", "tb_quote", "ignore"
        ])
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        return df
    except Exception as e:
        print(f"바이낸스 조회 에러: {e}")
        return None

def monitor():
    send_telegram(f"시작 ({SYMBOL} {INTERVAL} 바이낸스 차트 동기화 가동)")
    last_candle_time = None
    notified_upper = False
    notified_lower = False

    while True:
        try:
            df = get_klines()
            if df is not None and len(df) == BB_PERIOD:
                current_candle_time = df.iloc[-1]["open_time"]

                # 새 봉이 시작되면 알림 여부 초기화
                if current_candle_time != last_candle_time:
                    last_candle_time = current_candle_time
                    notified_upper = False
                    notified_lower = False

                # 바이낸스 차트 기본 볼밴 공식 (실시간 종가 포함 45개, 모표준편차 ddof=0)
                ma = df["close"].mean()
                std = df["close"].std(ddof=0)
                upper_band = ma + (BB_STD * std)
                lower_band = ma - (BB_STD * std)

                current_high = df.iloc[-1]["high"]
                current_low = df.iloc[-1]["low"]

                # 볼린저 밴드 상단 돌파 시
                if current_high >= upper_band and not notified_upper:
                    send_telegram("상단")
                    notified_upper = True

                # 볼린저 밴드 하단 돌파 시
                if current_low <= lower_band and not notified_lower:
                    send_telegram("하단")
                    notified_lower = True

        except Exception as e:
            print(f"루프 내부 에러: {e}")

        time.sleep(5)

def keep_alive():
    while True:
        try:
            requests.get("http://127.0.0.1:10000", timeout=5)
        except Exception:
            pass
        time.sleep(600)

if __name__ == "__main__":
    t1 = Thread(target=monitor)
    t1.daemon = True
    t1.start()

    t2 = Thread(target=keep_alive)
    t2.daemon = True
    t2.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
