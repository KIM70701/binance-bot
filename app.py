import os
import time
import requests
import pandas as pd
from threading import Thread
from flask import Flask

SYMBOL = "ETHUSDT"
INTERVAL = "15m"
BB_PERIOD = 45
BB_STD = 2
POLL_INTERVAL = 3

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!"

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("경고: Render 환경변수에 TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID가 설정되지 않았습니다.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        res = requests.post(url, json=payload, timeout=5)
        if res.status_code != 200:
            print(f"텔레그램 발송 실패 (HTTP {res.status_code}): {res.text}")
    except Exception as e:
        print(f"텔레그램 발송 예외: {e}")

def get_klines():
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": SYMBOL, "interval": INTERVAL, "limit": BB_PERIOD}
    try:
        res = requests.get(url, params=params, timeout=5)
        if res.status_code != 200:
            print(f"바이낸스 API 응답 이상 (HTTP {res.status_code})")
            return None
        data = res.json()
        if not isinstance(data, list):
            return None
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "q_vol", "trades", "tb_base", "tb_quote", "ignore"
        ])
        df["close"] = df["close"].astype(float)
        return df
    except Exception as e:
        print(f"바이낸스 조회/파싱 에러: {e}")
        return None

def monitor():
    send_telegram(f"시작 ({SYMBOL} {INTERVAL} BB({BB_PERIOD},{BB_STD}) 단순 실시간 선통과 감지 가동)")
    prev_above_upper = None
    prev_below_lower = None

    while True:
        try:
            df = get_klines()
            if df is not None and len(df) >= BB_PERIOD:
                df_calc = df.iloc[-BB_PERIOD:].copy()
                ma = float(df_calc["close"].mean())
                std = float(df_calc["close"].std(ddof=0))
                upper_band = ma + (BB_STD * std)
                lower_band = ma - (BB_STD * std)

                current_price = float(df_calc.iloc[-1]["close"])
                curr_above_upper = (current_price >= upper_band)
                curr_below_lower = (current_price <= lower_band)

                if prev_above_upper is None:
                    prev_above_upper = curr_above_upper
                if prev_below_lower is None:
                    prev_below_lower = curr_below_lower

                # 상단 라인 실시간 교차 감지
                if not prev_above_upper and curr_above_upper:
                    send_telegram("상단돌파")
                    prev_above_upper = True
                elif prev_above_upper and not curr_above_upper:
                    send_telegram("상단리턴")
                    prev_above_upper = False

                # 하단 라인 실시간 교차 감지
                if not prev_below_lower and curr_below_lower:
                    send_telegram("하단돌파")
                    prev_below_lower = True
                elif prev_below_lower and not curr_below_lower:
                    send_telegram("하단리턴")
                    prev_below_lower = False

        except Exception as e:
            print(f"모니터링 루프 에러: {e}")

        time.sleep(POLL_INTERVAL)

def keep_alive():
    time.sleep(5)
    port = os.environ.get("PORT", "10000")
    while True:
        try:
            requests.get(f"http://127.0.0.1:{port}", timeout=5)
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
