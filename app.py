import os
import time
import requests
import pandas as pd
from threading import Thread
from flask import Flask

# ----------------- 기본 설정 -----------------
SYMBOL = "ETHUSDT"
INTERVAL = "1m"      # 빠른 테스트용 1분봉 (검증 후 15m 변경 가능)
BB_PERIOD = 10      # 빠른 테스트용 볼밴 기간 10 (기존 45)
BB_STD = 1          # 빠른 테스트용 볼밴 승수 1 (기존 2)
POLL_INTERVAL = 3   # 3초 폴링 주기 (최적 반응성 및 안전한 API 호출)

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
    send_telegram(f"시작 ({SYMBOL} {INTERVAL} BB({BB_PERIOD},{BB_STD}) 실시간 크로스 감지 가동)")
    
    # 직전 루프(3초 전)의 위치 상태 추적 (초기값 None으로 시작 시 자동 동기화)
    prev_upper_outside = None
    prev_lower_outside = None

    while True:
        try:
            df = get_klines()
            if df is not None and len(df) >= BB_PERIOD:
                df_calc = df.iloc[-BB_PERIOD:].copy()

                # 바이낸스 공식 볼린저 밴드 (실시간 종가 포함, 모표준편차 ddof=0)
                ma = float(df_calc["close"].mean())
                std = float(df_calc["close"].std(ddof=0))
                upper_band = ma + (BB_STD * std)
                lower_band = ma - (BB_STD * std)

                current_high = float(df_calc.iloc[-1]["high"])
                current_low = float(df_calc.iloc[-1]["low"])
                current_close = float(df_calc.iloc[-1]["close"])

                # 현재 3초 시점의 이탈 상태 판정
                # 돌파는 순간 꼬리(High/Low) 반영, 리턴은 현재가(Close) 안착 기준
                curr_upper_outside = (current_high >= upper_band)
                curr_lower_outside = (current_low <= lower_band)

                # 첫 실행 시 현재 위치를 즉각 동기화하여 침묵 락 방지
                if prev_upper_outside is None:
                    prev_upper_outside = (current_close >= upper_band)
                if prev_lower_outside is None:
                    prev_lower_outside = (current_close <= lower_band)

                # ---------------- 상단 라인 실시간 크로스 판정 ----------------
                # 1. 상단돌파: 직전에 안쪽에 있다가 현재 상단을 뚫고 나간 순간 (Cross-Over)
                if not prev_upper_outside and curr_upper_outside:
                    send_telegram("상단돌파")
                    prev_upper_outside = True

                # 2. 상단리턴: 직전에 바깥에 있다가 현재가가 상단 안쪽으로 내려앉은 순간 (Cross-Under)
                elif prev_upper_outside and current_close < upper_band:
                    send_telegram("상단리턴")
                    prev_upper_outside = False

                # ---------------- 하단 라인 실시간 크로스 판정 ----------------
                # 3. 하단돌파: 직전에 안쪽에 있다가 현재 하단을 뚫고 내려간 순간 (Cross-Under)
                if not prev_lower_outside and curr_lower_outside:
                    send_telegram("하단돌파")
                    prev_lower_outside = True

                # 4. 하단리턴: 직전에 바깥에 있다가 현재가가 하단 안쪽으로 올라선 순간 (Cross-Over)
                elif prev_lower_outside and current_close > lower_band:
                    send_telegram("하단리턴")
                    prev_lower_outside = False

        except Exception as e:
            print(f"루프 내부 에러: {e}")

        time.sleep(POLL_INTERVAL)

def keep_alive():
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
