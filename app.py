import os
import time
import requests
import pandas as pd
from threading import Thread
from flask import Flask

# ----------------- 기본 설정 -----------------
SYMBOL = "ETHUSDT"
INTERVAL = "1m"   # 검증 완료 후 "15m"으로 변경
BB_PERIOD = 45
BB_STD = 2
POLL_INTERVAL = 3  # 폴링 주기 3초

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
    send_telegram(f"시작 ({SYMBOL} {INTERVAL} 4단계 상태머신 정상화 가동)")
    last_candle_time = None
    
    # 캔들별 알림 발송 플래그
    notified_upper_break = False
    notified_upper_return = False
    notified_lower_break = False
    notified_lower_return = False

    while True:
        try:
            df = get_klines()
            # 데이터 수량이 BB_PERIOD 이상이면 최근 BB_PERIOD개만 안전하게 슬라이싱
            if df is not None and len(df) >= BB_PERIOD:
                df_calc = df.iloc[-BB_PERIOD:].copy()
                current_candle_time = df_calc.iloc[-1]["open_time"]

                # 새 봉 시작 감지 (방식 A: 봉 전환 시 모든 알림 상태 리셋)
                if current_candle_time != last_candle_time:
                    last_candle_time = current_candle_time
                    notified_upper_break = False
                    notified_upper_return = False
                    notified_lower_break = False
                    notified_lower_return = False

                # 바이낸스 공식 볼린저 밴드 (실시간 종가 포함 45봉, 모표준편차 ddof=0)
                ma = float(df_calc["close"].mean())
                std = float(df_calc["close"].std(ddof=0))
                upper_band = ma + (BB_STD * std)
                lower_band = ma - (BB_STD * std)

                current_high = float(df_calc.iloc[-1]["high"])
                current_low = float(df_calc.iloc[-1]["low"])
                current_close = float(df_calc.iloc[-1]["close"])

                # ---------------- 상단 라인 판정 ----------------
                # 1. 상단돌파: 캔들 고가가 상단 밴드 이상인 경우 (1회)
                if current_high >= upper_band and not notified_upper_break:
                    send_telegram("상단돌파")
                    notified_upper_break = True

                # 2. 상단리턴: 돌파 후 현재가가 상단 밴드 안쪽으로 회귀한 경우 (1회)
                if notified_upper_break and not notified_upper_return and current_close < upper_band:
                    send_telegram("상단리턴")
                    notified_upper_return = True

                # ---------------- 하단 라인 판정 ----------------
                # 3. 하단돌파: 캔들 저가가 하단 밴드 이하인 경우 (1회)
                if current_low <= lower_band and not notified_lower_break:
                    send_telegram("하단돌파")
                    notified_lower_break = True

                # 4. 하단리턴: 돌파 후 현재가가 하단 밴드 안쪽으로 회귀한 경우 (1회)
                if notified_lower_break and not notified_lower_return and current_close > lower_band:
                    send_telegram("하단리턴")
                    notified_lower_return = True

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
