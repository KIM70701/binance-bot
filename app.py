import os
import time
import requests
import pandas as pd
from threading import Thread
from flask import Flask

# ----------------- 실전 운용 설정 -----------------
SYMBOL = "ETHUSDT"
INTERVAL = "15m"     # 실전 운용 15분봉
BB_PERIOD = 45      # 실전 운용 볼밴 기간 45
BB_STD = 2          # 실전 운용 볼밴 승수 2
POLL_INTERVAL = 3   # 3초 폴링 주기

# 텔레그램 설정
TELEGRAM_BOT_TOKEN = "8866848171:AAH0Jjh18W-XA2eRQIsOG0WFS4YMxmp7ICc"
TELEGRAM_CHAT_ID = "5624306078"
# --------------------------------------------------

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
        # HTTP 상태 코드가 200이 아니거나 JSON 디코딩 실패 시 안전하게 제외
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
    
    # 직전 루프 실시간 가격 위치 추적
    prev_above_upper = None
    prev_below_lower = None

    while True:
        try:
            df = get_klines()
            # 데이터 수신에 성공했을 때만 판정 수행 (실패 시 직전 상태 보존)
            if df is not None and len(df) >= BB_PERIOD:
                df_calc = df.iloc[-BB_PERIOD:].copy()

                # 바이낸스 공식 볼린저 밴드 (실시간 종가 포함 45봉, 모표준편차 ddof=0)
                ma = float(df_calc["close"].mean())
                std = float(df_calc["close"].std(ddof=0))
                upper_band = ma + (BB_STD * std)
                lower_band = ma - (BB_STD * std)

                # 현재 3초 시점의 실시간 현재가
                current_price = float(df_calc.iloc[-1]["close"])

                # 현재 가격의 밴드 경계선 위치 판정
                curr_above_upper = (current_price >= upper_band)
                curr_below_lower = (current_price <= lower_band)

                # 첫 시작 시 현재 위치 동기화
                if prev_above_upper is None:
                    prev_above_upper = curr_above_upper
                if prev_below_lower is None:
                    prev_below_lower = curr_below_lower

                # ---------------- 상단 라인 실시간 교차 감지 ----------------
                # 1. 상단돌파: 안쪽에 있다가 상단선 위로 뚫고 나간 순간
                if not prev_above_upper and curr_above_upper:
                    send_telegram("상단돌파")
                    prev_above_upper = True

                # 2. 상단리턴: 바깥에 있다가 상단선 아래로 뚫고 들어온 순간
                elif prev_above_upper and not curr_above_upper:
                    send_telegram("상단리턴")
                    prev_above_upper = False

                # ---------------- 하단 라인 실시간 교차 감지 ----------------
                # 3. 하단돌파: 안쪽에 있다가 하단선 아래로 뚫고 내려간 순간
                if not prev_below_lower and curr_below_lower:
                    send_telegram("하단돌파")
                    prev_below_lower = True

                # 4. 하단리턴: 바깥에 있다가 하단선 위로 뚫고 올라온 순간
                elif prev_below_lower and not curr_below_lower:
                    send_telegram("하단리턴")
                    prev_below_lower = False

        except Exception as e:
            print(f"모니터링 루프 에러: {e}")

        time.sleep(POLL_INTERVAL)

def keep_alive():
    # Flask 서버가 완전히 바인딩될 때까지 5초 대기
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
