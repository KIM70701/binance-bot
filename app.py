import os
import time
import json
import requests
import pandas as pd
import websocket
from threading import Thread
from flask import Flask

SYMBOL = "ETHUSDT"
INTERVAL = "15m"
BB_PERIOD = 45
BB_STD = 2
ALERT_COOLDOWN = 60

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL")

app = Flask(__name__)

# 전역 변수: 실시간 캔들 데이터프레임과 이전 상태 추적용
df_klines = pd.DataFrame()
prev_above_upper = None
prev_below_lower = None
last_alert_times = {
    "상단돌파": 0.0,
    "상단리턴": 0.0,
    "하단돌파": 0.0,
    "하단리턴": 0.0
}

@app.route("/")
def home():
    return "WebSocket Bot is running!"

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

def fetch_initial_klines():
    """봇 기동 시 단 1회 REST API로 과거 100개 캔들 데이터를 확보"""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": SYMBOL, "interval": INTERVAL, "limit": 100}
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code != 200:
            print(f"초기 캔들 로드 실패 (HTTP {res.status_code}): {res.text}")
            return None
        data = res.json()
        if not isinstance(data, list):
            print("응답 형식 오류: 리스트가 아닙니다.")
            return None
        
        # DataFrame 초기화
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "q_vol", "trades", "tb_base", "tb_quote", "ignore"
        ])
        df["close"] = df["close"].astype(float)
        df["open_time"] = df["open_time"].astype(int)
        df["close_time"] = df["close_time"].astype(int)
        return df
    except Exception as e:
        print(f"초기 캔들 로드 중 예외 발생: {e}")
        return None

def process_websocket_message(message):
    global df_klines, prev_above_upper, prev_below_lower, last_alert_times

    try:
        data = json.loads(message)
        kline = data.get("k", {})
        if not kline:
            return

        is_closed = kline.get("x", False) # 캔들 마감 여부
        current_price = float(kline.get("c", 0))
        current_time = int(kline.get("t", 0))

        if df_klines.empty:
            return

        # 1. 캔들 갱신 로직 (덮어쓰기 또는 새 행 추가)
        last_index = df_klines.index[-1]
        if current_time == df_klines.at[last_index, "open_time"]:
            # 같은 캔들 진행 중: 현재가만 덮어쓰기
            df_klines.at[last_index, "close"] = current_price
        elif current_time > df_klines.at[last_index, "open_time"]:
            # 새로운 캔들 시작: 새 행 추가 (과거 데이터가 너무 커지지 않게 유지)
            new_row = {"open_time": current_time, "close": current_price}
            df_klines = pd.concat([df_klines, pd.DataFrame([new_row])], ignore_index=True)
            if len(df_klines) > 100:
                df_klines = df_klines.iloc[-100:].reset_index(drop=True)

        # 2. 지표 엔진 검증 및 4대 알림 판정 (기존 불변 규칙 완벽 동일)
        if len(df_klines) >= BB_PERIOD:
            df_calc = df_klines.iloc[-BB_PERIOD:].copy()
            ma = float(df_calc["close"].mean())
            std = float(df_calc["close"].std(ddof=0))
            upper_band = ma + (BB_STD * std)
            lower_band = ma - (BB_STD * std)

            curr_above_upper = (current_price >= upper_band)
            curr_below_lower = (current_price <= lower_band)

            if prev_above_upper is None:
                prev_above_upper = curr_above_upper
            if prev_below_lower is None:
                prev_below_lower = curr_below_lower

            now = time.time()

            # 상단 라인 판정
            if not prev_above_upper and curr_above_upper:
                if now - last_alert_times["상단돌파"] >= ALERT_COOLDOWN:
                    send_telegram("상단돌파")
                    last_alert_times["상단돌파"] = now
                print(f"[상단돌파] 현재가: {current_price} >= 상단: {upper_band:.2f}")
                prev_above_upper = True
            elif prev_above_upper and not curr_above_upper:
                if now - last_alert_times["상단리턴"] >= ALERT_COOLDOWN:
                    send_telegram("상단리턴")
                    last_alert_times["상단리턴"] = now
                print(f"[상단리턴] 현재가: {current_price} < 상단: {upper_band:.2f}")
                prev_above_upper = False

            # 하단 라인 판정
            if not prev_below_lower and curr_below_lower:
                if now - last_alert_times["하단돌파"] >= ALERT_COOLDOWN:
                    send_telegram("하단돌파")
                    last_alert_times["하단돌파"] = now
                print(f"[하단돌파] 현재가: {current_price} <= 하단: {lower_band:.2f}")
                prev_below_lower = True
            elif prev_below_lower and not curr_below_lower:
                if now - last_alert_times["하단리턴"] >= ALERT_COOLDOWN:
                    send_telegram("하단리턴")
                    last_alert_times["하단리턴"] = now
                print(f"[하단리턴] 현재가: {current_price} > 하단: {lower_band:.2f}")
                prev_below_lower = False

    except Exception as e:
        print(f"WebSocket 메시지 처리 중 에러: {e}")

def on_message(ws, message):
    process_websocket_message(message)

def on_error(ws, error):
    print(f"WebSocket 에러 발생: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket 연결 종료. (Code: {close_status_code}, Msg: {close_msg})")
    send_telegram(f"경고: {SYMBOL} 웹소켓 연결 끊김. 5초 후 재연결 시도합니다.")

def on_open(ws):
    print(f"WebSocket 연결 성공. 실시간 스트림 수신 시작...")
    send_telegram(f"시작 ({SYMBOL} {INTERVAL} BB({BB_PERIOD},{BB_STD}) 웹소켓 실시간 감지 가동)")

def start_websocket():
    global df_klines
    
    # 1. 초기 과거 캔들 데이터 1회 로드
    df_klines = fetch_initial_klines()
    while df_klines is None or df_klines.empty:
        print("초기 캔들 로드 실패. 10초 후 재시도...")
        time.sleep(10)
        df_klines = fetch_initial_klines()
        
    print(f"초기 캔들 {len(df_klines)}개 로드 완료. 웹소켓 연결 준비 중...")

    # 2. 실시간 웹소켓 연결 유지(재연결 무한 루프)
    stream_url = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@kline_{INTERVAL}"
    
    while True:
        ws = websocket.WebSocketApp(
            stream_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.on_open = on_open
        ws.run_forever()
        # 소켓이 끊어지면 5초 대기 후 재연결
        time.sleep(5)

def keep_alive():
    time.sleep(5)
    if not RENDER_EXTERNAL_URL:
        print("경고: RENDER_EXTERNAL_URL 환경변수가 없어 자체 외부 핑을 생략합니다.")
        return

    while True:
        try:
            res = requests.get(RENDER_EXTERNAL_URL, timeout=10)
            print(f"[Keep-Alive] 외부 핑 전송 ({RENDER_EXTERNAL_URL}) 상태: {res.status_code}")
        except Exception as e:
            print(f"[Keep-Alive] 핑 실패: {e}")
        time.sleep(600)

if __name__ == "__main__":
    # 라이브러리 의존성 체크(선택사항): websocket-client 필요
    # pip install websocket-client pandas requests flask
    
    t1 = Thread(target=start_websocket)
    t1.daemon = True
    t1.start()

    t2 = Thread(target=keep_alive)
    t2.daemon = True
    t2.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
