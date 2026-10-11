import os
import time
import json
import requests
import pandas as pd
import websocket
import signal
from threading import Thread
from flask import Flask

# ==========================================
# [불변 헌법] 지표 및 시스템 기본 명세
# ==========================================
SYMBOL = "ETHUSDT"
INTERVAL = "15m"
BB_PERIOD = 45
BB_STD = 2
ALERT_COOLDOWN = 60

# ==========================================
# [절대 규칙] 환경변수 완벽 격리 (Zero Hardcoding)
# ==========================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL")

app = Flask(__name__)

# ==========================================
# 전역 상태 변수 (웹소켓 단일 스레드 전용)
# ==========================================
df_klines = pd.DataFrame()
prev_above_upper = None
prev_below_lower = None
last_alert_times = {
    "상단돌파": 0.0,
    "상단리턴": 0.0,
    "하단돌파": 0.0,
    "하단리턴": 0.0
}

# ==========================================
# 1. 공통 유틸리티 및 텔레그램 함수
# ==========================================
@app.route("/")
def home():
    return "WebSocket Bot is running! (V11.24 - Pure Engine)"

def send_telegram(message):
    """단방향 메시지 발송 함수 (블로킹 방지 Timeout 5초)"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("경고: 텔레그램 환경변수 누락으로 발송 생략.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        res = requests.post(url, json=payload, timeout=5)
        if res.status_code != 200:
            print(f"텔레그램 발송 실패 (HTTP {res.status_code}): {res.text}")
    except Exception as e:
        print(f"텔레그램 발송 예외: {e}")

# --- Graceful Shutdown (Render 서버 재시작 감지) ---
def handle_sigterm(signum, frame):
    msg = "🔄 [시스템] Render 인프라 강제 재시작(SIGTERM)이 감지되었습니다. 봇이 곧 재가동됩니다."
    print(msg)
    send_telegram(msg)
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

# ==========================================
# 2. 거래소 API 통신 및 방어 로직
# ==========================================
def fetch_initial_klines():
    """초기 100개 캔들 로드 및 IP 밴 대기 방어 (REST API는 1회만 호출)"""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": SYMBOL, "interval": INTERVAL, "limit": 100}
    try:
        res = requests.get(url, params=params, timeout=10)
        
        # 429(Rate Limit) 또는 418(IP Ban) 감지 시 Retry-After 적용
        if res.status_code in (429, 418):
            retry_after = res.headers.get("Retry-After")
            wait_seconds = int(retry_after) if retry_after else 900
            msg = f"🚨 [IP 밴 방어] 바이낸스 API 제한(HTTP {res.status_code}). {wait_seconds}초 동안 안전 동면(Sleep) 진입합니다."
            print(msg)
            send_telegram(msg)
            time.sleep(wait_seconds) # 밴 해제 시간까지 완벽히 대기
            return None 
            
        if res.status_code != 200:
            print(f"초기 캔들 로드 실패 (HTTP {res.status_code}): {res.text}")
            return None
            
        data = res.json()
        if not isinstance(data, list):
            return None
        
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

# ==========================================
# 3. 핵심 지표 판정 엔진 (불변 헌법 100% 보존)
# ==========================================
def process_websocket_message(message):
    global df_klines, prev_above_upper, prev_below_lower, last_alert_times

    try:
        data = json.loads(message)
        kline = data.get("k", {})
        if not kline:
            return

        current_price = float(kline.get("c", 0))
        current_time = int(kline.get("t", 0))

        if df_klines.empty:
            return

        # 캔들 갱신 및 슬라이싱 (동시성 꼬임 원천 제거됨)
        last_index = df_klines.index[-1]
        if current_time == df_klines.at[last_index, "open_time"]:
            df_klines.at[last_index, "close"] = current_price
        elif current_time > df_klines.at[last_index, "open_time"]:
            new_row = {"open_time": current_time, "close": current_price}
            df_klines = pd.concat([df_klines, pd.DataFrame([new_row])], ignore_index=True)
            if len(df_klines) > 100:
                df_klines = df_klines.iloc[-100:].reset_index(drop=True)

        if len(df_klines) >= BB_PERIOD:
            # 연산 범위 및 모표준편차 적용 (바이낸스 수식 일치)
            df_calc = df_klines.iloc[-BB_PERIOD:].copy()
            ma = float(df_calc["close"].mean())
            std = float(df_calc["close"].std(ddof=0))
            upper_band = ma + (BB_STD * std)
            lower_band = ma - (BB_STD * std)

            curr_above_upper = (current_price >= upper_band)
            curr_below_lower = (current_price <= lower_band)

            if prev_above_upper is None: prev_above_upper = curr_above_upper
            if prev_below_lower is None: prev_below_lower = curr_below_lower

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
        print(f"WebSocket 메시지 처리 중 내부 에러: {e}")

def on_message(ws, message):
    process_websocket_message(message)

def on_error(ws, error):
    print(f"WebSocket 통신 에러 발생: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket 연결 종료. (Code: {close_status_code})")
    send_telegram(f"⚠️ {SYMBOL} 실시간 웹소켓 끊김. 봇이 재연결 루프에 진입합니다.")

def on_open(ws):
    print("WebSocket 연결 성공. 실시간 스트림 감시 시작...")
    send_telegram(f"🚀 V11.24 가동 시작 (단일 엔진, {SYMBOL} {INTERVAL} BB({BB_PERIOD},{BB_STD}) 감시 중)")

def start_websocket():
    global df_klines
    
    df_klines = fetch_initial_klines()
    while df_klines is None or df_klines.empty:
        print("초기 캔들 로드 실패. 10초 후 재시도...")
        time.sleep(10)
        df_klines = fetch_initial_klines()
        
    print(f"초기 캔들 {len(df_klines)}개 확보. 웹소켓 연결 준비...")

    stream_url = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@kline_{INTERVAL}"
    backoff_time = 5
    
    # [핵심 교정 로직] 불사의 방어막 (전면적 예외 처리)
    while True:
        try:
            ws = websocket.WebSocketApp(
                stream_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.on_open = on_open
            
            # 60초마다 핑을 쏘고 10초 내 무응답 시 즉시 소켓 파괴
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except Exception as e:
            print(f"웹소켓 구동 중 치명적 예외 발생 (스레드 보호됨): {e}")
        
        # [IP 밴 방어] 지수 백오프(Exponential Backoff) 재접속 대기
        print(f"웹소켓 드랍 감지. {backoff_time}초 후 재접속 타진...")
        time.sleep(backoff_time)
        backoff_time = min(backoff_time * 2, 60)

# ==========================================
# 4. Render 생명 유지 스레드 (격리됨)
# ==========================================
def keep_alive():
    """헌법 필수 항목: Render 동면 방지용 독립 외부 핑 통신망"""
    time.sleep(5)
    while True:
        if RENDER_EXTERNAL_URL:
            try:
                res = requests.get(RENDER_EXTERNAL_URL, timeout=10)
                print(f"[Keep-Alive] 핑 상태: {res.status_code}")
            except Exception as e:
                print(f"[Keep-Alive] 핑 실패: {e}")
                
        time.sleep(600) # 10분 간격

# ==========================================
# 5. 애플리케이션 진입점
# ==========================================
if __name__ == "__main__":
    t1 = Thread(target=start_websocket)
    t1.daemon = True
    t1.start()

    t2 = Thread(target=keep_alive)
    t2.daemon = True
    t2.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
