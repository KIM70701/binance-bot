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
# 기본 설정
# ==========================================
SYMBOL = "ETHUSDT"
INTERVAL = "15m"
BB_PERIOD = 45
BB_STD = 2
ALERT_COOLDOWN = 60

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL")

app = Flask(__name__)

# ==========================================
# 봇 상태 관리 (초간단/무결점 구조)
# ==========================================
klines_list = []      # 캔들 데이터가 쌓일 리스트
current_state = "INSIDE"  # 3가지 상태: INSIDE, ABOVE_UPPER, BELOW_LOWER
last_alert_times = {
    "상단돌파": 0.0,
    "상단리턴": 0.0,
    "하단돌파": 0.0,
    "하단리턴": 0.0
}

# Render Sleep 방지용 웹서버 라우트
@app.route("/")
def home():
    return "Bot is running! (새로운 순정 엔진 가동 중)"

# 강제 출력 로그 함수 (Render 로그 창에서 실시간으로 즉시 보이게 함)
def log(msg):
    print(msg, flush=True)

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=5)
    except Exception as e:
        log(f"텔레그램 발송 에러: {e}")

# 인프라 재시작 감지
def handle_sigterm(signum, frame):
    log("🔄 [시스템] 서버 재시작 감지됨.")
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

# ==========================================
# 초기 캔들 가져오기 (REST API)
# ==========================================
def fetch_initial_klines():
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": SYMBOL, "interval": INTERVAL, "limit": 100}
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code in (429, 418):
            wait = int(res.headers.get("Retry-After", 900))
            log(f"🚨 IP 제한 감지. {wait}초 대기...")
            time.sleep(wait)
            return []
        
        data = res.json()
        # [ {시간, 종가}, {시간, 종가} ... ] 형태로 가볍게 저장
        return [{"t": int(x[0]), "c": float(x[4])} for x in data]
    except Exception as e:
        log(f"초기 데이터 로드 에러: {e}")
        return []

# ==========================================
# 핵심 판정 엔진 (웹소켓 데이터 수신 시마다 작동)
# ==========================================
def process_websocket_message(message):
    global klines_list, current_state, last_alert_times
    try:
        data = json.loads(message)
        k = data.get("k")
        if not k: 
            return
        
        curr_t = int(k["t"])
        curr_c = float(k["c"])
        
        if not klines_list:
            return
        
        # 1. 캔들 리스트 최신화 (안전한 리스트 방식)
        last_t = klines_list[-1]["t"]
        if curr_t == last_t:
            klines_list[-1]["c"] = curr_c
        elif curr_t > last_t:
            klines_list.append({"t": curr_t, "c": curr_c})
            if len(klines_list) > 100:
                klines_list.pop(0)
                
        # 2. 볼린저 밴드 계산 (최근 45개만 짤라서 계산)
        if len(klines_list) >= BB_PERIOD:
            recent_45 = klines_list[-BB_PERIOD:]
            df = pd.DataFrame(recent_45)
            ma = df["c"].mean()
            std = df["c"].std(ddof=0)
            up = ma + (BB_STD * std)
            dn = ma - (BB_STD * std)
            
            now = time.time()
            alert_msg = None
            
            # 3. 직관적인 3단계 상태 돌파 판정
            if curr_c >= up and current_state != "ABOVE_UPPER":
                current_state = "ABOVE_UPPER"
                alert_msg = "상단돌파"
            elif curr_c <= dn and current_state != "BELOW_LOWER":
                current_state = "BELOW_LOWER"
                alert_msg = "하단돌파"
            elif dn < curr_c < up:
                if current_state == "ABOVE_UPPER":
                    current_state = "INSIDE"
                    alert_msg = "상단리턴"
                elif current_state == "BELOW_LOWER":
                    current_state = "INSIDE"
                    alert_msg = "하단리턴"
                    
            # 4. 알림 발송 및 쿨다운 체크
            if alert_msg:
                if now - last_alert_times[alert_msg] >= ALERT_COOLDOWN:
                    log(f"🎯 [{alert_msg}] 가격: {curr_c} / 밴드상단: {up:.2f} / 하단: {dn:.2f}")
                    send_telegram(alert_msg)
                    last_alert_times[alert_msg] = now
                else:
                    log(f"⏳ [{alert_msg}] 조건 달성 (현재 60초 쿨다운 대기 중)")
                    
    except Exception as e:
        log(f"판정 엔진 내부 에러: {e}")

# ==========================================
# 웹소켓 연결 및 재시작 관리
# ==========================================
def start_websocket():
    global klines_list
    
    klines_list = fetch_initial_klines()
    while not klines_list:
        log("초기 캔들 100개 로드 실패. 10초 후 재시도...")
        time.sleep(10)
        klines_list = fetch_initial_klines()
        
    log(f"초기 캔들 {len(klines_list)}개 확보 완료. 웹소켓 연결 시작.")
    
    stream_url = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@kline_{INTERVAL}"
    backoff = 5
    
    while True:
        try:
            ws = websocket.WebSocketApp(
                stream_url,
                on_message=lambda ws, msg: process_websocket_message(msg),
                on_error=lambda ws, err: log(f"웹소켓 네트워크 에러: {err}"),
                on_close=lambda ws, status_code, msg: log(f"웹소켓 닫힘 (Code: {status_code})")
            )
            ws.on_open = lambda ws: (log("웹소켓 연결 성공! 실시간 감시 시작"), send_telegram(f"🚀 봇 새 엔진으로 가동 시작 ({SYMBOL} 15m BB 감시)"))
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except Exception as e:
            log(f"웹소켓 루프 치명적 에러 (스레드 보호됨): {e}")
        
        log(f"웹소켓 끊김. {backoff}초 후 안전 재접속...")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)

# ==========================================
# 서버 수명 연장 (Keep-Alive)
# ==========================================
def keep_alive():
    time.sleep(5)
    while True:
        if RENDER_EXTERNAL_URL:
            try:
                res = requests.get(RENDER_EXTERNAL_URL, timeout=10)
                log(f"[Keep-Alive] 핑 상태: {res.status_code}")
            except Exception as e:
                log(f"[Keep-Alive] 핑 실패: {e}")
        time.sleep(600)

# ==========================================
# 실행부
# ==========================================
if __name__ == "__main__":
    Thread(target=start_websocket, daemon=True).start()
    Thread(target=keep_alive, daemon=True).start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
