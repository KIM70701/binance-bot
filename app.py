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

klines_list = []
current_state = "INSIDE"
last_alert_times = {
    "상단돌파": 0.0,
    "상단리턴": 0.0,
    "하단돌파": 0.0,
    "하단리턴": 0.0
}

@app.route("/")
def home():
    return "Bot is running! (15m BB 순정 엔진)"

def log(msg):
    print(msg, flush=True)

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("⚠️ 텔레그램 환경변수가 설정되지 않아 알림 발송을 건너뜁니다.")
        return
    url = "https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=5)
    except Exception as e:
        log(f"텔레그램 발송 에러: {e}")

def handle_sigterm(signum, frame):
    log("🔄 [시스템] 서버 재시작 감지됨.")
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

# ==========================================
# 초기 데이터 로드 (REST API)
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
        return [{"t": int(x[0]), "c": float(x[4])} for x in data]
    except Exception as e:
        log(f"초기 데이터 로드 에러: {e}")
        return []

# ==========================================
# 웹소켓 수신 및 지표 연산 콜백 (명시적 함수)
# ==========================================
def on_message(ws, message):
    global klines_list, current_state, last_alert_times
    try:
        data = json.loads(message)
        
        # 다중 스트림 규격 데이터 추출
        if "data" in data:
            k = data["data"].get("k")
        else:
            k = data.get("k")
            
        if not k:
            return
        
        curr_t = int(k["t"])
        curr_c = float(k["c"])
        
        if not klines_list:
            return
        
        last_t = klines_list[-1]["t"]
        if curr_t == last_t:
            klines_list[-1]["c"] = curr_c
        elif curr_t > last_t:
            klines_list.append({"t": curr_t, "c": curr_c})
            if len(klines_list) > 100:
                klines_list.pop(0)
                
        # 헌법 규격에 맞춘 지표 및 판정식
        if len(klines_list) >= BB_PERIOD:
            df = pd.DataFrame(klines_list[-BB_PERIOD:].copy())
            ma = df["c"].mean()
            std = df["c"].std(ddof=0)
            up = ma + (BB_STD * std)
            dn = ma - (BB_STD * std)
            
            now = time.time()
            alert_msg = None
            
            prev_above_upper = (current_state == "ABOVE_UPPER")
            prev_below_lower = (current_state == "BELOW_LOWER")
            curr_above_upper = (curr_c >= up)
            curr_below_lower = (curr_c <= dn)
            
            if not prev_above_upper and curr_above_upper:
                current_state = "ABOVE_UPPER"
                alert_msg = "상단돌파"
            elif prev_above_upper and not curr_above_upper:
                current_state = "INSIDE"
                alert_msg = "상단리턴"
            elif not prev_below_lower and curr_below_lower:
                current_state = "BELOW_LOWER"
                alert_msg = "하단돌파"
            elif prev_below_lower and not curr_below_lower:
                current_state = "INSIDE"
                alert_msg = "하단리턴"
                    
            if alert_msg:
                if now - last_alert_times[alert_msg] >= ALERT_COOLDOWN:
                    log(f"🎯 [{alert_msg}] 가격: {curr_c} / 밴드상단: {up:.2f} / 하단: {dn:.2f}")
                    send_telegram(alert_msg)
                    last_alert_times[alert_msg] = now
                else:
                    log(f"⏳ [{alert_msg}] 조건 달성 (현재 60초 쿨다운 대기 중)")
                    
    except Exception as e:
        log(f"판정 엔진 내부 에러: {e}")

def on_error(ws, error):
    log(f"웹소켓 네트워크 에러: {error}")

def on_close(ws, close_status_code, close_msg):
    log(f"웹소켓 닫힘 (Code: {close_status_code})")
    send_telegram("⚠️ 웹소켓 연결이 끊어졌습니다. 재접속을 시도합니다.")

def on_open(ws):
    log("웹소켓 연결 성공! 실시간 감시 시작")
    send_telegram("🚀 봇 순정 엔진 가동 시작 (ETHUSDT 15m 감시)")

# ==========================================
# 실행부 및 다중 스트림 연결
# ==========================================
def start_websocket():
    global klines_list
    
    klines_list = fetch_initial_klines()
    while not klines_list:
        log("초기 캔들 100개 로드 실패. 10초 후 재시도...")
        time.sleep(10)
        klines_list = fetch_initial_klines()
        
    log(f"초기 캔들 {len(klines_list)}개 확보 완료. 웹소켓 연결 시작.")
    
    stream_url = "wss://fstream.binance.com/stream?streams=" + SYMBOL.lower() + "@kline_" + INTERVAL
    backoff = 5
    
    while True:
        try:
            ws = websocket.WebSocketApp(
                stream_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open
            )
            ws.run_forever()
        except Exception as e:
            log(f"웹소켓 루프 치명적 에러 (스레드 보호됨): {e}")
        
        log(f"웹소켓 끊김. {backoff}초 후 안전 재접속...")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)

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

if __name__ == "__main__":
    Thread(target=start_websocket, daemon=True).start()
    Thread(target=keep_alive, daemon=True).start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

