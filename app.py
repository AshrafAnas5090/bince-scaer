import asyncio
import json
import os
import aiohttp
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI()

# جلسة الاتصال العامة
http_session: aiohttp.ClientSession = None

class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

current_config = {
    "threshold": 4.0,
    "min_volume": 0.0,
    "mode": "up",
    "timeframe": "15m",
    "calc_base": "open"
}

force_scan_event = asyncio.Event()
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def is_triggered(change_pct: float, volume: float, thresh: float, min_vol: float, mode: str) -> bool:
    if volume < min_vol:
        return False
    if mode == "up" and change_pct >= thresh:
        return True
    elif mode == "down" and change_pct <= -thresh:
        return True
    elif mode == "both" and abs(change_pct) >= thresh:
        return True
    return False

# التحكم بالتزامن لمنع حظر IP من بايننس (أقصى حد 20 طلب متزامن)
semaphore = asyncio.Semaphore(20)

async def fetch_symbol_data(symbol: str, tf: str, calc_base: str, ticker_24h_map: dict):
    async with semaphore:
        try:
            t_data = ticker_24h_map.get(symbol, {})
            high_24h = float(t_data.get("highPrice", 0))
            low_24h = float(t_data.get("lowPrice", 0))

            if tf in ["24h", "1d"]:
                return {
                    "symbol": symbol,
                    "change": float(t_data.get("priceChangePercent", 0)),
                    "price": float(t_data.get("lastPrice", 0)),
                    "volume": float(t_data.get("quoteVolume", 0)),
                    "high_24h": high_24h,
                    "low_24h": low_24h
                }
            else:
                url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={tf}&limit=1"
                async with http_session.get(url, headers=HEADERS, timeout=3.0) as res:
                    if res.status == 200:
                        k = await res.json()
                        k = k[0]
                        open_p = float(k[1])
                        low_p = float(k[3])
                        close_p = float(k[4])
                        volume = float(k[7])

                        base_p = open_p if calc_base == "open" else low_p
                        change_pct = ((close_p - base_p) / base_p) * 100 if base_p > 0 else 0

                        return {
                            "symbol": symbol,
                            "change": change_pct,
                            "price": close_p,
                            "volume": volume,
                            "high_24h": high_24h,
                            "low_24h": low_24h
                        }
        except Exception:
            pass
        return None

async def get_all_24h_map():
    try:
        async with http_session.get("https://fapi.binance.com/fapi/v1/ticker/24hr", headers=HEADERS, timeout=5.0) as res:
            if res.status == 200:
                data = await res.json()
                return {item["symbol"]: item for item in data if item["symbol"].endswith("USDT")}
    except Exception:
        pass
    return {}

async def run_scan_cycle():
    try:
        ticker_map = await get_all_24h_map()
        symbols = list(ticker_map.keys())

        if not symbols:
            return

        tf = current_config["timeframe"]
        thresh = current_config["threshold"]
        min_vol = current_config["min_volume"]
        mode = current_config["mode"]
        calc_base = current_config["calc_base"]

        await manager.broadcast({"type": "scan_start"})

        tasks = [fetch_symbol_data(sym, tf, calc_base, ticker_map) for sym in symbols]
        results = await asyncio.gather(*tasks)

        for res_data in results:
            if not res_data:
                continue

            change_pct = res_data["change"]
            volume = res_data["volume"]
            triggered = is_triggered(change_pct, volume, thresh, min_vol, mode)

            payload = {
                "type": "data",
                "symbol": res_data["symbol"],
                "change": round(change_pct, 2),
                "price": res_data["price"],
                "volume": round(volume, 2),
                "high_24h": res_data["high_24h"],
                "low_24h": res_data["low_24h"],
                "timeframe": tf,
                "triggered": triggered
            }
            await manager.broadcast(payload)

        await manager.broadcast({"type": "scan_complete"})
    except Exception as e:
        print(f"⚠️ خطأ أثناء الفحص: {e}")

async def scanner_loop():
    while True:
        await run_scan_cycle()
        try:
            await asyncio.wait_for(force_scan_event.wait(), timeout=9.0)
            force_scan_event.clear()
        except asyncio.TimeoutError:
            pass

@app.on_event("startup")
async def startup_event():
    global http_session
    connector = aiohttp.TCPConnector(limit=100)
    http_session = aiohttp.ClientSession(connector=connector)
    asyncio.create_task(scanner_loop())

@app.on_event("shutdown")
async def shutdown_event():
    if http_session:
        await http_session.close()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            config = json.loads(data)
            
            if "threshold" in config: current_config["threshold"] = float(config["threshold"])
            if "min_volume" in config: current_config["min_volume"] = float(config["min_volume"])
            if "mode" in config: current_config["mode"] = str(config["mode"])
            if "timeframe" in config: current_config["timeframe"] = str(config["timeframe"])
            if "calc_base" in config: current_config["calc_base"] = str(config["calc_base"])
            
            force_scan_event.set()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ملاحظة: يتم وضع كود HTML_CONTENT هنا كما هو لديك في النص الأصل
HTML_CONTENT = """...""" # تم الاحتفاظ بـ HTML الأصلي كما هو

@app.get("/", response_class=HTMLResponse)
async def get_web_page():
    return HTML_CONTENT

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
