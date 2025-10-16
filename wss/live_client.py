import asyncio
import json
import logging
import ssl
import websockets
import requests
import pandas as pd
from time import sleep
from datetime import datetime, timedelta, timezone
from collections import deque
import traceback
from typing import List, Dict, Optional, Set
from asyncio import Queue, QueueFull, QueueEmpty
import pytz
import motor.motor_asyncio
import upstox_client
from upstox_client.rest import ApiException
from google.protobuf.json_format import MessageToDict

import MarketDataFeedV3_pb2 as pb
from config import config
from utils.mapping import create_bidirectional_mapping
from utils.market_hours import is_market_open

# --- Global State ---
websocket = None
from utils.data_store import symbol_URL_LOGO_nameDF
BULK_TICK_QUEUE = Queue(maxsize=10000)
BULK_ALERT_QUEUE = Queue(maxsize=10000)
TICKER_STATES: Dict[str, 'TickerState'] = {}
SQUEEZE_CONTEXT: Dict[str, dict] = {}
SQUEEZE_CONTEXT_LOCK = asyncio.Lock()
CURRENTLY_SUBSCRIBED_KEYS: Set[str] = set()
TARGET_SYMBOLS: List[str] = []

instrument_to_name, name_to_instrument, name_to_tradingSymbol = create_bidirectional_mapping()

async def get_latest_target_symbols() -> Set[str]:
    """
    Retrieves the latest target symbols from the squeeze context in MongoDB.
    """
    async with SQUEEZE_CONTEXT_LOCK:
        return set(SQUEEZE_CONTEXT.keys())

class TickerState:
    def __init__(self, ticker_symbol):
        self.ticker = ticker_symbol
        self.current_bar_volume = 0
        self.current_bar_high = -float('inf')
        self.current_bar_low = float('inf')
        self.current_bar_start_price = None
        self.current_bar_end_price = None
        self.current_bar_start_time = None
        self.recent_volumes = deque([0] * config.HISTORY_SIZE, maxlen=config.HISTORY_SIZE)
        self.recent_price_ranges = deque([0] * config.HISTORY_SIZE, maxlen=config.HISTORY_SIZE)
        self.current_bar_time_boundary = None

    def _calculate_averages(self):
        if config.HISTORY_SIZE == 0:
            return 0, 0
        avg_volume = sum(self.recent_volumes) / config.HISTORY_SIZE
        avg_price_range = sum(self.recent_price_ranges) / config.HISTORY_SIZE
        return avg_volume, avg_price_range

    def update_bar(self, tick):
        ltp, ltq, timestamp = tick['ltp'], tick['ltq'], tick['timestamp']
        if self.current_bar_start_price is None:
            self.current_bar_start_time = timestamp
            self.current_bar_start_price = ltp
            self.current_bar_time_boundary = self.current_bar_start_time + timedelta(seconds=config.BAR_INTERVAL_SECONDS)
        self.current_bar_volume += ltq
        self.current_bar_high = max(self.current_bar_high, ltp)
        self.current_bar_low = min(self.current_bar_low, ltp)
        self.current_bar_end_price = ltp

    async def finalize_and_check_bar(self):
        if self.current_bar_start_time is None:
            self._reset_current_bar(datetime.now(timezone.utc), None)
            return False
        bar_price_range = self.current_bar_high - self.current_bar_low
        bar_price_change = abs(self.current_bar_end_price - self.current_bar_start_price)
        self.recent_volumes.append(self.current_bar_volume)
        self.recent_price_ranges.append(bar_price_range)
        avg_volume, avg_range = self._calculate_averages()
        if avg_volume == 0 or avg_range == 0:
            self._reset_current_bar(self.current_bar_time_boundary, self.current_bar_end_price)
            return False

        SymbolName = instrument_to_name.get(self.ticker, self.ticker)
        TradingName = name_to_tradingSymbol.get(SymbolName, SymbolName)
        triggered = False

        volume_spike = self.current_bar_volume >= (avg_volume * config.VOLUME_MULTIPLIER_THRESHOLD) and self.current_bar_volume >= config.MIN_VOLUME_THRESHOLD
        price_signal = (bar_price_range >= (avg_range * config.PRICE_MULTIPLIER_THRESHOLD) or bar_price_change >= (avg_range * config.PRICE_MULTIPLIER_THRESHOLD))

        if volume_spike and price_signal:
            direction = "UP" if self.current_bar_end_price > self.current_bar_start_price else "DOWN"
            bar_data = {
                "ticker": self.ticker, "direction": direction, "volume": self.current_bar_volume,
                "avg_volume": round(avg_volume, 1), "volume_multiplier": round(self.current_bar_volume / avg_volume, 1),
                "price_change": round(bar_price_change, 2), "price_range": round(bar_price_range, 2), "level": "CRITICAL"
            }
            await log_anomaly(self.ticker, TradingName, "BIG_MOVE", bar_data)
            print(f"🔥 BIG MOVE ALERT: {self.ticker} -{SymbolName}- {direction}")
            triggered = True

        acc_volume_high = self.current_bar_volume >= (avg_volume * config.ACCUMULATION_VOLUME_THRESHOLD) and self.current_bar_volume >= config.MIN_VOLUME_THRESHOLD
        acc_price_low = bar_price_change < (avg_range * config.ACCUMULATION_PRICE_RANGE_MAX_MULTIPLIER)

        if acc_volume_high and acc_price_low:
            bar_data = {
                "ticker": self.ticker, "volume": self.current_bar_volume, "avg_volume": round(avg_volume, 1),
                "volume_multiplier": round(self.current_bar_volume / avg_volume, 1),
                "price_change": round(bar_price_change, 2), "level": "WARNING"
            }
            await log_anomaly(self.ticker, TradingName, "ACCUMULATION", bar_data)
            print(f"⚠️ PREP SIGNAL: {self.ticker} - {SymbolName} - ACCUMULATION")
            triggered = True

        self._reset_current_bar(self.current_bar_time_boundary, self.current_bar_end_price)
        return triggered

    def _reset_current_bar(self, new_start_time, last_price):
        self.current_bar_volume = 0
        last_price = last_price if last_price is not None else 0
        self.current_bar_high = last_price
        self.current_bar_low = last_price
        self.current_bar_start_price = last_price
        self.current_bar_end_price = last_price
        self.current_bar_start_time = new_start_time
        if new_start_time:
            self.current_bar_time_boundary = new_start_time + timedelta(seconds=config.BAR_INTERVAL_SECONDS)

async def log_anomaly(instrument_key: str, TradingName:str, alert_type: str, bar: dict):
    async with SQUEEZE_CONTEXT_LOCK:
        squeeze_data = SQUEEZE_CONTEXT.get(instrument_key, {})
        alert_doc = {
            "timestamp": datetime.now(pytz.utc), "ticker": instrument_key, "tradingname": TradingName,
            "alert_type": alert_type, "bar_data": bar, "context_tf": f"{config.BAR_INTERVAL_SECONDS}s",
            "long_term_context": {
                "is_in_squeeze": squeeze_data.get('is_in_squeeze', False),
                "highest_tf_in_squeeze": squeeze_data.get('highest_tf', 'N/A'),
                "squeeze_strength": squeeze_data.get('squeeze_strength', 'N/A'),
                "scanner_rvol": squeeze_data.get('rvol', 0.0)
            }
        }
        try:
            await BULK_ALERT_QUEUE.put(alert_doc)
            print(f"[ALERT] {instrument_key} - {alert_type}. Squeeze TF: {squeeze_data.get('highest_tf', 'N/A')}")
        except QueueFull:
            print("Alert Queue full. Dropping alert.")

def get_market_data_feed_authorize():
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {config.ACCESS_TOKEN}'}
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    try:
        api_response = requests.get(url=url, headers=headers)
        api_response.raise_for_status()
        return api_response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Auth API failed: {e}")
        return None

def decode_protobuf(buffer):
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

async def fetch_market_data():
    global websocket
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    response = get_market_data_feed_authorize()
    async with websockets.connect(response["data"]["authorized_redirect_uri"], ssl=ssl_context) as ws:
        print('Connection established')
        websocket = ws
        await asyncio.sleep(1)
        # Initial subscription will be handled by update_subscriptions_periodically
        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)
            await process_and_queue_tick(decoded_data)

async def process_and_queue_tick(response: pb.FeedResponse):
    if response and response.feeds:
        for instrument_key, feed_data in response.feeds.items():
            if feed_data.HasField("ltpc"):
                ltpc = feed_data.ltpc
                timestamp_dt = datetime.fromtimestamp(ltpc.ltt / 1000)
                tick_document = {
                    "timestamp": timestamp_dt, "ticker": instrument_key.split('|')[-1],
                    "ltp": ltpc.ltp, "ltq": ltpc.ltq, "close_price": ltpc.cp
                }
                try:
                    await analyze_tick_live(tick_document)
                    await BULK_TICK_QUEUE.put(tick_document)
                except QueueFull:
                    print(f"⚠️ Warning: Tick queue is full. Dropping tick for {instrument_key}")

async def analyze_tick_live(tick_document):
    ticker = tick_document['ticker']
    timestamp = tick_document['timestamp']
    if ticker not in TICKER_STATES:
        TICKER_STATES[ticker] = TickerState(ticker)
    state = TICKER_STATES[ticker]
    if state.current_bar_time_boundary and timestamp >= state.current_bar_time_boundary:
        await state.finalize_and_check_bar()
        while state.current_bar_time_boundary and timestamp >= state.current_bar_time_boundary:
            state.current_bar_time_boundary += timedelta(seconds=config.BAR_INTERVAL_SECONDS)
    state.update_bar(tick_document)

async def flush_ticks_to_mongodb(collection, interval):
    while True:
        await asyncio.sleep(interval)
        if BULK_TICK_QUEUE.empty():
            continue
        write_operations = []
        while not BULK_TICK_QUEUE.empty():
            try:
                tick = BULK_TICK_QUEUE.get_nowait()
                write_operations.append(tick)
                BULK_TICK_QUEUE.task_done()
            except QueueEmpty:
                break
        if write_operations:
            try:
                result = await collection.insert_many(write_operations, ordered=False)
                print(f"✅ Flushed {len(result.inserted_ids)} ticks to MongoDB.")
            except Exception as e:
                print(f"❌ MongoDB Bulk Write Error: {e}")

async def flush_alerts_to_mongodb(collection, interval):
    while True:
        await asyncio.sleep(interval)
        if BULK_ALERT_QUEUE.empty():
            continue
        write_operations = []
        while not BULK_ALERT_QUEUE.empty():
            try:
                alert = BULK_ALERT_QUEUE.get_nowait()
                write_operations.append(alert)
                BULK_ALERT_QUEUE.task_done()
            except QueueEmpty:
                break
        if write_operations:
            try:
                result = await collection.insert_many(write_operations, ordered=False)
                print(f"🚨 Flushed {len(result.inserted_ids)} anomalies to MongoDB.")
            except Exception as e:
                print(f"❌ MongoDB Alert Bulk Write Error: {e}")

async def update_squeeze_context_periodically(db: motor.motor_asyncio.AsyncIOMotorDatabase):
    squeeze_collection = db[config.SQUEEZE_CONTEXT_COLLECTION_NAME]
    while True:
        try:
            cursor = squeeze_collection.find({})
            new_squeeze_context = {}
            async for doc in cursor:
                instrument_key = doc.get("ticker")
                if instrument_key:
                    new_squeeze_context[instrument_key] = doc
            async with SQUEEZE_CONTEXT_LOCK:
                global SQUEEZE_CONTEXT
                SQUEEZE_CONTEXT = new_squeeze_context
            print(f"[SYNC] Squeeze Context updated. Total instruments: {len(SQUEEZE_CONTEXT)}")
        except Exception as e:
            print(f"❌ Error during Squeeze Context synchronization: {e}")
            traceback.print_exc()
        await asyncio.sleep(config.SQUEEZE_UPDATE_INTERVAL_SECONDS)

async def update_subscriptions_periodically():
    global CURRENTLY_SUBSCRIBED_KEYS, websocket
    while True:
        if not websocket:
            await asyncio.sleep(5)
            continue
        try:
            async with SQUEEZE_CONTEXT_LOCK:
                desired_keys = set(SQUEEZE_CONTEXT.keys())

            to_subscribe = list(desired_keys - CURRENTLY_SUBSCRIBED_KEYS)
            to_unsubscribe = list(CURRENTLY_SUBSCRIBED_KEYS - desired_keys)

            if to_subscribe or to_unsubscribe:
                print(f"📊 SUBSCRIPTION UPDATE: +{len(to_subscribe)} / -{len(to_unsubscribe)}")
                if to_unsubscribe:
                    await send_subscription_update(to_unsubscribe, "unsub")
                if to_subscribe:
                    await send_subscription_update(to_subscribe, "sub")
                CURRENTLY_SUBSCRIBED_KEYS = desired_keys
        except Exception as e:
            print(f"❌ Error during periodic subscription update: {e}")
        await asyncio.sleep(config.SUBSCRIPTION_UPDATE_INTERVAL_SECONDS)

async def send_subscription_update(instrument_keys: List[str], method: str):
    global websocket
    if not websocket or not instrument_keys:
        return
    try:
        message_data = {"instrumentKeys": instrument_keys}
        if method == "sub":
            message_data["mode"] = "ltpc"
        message = json.dumps({"guid": f"update_{datetime.now().timestamp()}", "method": method, "data": message_data})
        await websocket.send(message.encode('utf-8'))
        print(f"📡 WSS Sent '{method}' message for {len(instrument_keys)} keys.")
    except Exception as e:
        print(f"WSS Subscription Update Error ({method}): {e}")

async def main():
    if is_market_open():
        motor_client = motor.motor_asyncio.AsyncIOMotorClient(config.MONGO_URI)
        db = motor_client[config.DATABASE_NAME]
        ticks_collection = db[config.TICKS_COLLECTION_NAME]
        alerts_collection = db[config.ALERTS_COLLECTION_NAME]
        print(f"Starting WSS Client and Anomaly Detector...")
        await asyncio.gather(
            fetch_market_data(),
            update_subscriptions_periodically(),
            flush_ticks_to_mongodb(ticks_collection, config.FLUSH_INTERVAL_SECONDS),
            flush_alerts_to_mongodb(alerts_collection, config.FLUSH_INTERVAL_SECONDS),
            update_squeeze_context_periodically(db),
        )
    else:
        print("Market is closed. WSS client will not start.")
        sleep(300)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted and shut down.")