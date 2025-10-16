import asyncio
import json
import logging
# import time
from time import sleep

from datetime import datetime, timedelta, timezone,time
from collections import deque
import traceback
from typing import List, Dict, Optional, Set
from asyncio import QueueEmpty
import pytz 
import websockets
# Async MongoDB driver
import motor.motor_asyncio
from testReport import create_bidirectional_mapping
# --- Strategy Imports (Placeholder - Ensure these are available) ---
# Assuming you have compiled the .proto file and the module is named 'MarketDataFeedV3_pb2'
# NOTE: This line requires you to have compiled your protobuf file.
import MarketDataFeedV3_pb2 as pb 
# Assuming utility function for symbol-to-name mapping
# NOTE: Replace 'from testReport import create_bidirectional_mapping' with your actual utility file/function
 # Import necessary modules
import asyncio
from asyncio import QueueFull,QueueEmpty,Queue
import json
import ssl
import websockets
import requests
from google.protobuf.json_format import MessageToDict

import MarketDataFeedV3_pb2 as pb
import pandas as pd




import upstox_client
from upstox_client.rest import ApiException


# --- CONFIGURATION (WSS & MongoDB) ---
MONGO_URI = "mongodb://localhost:27017/" # Replace with your MongoDB URI
DATABASE_NAME = "upstox_data"
COLLECTION_NAME = "upstox_ticks"
ALERTS_COLLECTION_NAME = "anomaly_alerts"


# NEW: Collection where the Squeeze Scanner (app.py) saves its full results
SQUEEZE_CONTEXT_COLLECTION_NAME = "squeeze_context" 


FLUSH_INTERVAL_SECONDS = 2
QUEUE_MAX_SIZE = 10000 
ALERT_LOG_FILE = "D://py_code_workspace//BREAKOUTSCANNER//static//anomaly_alerts.json" # File for dashboard
SUBSCRIPTION_UPDATE_INTERVAL_SECONDS = 60 # Check BBScan output every 60 seconds
BULK_ALERT_QUEUE = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
global websocket
websocket = None    

# !!! Replace with your actual credentials and instruments !!!
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3NkFGMzUiLCJqdGkiOiI2OGYwMzIxZDMwYTg4YjVmYzFmNDgyMTciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc2MDU3MTkzMywiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzYwNjUyMDAwfQ.lPe3bg5mEahv7YqzKXEW_CCv3YQ7nKI4cze_pEgFFT4"
# FIX: Updated WSS URL to the V3 endpoint
WSS_URL = "wss://api-v2.upstox.com/feed/api/v3/ws"
CSV_FILE = "instruments.csv" # Placeholder for your instrument CSV file
global symbol_URL_LOGO_nameDF 

import os
file_pattern = os.path.join("D://py_code_workspace//BREAKOUTSCANNER", 'BBSCAN_FIRED_*.csv')
symbol_URL_LOGO_nameDF = pd.DataFrame()
# --- EXTERNAL DEPENDENCIES (MOCK/PLACEHOLDERS) ---
import glob
def get_symbols_from_bbscan():
    """
    Reads tickers from the latest BBSCAN file in the project root and maps them
    to instrument keys using the 'instruments.csv' file.

    Args:
        project_root (str): The absolute path to the project's root directory.

    Returns:
        list: A list of instrument keys corresponding to the tickers in the BBSCAN file.
    """
    global symbol_URL_LOGO_nameDF

    try:
        # bbscan_pattern = r".\BBSCAN_FIRED_*.csv"
        


        # Get a list of all files matching the pattern
        list_of_files = glob.glob(file_pattern)

        # Find the latest BBSCAN file based on the pattern

        # Create an empty list to hold the individual DataFrames
        all_dataframes = []

        # Loop through every file, read it, and add it to the list
        for filename in list_of_files:
            print(f"Reading file: {filename}")
            df = pd.read_csv(filename)
            all_dataframes.append(df)

        # Concatenate all the DataFrames in the list into one single DataFrame
        if all_dataframes:
            merged_df = pd.concat(all_dataframes, ignore_index=True)
            print("\nSuccessfully combined all data into a single DataFrame.")
            print(f"Merged DataFrame shape: {merged_df.shape}")
            symbol_URL_LOGO_nameDF= merged_df
            tickers = merged_df['ticker'].unique().tolist()
            print(f"Found {len(tickers)} unique tickers in BBSCAN file.")
            return tickers

        else:
            print("No CSV files found in the directory.")
            
    except FileNotFoundError as e:
        print(f"Error: A required file was not found. {e}")
        return []
    except Exception as e:
        print(f"An error occurred in get_instrument_keys_from_bbscan: {e}")
        return []


def get_instrument_keys_for_symbols(csv_file_path: str, symbol_strings: List[str]) -> Dict[str, Optional[str]]:
    """
    Extracts the instrument_key for a list of symbol strings 
    (e.g., ["NSE:ADANIGREEN", "NSE:RELIANCE"]) from a CSV file.

    Args:
        csv_file_path (str): The path to your instruments CSV file.
        symbol_strings (List[str]): A list of symbols in "EXCHANGE:TRADINGSYMBOL" format.

    Returns:
        Dict[str, Optional[str]]: A dictionary mapping each symbol string to its 
                                   instrument_key. If a key is not found, its value is None.
    """
    try:
        # Step 1: Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Step 2: Extract exchange and trading symbol from the list of symbols
        parsed_symbols = [symbol.split(':', 1) for symbol in symbol_strings]
        exchanges = [s[0].strip() for s in parsed_symbols]
        trading_symbols = [s[1].strip() for s in parsed_symbols]
        
        # In a typical instrument CSV, 'NSE' maps to 'NSE_EQ' for equity.
        # You may need to adapt this logic for other instrument types.
        exchange_map = {exchange: f"{exchange}_EQ" for exchange in set(exchanges)}

        # Create a new DataFrame from your list of symbols to merge with the instrument data.
        symbols_df = pd.DataFrame({
            'exchange': [exchange_map.get(e, e) for e in exchanges],
            'tradingsymbol': trading_symbols
        })
        
        # Step 3: Merge the two DataFrames to find the matching instrument keys
        # A left merge will keep all your original symbols and add the instrument_key
        # if a match is found. Non-matching symbols will have NaN for instrument_key.
        merged_df = pd.merge(symbols_df, df, on=['exchange', 'tradingsymbol'], how='left')

        # Step 4: Map the original symbol back to its instrument_key
        result = {}
        for i, symbol in enumerate(symbol_strings):
            instrument_key = merged_df['instrument_key'].iloc[i]
            result[symbol] = instrument_key if pd.notna(instrument_key) else None
            
        return result
        

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file_path}")
        return {}
    except Exception as e:
        print(f"An error occurred: {e}")
        return {}



# --- DYNAMIC SYMBOL LIST GENERATION FUNCTION ---
async def get_latest_target_symbols() -> Set[str]:
    """
    Retrieves the latest symbols from the BBScan, maps them to instrument keys,
    and returns a set of fully qualified instrument keys (e.g., 'NSE_EQ|INE...').
    """
    global websocket 
    loop = asyncio.get_event_loop()
    list_of_symbols = await loop.run_in_executor(None, get_symbols_from_bbscan)

    # Use lambda to execute the synchronous mapping function in the thread pool
    instrument_keys_map = await loop.run_in_executor(
        None, 
        lambda: get_instrument_keys_for_symbols(CSV_FILE, list_of_symbols)
    )

    # Filter out None values and use the fully qualified key for subscription
    latest_keys = {
        v for v in instrument_keys_map.values() if v is not None
    }
    
    # Store just the ISIN/identifier (part after the '|') for the Anomaly Detector state
    global TARGET_SYMBOLS 
    TARGET_SYMBOLS = [k.split('|')[-1] for k in latest_keys]
    print("KEYS to SUBSCRIBE "+str(len(latest_keys)))
    return latest_keys

# --- STRATEGY CONFIGURATION ---
BAR_INTERVAL_SECONDS = 30
HISTORY_SIZE = 600
VOLUME_MULTIPLIER_THRESHOLD = 7.0
PRICE_MULTIPLIER_THRESHOLD = 7.0   
ACCUMULATION_VOLUME_THRESHOLD = 2.0
ACCUMULATION_PRICE_RANGE_MAX_MULTIPLIER = 0.75 
MIN_VOLUME_THRESHOLD = 1000
# --- GLOBAL STATE ---
BULK_TICK_QUEUE = asyncio.Queue(maxsize=QUEUE_MAX_SIZE) 
TICKER_STATES: Dict[str, 'TickerState'] = {} 


# --- GLOBAL STATE (New State Variable) ---
# Global dictionary to store the latest multi-timeframe squeeze data: 
# Key: instrument_key (str), Value: dict of squeeze details
SQUEEZE_CONTEXT: Dict[str, dict] = {} 
# Lock for thread-safe access to SQUEEZE_CONTEXT
SQUEEZE_CONTEXT_LOCK = asyncio.Lock()


instrument_to_name, name_to_instrument , name_to_tradingSymbol = create_bidirectional_mapping() 
# State to track instruments currently subscribed on the WSS feed (fully qualified keys)
CURRENTLY_SUBSCRIBED_KEYS: Set[str] = set() 
TARGET_SYMBOLS: List[str] = [] # List of ISINs/identifiers for the TickerState

 

 
async def log_anomaly(instrument_key: str, TradingName:str, alert_type: str, bar: dict):
    """
    Logs the detected anomaly to MongoDB, enriched with long-term squeeze context.
    """
    
    # 1. Look up the long-term Squeeze Context
    squeeze_data = {}
    async with SQUEEZE_CONTEXT_LOCK:
        # Retrieve the relevant context for the instrument
        NSE_NAME = instrument_to_name.get(instrument_key)
        squeeze_data = SQUEEZE_CONTEXT.get("NSE:"+NSE_NAME, {})
        print(" SQZ DATA RECEIVED AS ")
        print(squeeze_data)
        # 2. Define a Confluence Filter (Optional but highly recommended)
        # Only alert if the BIG_MOVE/ACCUMULATION happens when the stock is still 
        # in a squeeze on a HIGHER timeframe, indicating early positioning.
        # is_in_higher_tf_squeeze = squeeze_data.get('is_in_squeeze', False)
        highest_tf = squeeze_data.get('highest_tf', 'N/A')
        
        # You can add logic here: e.g., if not is_in_higher_tf_squeeze and alert_type != "BIG_MOVE", then return
        
        # 3. Build the enriched alert document
        alert_doc = {
            "timestamp": datetime.now(pytz.utc),
            "ticker": instrument_key,
            "tradingname":TradingName,
            "alert_type": alert_type,
            "bar_data": bar,
            # --- LONG-TERM CONTEXT ADDITION ---
            "context_tf": f"{BAR_INTERVAL_SECONDS}s",
            "long_term_context": {
                # "is_in_squeeze": is_in_higher_tf_squeeze,
                "highest_tf_in_squeeze": highest_tf,
                "squeeze_strength": squeeze_data.get('squeeze_strength', 'N/A'),
                "scanner_rvol": squeeze_data.get('current_rvol', 0.0)
            }
        }
        
        # Log to the queue (assuming you have an ALERT_QUEUE defined)
        try:
            BULK_ALERT_QUEUE.put_nowait(alert_doc)
            print(f"[ALERT] {instrument_key} - {alert_type}. Squeeze TF: {highest_tf}")
        except QueueFull:
            print("Alert Queue full. Dropping alert.")





# --- ANOMALY DETECTION CLASS ---
class TickerState:
    """Manages the rolling historical data and current bar aggregation for a single stock."""
    def __init__(self, ticker_symbol):
        self.ticker = ticker_symbol
        
        # Current Bar Aggregation
        self.current_bar_volume = 0
        self.current_bar_high = -float('inf')
        self.current_bar_low = float('inf')
        self.current_bar_start_price = None
        self.current_bar_end_price = None
        self.current_bar_start_time = None

        # Historical Rolling Data
        self.recent_volumes = deque([0] * HISTORY_SIZE, maxlen=HISTORY_SIZE)
        self.recent_price_ranges = deque([0] * HISTORY_SIZE, maxlen=HISTORY_SIZE)
        self.current_bar_time_boundary = None
        
    def _calculate_averages(self):
        """Calculates the average volume and price range from the rolling history."""
        if HISTORY_SIZE == 0:
            return 0, 0
            
        avg_volume = sum(self.recent_volumes) / HISTORY_SIZE
        avg_price_range = sum(self.recent_price_ranges) / HISTORY_SIZE
        return avg_volume, avg_price_range

    def update_bar(self, tick):
        """Updates the metrics of the current 1-second bar with a new tick."""
        ltp, ltq, timestamp = tick['ltp'], tick['ltq'], tick['timestamp']

        if self.current_bar_start_price is None:
            self.current_bar_start_time = timestamp
            self.current_bar_start_price = ltp
            self.current_bar_time_boundary = self.current_bar_start_time + timedelta(seconds=BAR_INTERVAL_SECONDS)
            
        self.current_bar_volume += ltq
        self.current_bar_high = max(self.current_bar_high, ltp)
        self.current_bar_low = min(self.current_bar_low, ltp)
        self.current_bar_end_price = ltp

    async def finalize_and_check_bar(self) -> bool:
        """
        Finalizes the current bar, updates history, and checks for all anomalies (1, 2, and 3).
        """
        if self.current_bar_start_time is None:
            self._reset_current_bar(datetime.now(timezone.utc), None)
            return False 
        
        bar_price_range = self.current_bar_high - self.current_bar_low
        bar_price_change = abs(self.current_bar_end_price - self.current_bar_start_price)
        
        self.recent_volumes.append(self.current_bar_volume)
        self.recent_price_ranges.append(bar_price_range)
        
        avg_volume, avg_range = self._calculate_averages()
        
        # Check for zero averages to prevent division by zero
        if avg_volume == 0 or avg_range == 0:
            self._reset_current_bar(self.current_bar_time_boundary, self.current_bar_end_price)
            return False

        kolkata_tz = pytz.timezone('Asia/Kolkata')
        utc_time_base = self.current_bar_start_time.replace(tzinfo=timezone.utc)
        ist_start_time = utc_time_base ##.astimezone(kolkata_tz)
        ist_time_str = ist_start_time.strftime('%H:%M:%S.%f')[:-3]
        
        SymbolName = instrument_to_name.get(self.ticker, self.ticker) 
        TradingName = name_to_tradingSymbol.get(SymbolName, SymbolName)
        triggered = False

        # Strategy 1 & 2: Momentum Ignition / Volatility Breakout Check
        volume_spike = self.current_bar_volume >= (avg_volume * VOLUME_MULTIPLIER_THRESHOLD) and self.current_bar_volume >=MIN_VOLUME_THRESHOLD
        price_signal = (bar_price_range >= (avg_range * PRICE_MULTIPLIER_THRESHOLD) or
                        bar_price_change >= (avg_range * PRICE_MULTIPLIER_THRESHOLD))
        
        if volume_spike and price_signal:
            direction = "UP" if self.current_bar_end_price > self.current_bar_start_price else "DOWN"
            
            # Package all bar data for the log_anomaly function
            bar_data = {
                "ticker": self.ticker, # ISIN/identifier
                "time_ist": ist_time_str,
                "timestamp_utc": self.current_bar_start_time.isoformat(),
                "direction": direction,
                "volume": self.current_bar_volume,
                "avg_volume": round(avg_volume, 1),
                "volume_multiplier": round(self.current_bar_volume / avg_volume, 1),
                "price_change": round(bar_price_change, 2),
                "price_range": round(bar_price_range, 2),
                "level": "CRITICAL"
            }
            # *** NEW AWAIT CALL ***
            await log_anomaly(self.ticker, TradingName,"BIG_MOVE", bar_data)
            print(f"🔥 BIG MOVE ALERT: {self.ticker} -{SymbolName}- {direction} @ {ist_time_str}")
            triggered = True
            
        # Strategy 3: Accumulation/Distribution Prep Signal
        acc_volume_high = self.current_bar_volume >= (avg_volume * ACCUMULATION_VOLUME_THRESHOLD) and self.current_bar_volume >=MIN_VOLUME_THRESHOLD
        acc_price_low = bar_price_change < (avg_range * ACCUMULATION_PRICE_RANGE_MAX_MULTIPLIER)

        if acc_volume_high and acc_price_low:
        
            bar_data = {
                "ticker": self.ticker,
                "time_ist": ist_time_str,
                "timestamp_utc": self.current_bar_start_time.isoformat(),
                "volume": self.current_bar_volume,
                "avg_volume": round(avg_volume, 1),
                "volume_multiplier": round(self.current_bar_volume / avg_volume, 1),
                "price_change": round(bar_price_change, 2),
                "level": "WARNING"
            }
            
            # *** NEW AWAIT CALL ***
            await log_anomaly(self.ticker,TradingName, "ACCUMULATION", bar_data)

            print(f"⚠️ PREP SIGNAL: {self.ticker} - {SymbolName} - ACCUMULATION @ {ist_time_str}")
            triggered = True
            
        self._reset_current_bar(self.current_bar_time_boundary, self.current_bar_end_price)
        
        return triggered

    def _reset_current_bar(self, new_start_time, last_price):
        """Resets the current bar collector for the next interval."""
        self.current_bar_volume = 0
        last_price = last_price if last_price is not None else 0
        self.current_bar_high = last_price
        self.current_bar_low = last_price
        self.current_bar_start_price = last_price
        self.current_bar_end_price = last_price
        self.current_bar_start_time = new_start_time
        if new_start_time:
            self.current_bar_time_boundary = new_start_time + timedelta(seconds=BAR_INTERVAL_SECONDS)


# --- WSS CONNECTION & MESSAGE HANDLERS ---

async def send_subscription_update(instrument_keys: List[str], method: str):
    """Sends a subscription or unsubscription message to the WSS feed."""
    global websocket
    
    # Check if websocket object exists and is open
    if not websocket :
        print(f"Cannot send '{method}' update: WebSocket is not connected or open.")
        return

    if not instrument_keys:
        return

    try:
        # --- FIX: Updated to V3 Payload Structure ---
        message_data = {
            "instrumentKeys": instrument_keys, 
        }
        
        # V3 requires the 'mode' field for subscription messages
        if method == "sub":
            # Using 'ltpc' (Last Traded Price and Change) as the default V3 mode
            message_data["mode"] = "ltpc" 

        message = json.dumps({
            "guid": f"update_{datetime.now().timestamp()}",
            "method": method,
            "data": message_data
        })
        # --- END FIX ---
        print(" Subscribing ... sending req ")
        await websocket.send(json.dumps(message).encode('utf-8'))
        print(f"📡 WSS Sent '{method}' message for {len(instrument_keys)} keys (V3 Mode: {'ltpc' if method=='sub' else 'N/A'}).")
        
    except Exception as e:
        print(f"WSS Subscription Update Error ({method}): {e}")
        
async def update_subscriptions_periodically():
    """Checks for new symbols from BBScan every minute and updates WSS subscriptions."""
    global CURRENTLY_SUBSCRIBED_KEYS, websocket
    
    # Run at startup to get the initial list and subscribe
    first_run = True 

    while True:
        # Only run the BBScan/symbol lookup if the WSS is connected
        if not websocket :
            print("WSS is not connected. Skipping periodic symbol check.")
            await asyncio.sleep(5) # Wait briefly for reconnection
            continue

        try:
            # 1. Get the latest desired list of fully qualified instrument keys
            desired_keys = await get_latest_target_symbols()
            
            # 2. Determine which keys to subscribe and unsubscribe
            to_subscribe = list(desired_keys - CURRENTLY_SUBSCRIBED_KEYS)
            to_unsubscribe = list(CURRENTLY_SUBSCRIBED_KEYS - desired_keys)

            if to_subscribe or to_unsubscribe:
                print(" B4 ")
                print(f"📊 SUBSCRIPTION UPDATE: +{len(to_subscribe)} / -{len(to_unsubscribe)}")
                
                # 3. Send Unsub messages first
                await send_subscription_update(to_unsubscribe, "unsub")
                
                # 4. Send Sub messages
                await send_subscription_update(to_subscribe, "sub")
                
                # 5. Update the tracking state
                CURRENTLY_SUBSCRIBED_KEYS = desired_keys
                
            elif first_run:
                print(f"📡 Initial subscription successful for {len(CURRENTLY_SUBSCRIBED_KEYS)} instruments.")
                
            first_run = False

        except Exception as e:
            print(f"❌ Error during periodic subscription update: {e}")
            
        await asyncio.sleep(SUBSCRIPTION_UPDATE_INTERVAL_SECONDS)

import requests
def get_market_data_feed_authorize_v3():
    """Get authorization for market data feed."""
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    try:
        api_response = requests.get(url=url, headers=headers)
        api_response.raise_for_status()
        return api_response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Auth API failed: {e}")
        return None
import ssl


def get_market_data_feed_authorize_v3():
    """Get authorization for market data feed."""
    access_token = ACCESS_TOKEN
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    api_response = requests.get(url=url, headers=headers)
    return api_response.json()


def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


async def fetch_market_data():
    """Fetch market data using WebSocket and print it."""
    # print(" STARTING FETCH MARKET DATA --------------------------")
    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    global websocket
    # Get market data feed authorization
    response = get_market_data_feed_authorize_v3()
    # Connect to the WebSocket with SSL context
    async with websockets.connect(response["data"]["authorized_redirect_uri"], ssl=ssl_context) as WEBSOCKET:
        print('Connection established')
        websocket = WEBSOCKET
        await asyncio.sleep(1)  # Wait for 1 second
        desired_keys = await get_latest_target_symbols()
        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "ltpc",
                "instrumentKeys": list(desired_keys)
            }
        }

        # Convert data to binary and send over WebSocket
        binary_data = json.dumps(data).encode('utf-8')
        await WEBSOCKET.send(binary_data)

        # Continuously receive and decode data from WebSocket
        while True:
            message = await WEBSOCKET.recv()
            decoded_data = decode_protobuf(message)
            await process_and_queue_tick(decoded_data)

            # --- 2. Tick Processing and Queuing (The Producer) ---
async def process_and_queue_tick(response: pb.FeedResponse):
    """Extracts required data from Protobuf and puts it into the async queue."""
    if response and response.feeds:
        for instrument_key, feed_data in response.feeds.items():
            
            # We are only interested in LTPC mode ticks
            if feed_data.HasField("ltpc"):
                ltpc = feed_data.ltpc
                
                # Upstox returns LTT in milliseconds, convert to BSON Date (Python datetime)
                timestamp_ms = ltpc.ltt
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000)
                
                # Prepare the document for the MongoDB Time Series Collection
                # Fields must match your Time Series collection setup (timeField: 'timestamp', metaField: 'ticker')
                tick_document = {
                    "timestamp": timestamp_dt,
                    "ticker": instrument_key.split('|')[-1], # Use the ISIN/Symbol as metaField
                    "ltp": ltpc.ltp,
                    "ltq": ltpc.ltq,
                    "close_price": ltpc.cp
                } 
                try:
                    # *** MODIFIED: AWAITING THE ANALYZER ***
                    await analyze_tick_live(tick_document)
                    BULK_TICK_QUEUE.put_nowait(tick_document)

                except asyncio.QueueFull:
                    print(f"⚠️ Warning: Tick queue is full. Dropping tick for {instrument_key}")
                
    
    # Optional: Log Market Status/Snapshot
    if response and response.marketInfo.segmentStatus:
        # Avoid saving this, as it's typically a snapshot/status update
        pass 



            # Convert the decoded data to a dictionary
            # data_dict = MessageToDict(decoded_data)

            # Print the dictionary representation
            # print(json.dumps(data_dict))



async def analyze_tick_live(tick_document):
    """Called immediately upon receiving a tick to run the anomaly detection logic."""
    ticker = tick_document['ticker']
    timestamp = tick_document['timestamp']
    
    if ticker not in TICKER_STATES:
        TICKER_STATES[ticker] = TickerState(ticker)
        
    state = TICKER_STATES[ticker]
    
    # 1. Check if the current 1-second bar is complete
    if state.current_bar_time_boundary and timestamp >= state.current_bar_time_boundary:
        await state.finalize_and_check_bar()
        # Handle case where multiple bars are missed
        while state.current_bar_time_boundary and timestamp >= state.current_bar_time_boundary:
            state.current_bar_time_boundary += timedelta(seconds=BAR_INTERVAL_SECONDS)

    # 2. Update the current bar
    state.update_bar(tick_document)

 

# --- MONGODB PERSISTENCE ---
async def flush_ticks_to_mongodb(collection, interval):
    """Periodically takes ticks from the queue and performs a bulk write to MongoDB."""
    print(f"MongoDB persistence task started. Flush interval: {interval}s")
    
    while True:
        await asyncio.sleep(interval)
        
        if BULK_TICK_QUEUE.empty():
            continue

        write_operations = []
        while True:
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

# --- MONGODB PERSISTENCE ---
# ... (existing flush_ticks_to_mongodb)

async def flush_alerts_to_mongodb(collection, interval):
    """Periodically takes alerts from the queue and performs a bulk write to MongoDB."""
    print(f"MongoDB Alert Flusher task started. Flush interval: {interval}s")
    
    while True:
        await asyncio.sleep(interval)
        
        if BULK_ALERT_QUEUE.empty():
            continue

        write_operations = []
        while True:
            try:
                alert = BULK_ALERT_QUEUE.get_nowait()
                write_operations.append(alert)
                BULK_ALERT_QUEUE.task_done()
            except QueueEmpty:
                break
        
        if write_operations:
            try:
                # Use insert_many for efficiency
                result = await collection.insert_many(write_operations, ordered=False)
                print(f"🚨 Flushed {len(result.inserted_ids)} anomalies to MongoDB.")
            except Exception as e:
                print(f"❌ MongoDB Alert Bulk Write Error: {e}")


def is_market_open():
    # return True
    # Configure the Upstox client
    configuration = upstox_client.Configuration()

    # (Optional) If you have an access token, add it here
    configuration.access_token = ACCESS_TOKEN

    api_instance = upstox_client.MarketHolidaysAndTimingsApi(upstox_client.ApiClient(configuration))

    exchange = "NSE" 

    try:
        """
        NORMAL_OPEN: Indicates the market is currently in a normal trading session.
        NORMAL_CLOSE: Indicates the end of the normal trading session.
        PRE_OPEN_START/END: These indicate phases before the official market opening. 
        """
        # Call the API to get the market status for the specified exchange
        api_response = api_instance.get_market_status(exchange)
        
        # Print the full response
        # print(api_response)
        status = api_response.data.status
        # Check if the market is open
        if status  == "NORMAL_CLOSE":
            print(f"\nMarket on {exchange} is currently CLOSED.")
            return False
        else:
            if status=="NORMAL_OPEN":            
                print(f"\nMarket on {exchange} is currently OPEN.")
                return True
            else:
                print(f"\nMarket on {exchange} is currently PRE-OPEN.")
                return True

    except ApiException as e:
        print("Exception when calling MarketHolidaysAndTimingsApi->get_market_status: %s\n" % e)

    return False





SQUEEZE_UPDATE_INTERVAL_SECONDS =10

# --- SQUEEZE CONTEXT SYNCHRONIZATION ---

async def update_squeeze_context_periodically(db: motor.motor_asyncio.AsyncIOMotorDatabase):
    """
    Periodically pulls the latest squeeze context data from MongoDB 
    and updates the global SQUEEZE_CONTEXT dictionary.
    """
    squeeze_collection = db[SQUEEZE_CONTEXT_COLLECTION_NAME]
    
    print(f"Starting Squeeze Context Sync Task. Checks every {SQUEEZE_UPDATE_INTERVAL_SECONDS} seconds.")
    
    while True:
        try:
            # Fetch all documents in the collection
            cursor = squeeze_collection.find({})
            new_squeeze_context = {}
            
            async for doc in cursor:
                # The document key is the ISIN or instrument identifier
                instrument_key = doc.get("name")
                if instrument_key:
                    # Store the entire document as the context data
                    new_squeeze_context[instrument_key] = doc
                    
            # Update the global context under the lock
            async with SQUEEZE_CONTEXT_LOCK:
                global SQUEEZE_CONTEXT
                SQUEEZE_CONTEXT = new_squeeze_context
            
            print(f"[SYNC] Squeeze Context updated. Total instruments: {len(SQUEEZE_CONTEXT)}")
            
        except Exception as e:
            print(f"❌ Error during Squeeze Context synchronization: {e}")
            traceback.print_exc()

        # Wait for the next update cycle
        await asyncio.sleep(SQUEEZE_UPDATE_INTERVAL_SECONDS)


# --- MAIN EXECUTION ---

async def main():

    if is_market_open():
        print("Current time is between 9:00 AM and 3:30 PM. Performing action...")
         # Initialize Async MongoDB Client (using Motor)
        motor_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        db = motor_client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        alert_collection = db[ALERTS_COLLECTION_NAME]
        print(f"Using Access Token: {ACCESS_TOKEN[:10]}...{ACCESS_TOKEN[-10:]}")
        print(f"Starting the Unified Live WSS Client and Anomaly Detector (Async)...")

            # Start all tasks concurrently: WSS feed listener, dynamic subscription updater, and DB flusher
        await asyncio.gather(
            fetch_market_data(),
            update_subscriptions_periodically(), 
            flush_ticks_to_mongodb(collection, FLUSH_INTERVAL_SECONDS),
            flush_alerts_to_mongodb(alert_collection, FLUSH_INTERVAL_SECONDS), # NEW TASK FOR ALERTS
            update_squeeze_context_periodically(db), # NEW: Start the context synchronization task


        )

    
    else:
        print("Current time is outside the MARKET TIME range. Sleeping...")
        # Calculate time until the next check or until the start time
        # For simplicity, sleeping for 5 minutes here
        sleep(300) # Sleep for 300 seconds (5 minutes)
    

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted and shut down.")
    except RuntimeError as e:
        if "cannot run non-main thread" in str(e):
             print(f"Note: Asyncio main loop may already be running. Error: {e}")
        else:
            raise
