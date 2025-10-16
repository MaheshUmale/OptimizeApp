import os
import urllib.parse
import sqlite3
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from tradingview_screener import Query, col, And, Or
import rookiepy
from pymongo import MongoClient, ReplaceOne
from config import config
from utils.mapping import create_bidirectional_mapping

# --- Global State ---
cookies = None
try:
    cookies = rookiepy.to_cookiejar(rookiepy.brave(['.tradingview.com']))
    print("Successfully loaded TradingView cookies.")
except Exception as e:
    print(f"Warning: Could not load TradingView cookies. Scanning will be disabled. Error: {e}")

# --- Timeframe Configuration ---
timeframes = ['', '|1', '|5', '|15', '|30', '|60', '|120', '|240', '|1W', '|1M']
tf_order_map = {'|1M': 10, '|1W': 9, '|240': 8, '|120': 7, '|60': 6, '|30': 5, '|15': 4, '|5': 3, '|1': 2, '': 1}
tf_display_map = {'': 'Daily', '|1': '1m', '|5': '5m', '|15': '15m', '|30': '30m', '|60': '1H', '|120': '2H', '|240': '4H', '|1W': 'Weekly', '|1M': 'Monthly'}
tf_suffix_map = {v: k for k, v in tf_display_map.items()}

# --- Select Columns for Scanner ---
select_cols = ['name', 'logoid', 'close', 'MACD.hist']
for tf in timeframes:
    select_cols.extend([
        f'KltChnl.lower{tf}', f'KltChnl.upper{tf}', f'BB.lower{tf}', f'BB.upper{tf}',
        f'ATR{tf}', f'SMA20{tf}', f'volume{tf}', f'average_volume_10d_calc{tf}', f'Value.Traded{tf}'
    ])

# --- SQLite Timestamp Handling ---
def adapt_datetime_iso(val):
    return val.isoformat()

def convert_timestamp(val):
    return datetime.fromisoformat(val.decode())

sqlite3.register_adapter(datetime, adapt_datetime_iso)
sqlite3.register_converter("timestamp", convert_timestamp)

# --- Pandas Configuration ---
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 0)

# --- Helper Functions ---
def append_df_to_csv(df, csv_path):
    if not os.path.exists(csv_path):
        df.to_csv(csv_path, mode='a', header=True, index=False)
    else:
        df.to_csv(csv_path, mode='a', header=False, index=False)

def get_highest_squeeze_tf(row):
    for tf_suffix in sorted(tf_order_map, key=tf_order_map.get, reverse=True):
        if row.get(f'InSqueeze{tf_suffix}', False): return tf_display_map[tf_suffix]
    return 'Unknown'

def get_dynamic_rvol(row, timeframe_name, tf_suffix_map):
    tf_suffix = tf_suffix_map.get(timeframe_name)
    if tf_suffix is None: return 0
    vol_col, avg_vol_col = f'volume{tf_suffix}', f'average_volume_10d_calc{tf_suffix}'
    volume, avg_volume = row.get(vol_col), row.get(avg_vol_col)
    if pd.isna(volume) or pd.isna(avg_volume) or avg_volume == 0: return 0
    return volume / avg_volume

def get_squeeze_strength(row):
    highest_tf_name = row['highest_tf']
    tf_suffix = tf_suffix_map.get(highest_tf_name)
    if tf_suffix is None: return "N/A"
    bb_upper, bb_lower = row.get(f'BB.upper{tf_suffix}'), row.get(f'BB.lower{tf_suffix}')
    kc_upper, kc_lower = row.get(f'KltChnl.upper{tf_suffix}'), row.get(f'KltChnl.lower{tf_suffix}')
    if any(pd.isna(val) for val in [bb_upper, bb_lower, kc_upper, kc_lower]): return "N/A"
    bb_width, kc_width = bb_upper - bb_lower, kc_upper - kc_lower
    if bb_width == 0: return "N/A"
    sqz_strength = kc_width / bb_width
    if sqz_strength >= 2: return "VERY STRONG"
    elif sqz_strength >= 1.5: return "STRONG"
    elif sqz_strength > 1: return "Regular"
    else: return "N/A"

def process_fired_events(events, tf_order_map, tf_suffix_map):
    if not events: return pd.DataFrame()
    df = pd.DataFrame(events)
    def get_tf_sort_key(display_name):
        suffix = tf_suffix_map.get(display_name, '')
        return tf_order_map.get(suffix, -1)
    df['tf_order'] = df['fired_timeframe'].apply(get_tf_sort_key)
    processed_events = []
    for ticker, group in df.groupby('ticker'):
        highest_tf_event = group.loc[group['tf_order'].idxmax()]
        consolidated_event = highest_tf_event.to_dict()
        consolidated_event['SqueezeCount'] = group['fired_timeframe'].nunique()
        consolidated_event['highest_tf'] = highest_tf_event['fired_timeframe']
        processed_events.append(consolidated_event)
    return pd.DataFrame(processed_events)

def get_fired_breakout_direction(row, fired_tf_name, tf_suffix_map):
    tf_suffix = tf_suffix_map.get(fired_tf_name)
    if not tf_suffix: return 'Neutral'
    close, bb_upper, kc_upper, bb_lower, kc_lower = row.get('close'), row.get(f'BB.upper{tf_suffix}'), row.get(f'KltChnl.upper{tf_suffix}'), row.get(f'BB.lower{tf_suffix}'), row.get(f'KltChnl.lower{tf_suffix}')
    if any(pd.isna(val) for val in [close, bb_upper, kc_upper, bb_lower, kc_lower]): return 'Neutral'
    if close > bb_upper and bb_upper > kc_upper: return 'Bullish'
    elif close < bb_lower and bb_lower < kc_lower: return 'Bearish'
    else: return 'Neutral'

# --- Database Functions ---
def init_db():
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS squeeze_history (id INTEGER PRIMARY KEY AUTOINCREMENT, scan_timestamp TIMESTAMP NOT NULL, ticker TEXT NOT NULL, timeframe TEXT NOT NULL, volatility REAL, rvol REAL, SqueezeCount INTEGER, squeeze_strength TEXT, HeatmapScore REAL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS fired_squeeze_events (id INTEGER PRIMARY KEY AUTOINCREMENT, fired_timestamp TIMESTAMP NOT NULL, ticker TEXT NOT NULL, fired_timeframe TEXT NOT NULL, momentum TEXT, previous_volatility REAL, current_volatility REAL, rvol REAL, HeatmapScore REAL, URL TEXT, logo TEXT, SqueezeCount INTEGER, highest_tf TEXT)''')
    cursor.execute("PRAGMA table_info(fired_squeeze_events)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'confluence' not in columns:
        print("Adding 'confluence' column to 'fired_squeeze_events' table.")
        cursor.execute("ALTER TABLE fired_squeeze_events ADD COLUMN confluence BOOLEAN DEFAULT 0")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_scan_timestamp ON squeeze_history (scan_timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fired_timestamp ON fired_squeeze_events (fired_timestamp)')
    conn.commit()
    conn.close()

def load_previous_squeeze_list_from_db():
    conn = sqlite3.connect(config.DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT MAX(scan_timestamp) FROM squeeze_history')
        last_timestamp = cursor.fetchone()[0]
        if last_timestamp is None: return []
        cursor.execute('SELECT ticker, timeframe, volatility FROM squeeze_history WHERE scan_timestamp = ?', (last_timestamp,))
        return [(row[0], row[1], row[2]) for row in cursor.fetchall()]
    finally: conn.close()

def save_current_squeeze_list_to_db(squeeze_records):
    if not squeeze_records: return
    conn = sqlite3.connect(config.DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    now = datetime.now()
    data_to_insert = [(now, r['ticker'], r['timeframe'], r['volatility'], r.get('rvol'), r.get('SqueezeCount'), r.get('squeeze_strength'), r.get('HeatmapScore')) for r in squeeze_records]
    cursor.executemany('INSERT INTO squeeze_history (scan_timestamp, ticker, timeframe, volatility, rvol, SqueezeCount, squeeze_strength, HeatmapScore) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', data_to_insert)
    conn.commit()
    conn.close()

def save_fired_events_to_db(fired_events_df):
    if fired_events_df.empty: return
    conn = sqlite3.connect(config.DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    now = datetime.now()
    if 'confluence' not in fired_events_df.columns:
        fired_events_df['confluence'] = False
    data_to_insert = [
        (now, row['ticker'], row['fired_timeframe'], row.get('momentum'),
         row.get('previous_volatility'), row.get('current_volatility'),
         row.get('rvol'), row.get('HeatmapScore'), row.get('URL'),
         row.get('logo'), row.get('SqueezeCount'), row.get('highest_tf'),
         bool(row.get('confluence', False)))
        for _, row in fired_events_df.iterrows()
    ]
    cursor = conn.cursor()
    sql = '''
        INSERT INTO fired_squeeze_events (
            fired_timestamp, ticker, fired_timeframe, momentum,
            previous_volatility, current_volatility, rvol, HeatmapScore,
            URL, logo, SqueezeCount, highest_tf, confluence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    cursor.executemany(sql, data_to_insert)
    conn.commit()
    conn.close()

def load_recent_fired_events_from_db():
    conn = sqlite3.connect(config.DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    fifteen_minutes_ago = datetime.now() - timedelta(minutes=15)
    query = "SELECT * FROM fired_squeeze_events WHERE fired_timestamp >= ?"
    df = pd.read_sql_query(query, conn, params=(fifteen_minutes_ago,))
    conn.close()
    return df

def load_all_day_fired_events_from_db():
    conn = sqlite3.connect(config.DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    query = "SELECT * FROM fired_squeeze_events WHERE fired_timestamp >= ? ORDER BY fired_timestamp DESC"
    df = pd.read_sql_query(query, conn, params=(today_start,))
    conn.close()
    if not df.empty and 'fired_timestamp' in df.columns:
        df['fired_timestamp'] = pd.to_datetime(df['fired_timestamp']).apply(lambda x: x.isoformat())
    return df

# --- Main Scanning Logic ---
def generate_heatmap_data(df):
    base_required_cols = ['ticker', 'HeatmapScore', 'SqueezeCount', 'rvol', 'URL', 'logo', 'momentum', 'highest_tf', 'squeeze_strength']
    for c in base_required_cols:
        if c not in df.columns:
            if c == 'momentum': df[c] = 'Neutral'
            elif c in ['highest_tf', 'squeeze_strength']: df[c] = 'N/A'
            else: df[c] = 0
    heatmap_data = []
    for _, row in df.iterrows():
        stock_data = {
            "name": row['ticker'], "value": row['HeatmapScore'], "count": row.get('SqueezeCount', 0),
            "rvol": row['rvol'], "url": row['URL'], "logo": row['logo'], "momentum": row['momentum'],
            "highest_tf": row['highest_tf'], "squeeze_strength": row['squeeze_strength']
        }
        if 'fired_timeframe' in df.columns: stock_data['fired_timeframe'] = row['fired_timeframe']
        if 'fired_timestamp' in df.columns and pd.notna(row['fired_timestamp']):
            stock_data['fired_timestamp'] = row['fired_timestamp'].isoformat()
        if 'previous_volatility' in df.columns: stock_data['previous_volatility'] = row['previous_volatility']
        if 'current_volatility' in df.columns: stock_data['current_volatility'] = row['current_volatility']
        if 'volatility_increased' in df.columns: stock_data['volatility_increased'] = row['volatility_increased']
        heatmap_data.append(stock_data)
    return heatmap_data

def save_squeeze_context_to_mongodb(dfs_dict: dict):
    try:
        mongo_client = MongoClient(config.MONGO_URI)
        db = mongo_client[config.DATABASE_NAME]
        squeeze_context_collection = db[config.SQUEEZE_CONTEXT_COLLECTION_NAME]
    except Exception as e:
        print(f"ERROR connecting to MongoDB in scanner: {e}")
        return

    df_in_squeeze = dfs_dict.get("in_squeeze", pd.DataFrame()).copy()
    df_fired = dfs_dict.get("fired", pd.DataFrame()).copy()

    if not df_in_squeeze.empty:
        df_in_squeeze['is_in_squeeze'] = True
    if not df_fired.empty:
        df_fired['is_in_squeeze'] = False
        df_fired = df_fired[~df_fired['ticker'].isin(df_in_squeeze['ticker'])]

    final_df = pd.concat([df_in_squeeze, df_fired], ignore_index=True)
    if final_df.empty:
        print("Squeeze scan returned no data to save to context DB.")
        return

    context_data_list = generate_heatmap_data(final_df)
    operations = []
    instrument_to_name, name_to_instrument, name_to_tradingSymbol = create_bidirectional_mapping()
    for doc in context_data_list:
        symbol_NSE = str(doc.get('name'))
        if ":" in symbol_NSE:
            symbol = symbol_NSE.split(":")[1]
        else:
            symbol = symbol_NSE
        core_instrument_id = name_to_instrument.get(symbol, symbol_NSE)
        if core_instrument_id.startswith(symbol):
            print(f"Skipping {symbol}: Instrument key mapping failed.")
            continue
        doc['ticker'] = core_instrument_id
        status_row = final_df[final_df['ticker'] == symbol].iloc[0] if symbol in final_df['ticker'].values else None
        if status_row is not None:
            doc['is_in_squeeze'] = bool(status_row.get('is_in_squeeze', False))
        op = ReplaceOne({'ticker': core_instrument_id}, doc, upsert=True)
        operations.append(op)

    if operations:
        try:
            result = squeeze_context_collection.bulk_write(operations, ordered=False)
            print(f"MongoDB context update complete. Saved/Updated {result.upserted_count + result.modified_count} instruments.")
        except Exception as e:
            print(f"ERROR during MongoDB bulk write: {e}")

def run_scan(settings):
    if cookies is None:
        print("Skipping scan because cookies are not loaded.")
        return {"in_squeeze": pd.DataFrame(), "formed": pd.DataFrame(), "fired": pd.DataFrame()}
    try:
        # ... (rest of the run_scan function is the same)
        prev_squeeze_pairs = load_previous_squeeze_list_from_db()
        squeeze_conditions = [And(col(f'BB.upper{tf}') < col(f'KltChnl.upper{tf}'), col(f'BB.lower{tf}') > col(f'KltChnl.lower{tf}')) for tf in timeframes]
        filters = [
            col('is_primary') == True, col('typespecs').has('common'), col('type') == 'stock',
            col('exchange') == settings['exchange'],
            col('close').between(settings['min_price'], settings['max_price']), col('active_symbol') == True,
            col('average_volume_10d_calc|5') > settings['min_volume'], col('Value.Traded|5') > settings['min_value_traded'],
            Or(*squeeze_conditions)
        ]
        query_in_squeeze = Query().select(*select_cols).where2(And(*filters)).set_markets(settings['market'])
        _, df_in_squeeze = query_in_squeeze.get_scanner_data(cookies=cookies)
        print(f"Found {len(df_in_squeeze) if df_in_squeeze is not None else 0} stocks currently in a squeeze.")

        current_squeeze_pairs = []
        df_in_squeeze_processed = pd.DataFrame()
        if df_in_squeeze is not None and not df_in_squeeze.empty:
            for _, row in df_in_squeeze.iterrows():
                for tf_suffix, tf_name in tf_display_map.items():
                    if (row.get(f'BB.upper{tf_suffix}') < row.get(f'KltChnl.upper{tf_suffix}')) and (row.get(f'BB.lower{tf_suffix}') > row.get(f'KltChnl.lower{tf_suffix}')):
                        atr, sma20, bb_upper = row.get(f'ATR{tf_suffix}'), row.get(f'SMA20{tf_suffix}'), row.get(f'BB.upper{tf_suffix}')
                        volatility = (bb_upper - sma20) / atr if pd.notna(atr) and atr != 0 and pd.notna(sma20) and pd.notna(bb_upper) else 0
                        current_squeeze_pairs.append((row['ticker'], tf_name, volatility))

            df_in_squeeze['encodedTicker'] = df_in_squeeze['ticker'].apply(urllib.parse.quote)
            df_in_squeeze['URL'] = "https://in.tradingview.com/chart/N8zfIJVK/?symbol=" + df_in_squeeze['encodedTicker']
            df_in_squeeze['logo'] = df_in_squeeze['logoid'].apply(lambda x: f"https://s3-symbol-logo.tradingview.com/{x}.svg" if pd.notna(x) and x.strip() else '')
            for tf in timeframes:
                df_in_squeeze[f'InSqueeze{tf}'] = (df_in_squeeze[f'BB.upper{tf}'] < df_in_squeeze[f'KltChnl.upper{tf}']) & (df_in_squeeze[f'BB.lower{tf}'] > df_in_squeeze[f'KltChnl.lower{tf}'])
            df_in_squeeze['SqueezeCount'] = df_in_squeeze[[f'InSqueeze{tf}' for tf in timeframes]].sum(axis=1)
            df_in_squeeze['highest_tf'] = df_in_squeeze.apply(get_highest_squeeze_tf, axis=1)
            df_in_squeeze['squeeze_strength'] = df_in_squeeze.apply(get_squeeze_strength, axis=1)
            df_in_squeeze = df_in_squeeze[df_in_squeeze['squeeze_strength'].isin(['STRONG', 'VERY STRONG'])]
            df_in_squeeze['rvol'] = df_in_squeeze.apply(lambda row: get_dynamic_rvol(row, row['highest_tf'], tf_suffix_map), axis=1)
            df_in_squeeze['momentum'] = df_in_squeeze['MACD.hist'].apply(lambda x: 'Bullish' if x > 0 else 'Bearish' if x < 0 else 'Neutral')
            volatility_map = {(ticker, tf): vol for ticker, tf, vol in current_squeeze_pairs}
            df_in_squeeze['volatility'] = df_in_squeeze.apply(lambda row: volatility_map.get((row['ticker'], row['highest_tf']), 0), axis=1)
            df_in_squeeze['HeatmapScore'] = df_in_squeeze['rvol'] * df_in_squeeze['momentum'].map({'Bullish': 1, 'Neutral': 0.5, 'Bearish': -1}) * df_in_squeeze['volatility']

            ticker_data_map = {row['ticker']: row.to_dict() for _, row in df_in_squeeze.iterrows()}
            current_squeeze_records = [{'ticker': t, 'timeframe': tf, 'volatility': v, **ticker_data_map.get(t, {})} for t, tf, v in current_squeeze_pairs]
            df_in_squeeze_processed = df_in_squeeze
        else:
            current_squeeze_records = []

        prev_squeeze_set = {(ticker, tf) for ticker, tf, vol in prev_squeeze_pairs}
        current_squeeze_set = {(r['ticker'], r['timeframe']) for r in current_squeeze_records}
        prev_squeeze_state = {}
        for ticker, tf, _ in prev_squeeze_pairs:
            if ticker not in prev_squeeze_state:
                prev_squeeze_state[ticker] = set()
            prev_squeeze_state[ticker].add(tf)

        formed_pairs = current_squeeze_set - prev_squeeze_set
        df_formed_processed = pd.DataFrame()
        if formed_pairs:
            formed_tickers = list(set(ticker for ticker, tf in formed_pairs))
            df_formed_processed = df_in_squeeze_processed[df_in_squeeze_processed['ticker'].isin(formed_tickers)].copy()

        fired_pairs = prev_squeeze_set - current_squeeze_set
        if fired_pairs:
            fired_tickers = list(set(ticker for ticker, tf in fired_pairs))
            previous_volatility_map = {(ticker, tf): vol for ticker, tf, vol in prev_squeeze_pairs}
            query_fired = Query().select(*select_cols).set_tickers(*fired_tickers)
            _, df_fired = query_fired.get_scanner_data(cookies=cookies)

            if df_fired is not None and not df_fired.empty:
                newly_fired_events = []
                df_fired_map = {row['ticker']: row for _, row in df_fired.iterrows()}
                for ticker, fired_tf_name in fired_pairs:
                    if ticker in df_fired_map:
                        row_data = df_fired_map[ticker]
                        tf_suffix = tf_suffix_map.get(fired_tf_name)
                        if tf_suffix:
                            previous_volatility = previous_volatility_map.get((ticker, fired_tf_name), 0.0) or 0
                            atr, sma20, bb_upper = row_data.get(f'ATR{tf_suffix}'), row_data.get(f'SMA20{tf_suffix}'), row_data.get(f'BB.upper{tf_suffix}')
                            current_volatility = (bb_upper - sma20) / atr if pd.notna(atr) and atr != 0 and pd.notna(sma20) and pd.notna(bb_upper) else 0
                            if current_volatility > previous_volatility:
                                has_confluence = False
                                fired_tf_rank = tf_order_map.get(tf_suffix_map.get(fired_tf_name, ''), -1)
                                if ticker in prev_squeeze_state:
                                    for prev_tf in prev_squeeze_state[ticker]:
                                        prev_tf_rank = tf_order_map.get(tf_suffix_map.get(prev_tf, ''), -1)
                                        if prev_tf_rank > fired_tf_rank:
                                            has_confluence = True
                                            break

                                fired_event = row_data.to_dict()
                                fired_event.update({
                                    'fired_timeframe': fired_tf_name,
                                    'previous_volatility': previous_volatility,
                                    'current_volatility': current_volatility,
                                    'volatility_increased': True,
                                    'fired_timestamp': datetime.now(),
                                    'confluence': has_confluence
                                })
                                newly_fired_events.append(fired_event)
                                print(f"Fired: {ticker} on {fired_tf_name} | Prev Vol: {previous_volatility:.4f}, Curr Vol: {current_volatility:.4f}, Confluence: {has_confluence}")
                if newly_fired_events:
                    df_newly_fired = process_fired_events(newly_fired_events, tf_order_map, tf_suffix_map)
                    df_newly_fired['URL'] = "https://in.tradingview.com/chart/N8zfIJVK/?symbol=" + df_newly_fired['ticker'].apply(urllib.parse.quote)
                    df_newly_fired['logo'] = df_newly_fired['logoid'].apply(lambda x: f"https://s3-symbol-logo.tradingview.com/{x}.svg" if pd.notna(x) and x.strip() else '')
                    df_newly_fired['rvol'] = df_newly_fired.apply(lambda row: get_dynamic_rvol(row, row['highest_tf'], tf_suffix_map), axis=1)
                    df_newly_fired['momentum'] = df_newly_fired.apply(lambda row: get_fired_breakout_direction(row, row['highest_tf'], tf_suffix_map), axis=1)
                    df_newly_fired['squeeze_strength'] = np.where(df_newly_fired['confluence'], 'FIRED (Confluence)', 'FIRED')
                    df_newly_fired['HeatmapScore'] = df_newly_fired['rvol'] * df_newly_fired['momentum'].map({'Bullish': 1, 'Neutral': 0.5, 'Bearish': -1}) * df_newly_fired['current_volatility']
                    save_fired_events_to_db(df_newly_fired)
                    dd_mm_yy = datetime.now().strftime('%d_%m_%y')
                    append_df_to_csv(df_newly_fired, os.path.join('data', f'BBSCAN_FIRED_{dd_mm_yy}.csv'))
                    print(f"Saved {len(df_newly_fired)} newly fired events to the database and CSV.")

        df_recent_fired = load_recent_fired_events_from_db()
        if not df_recent_fired.empty:
            fired_events_list = df_recent_fired.to_dict('records')
            df_recent_fired_processed = process_fired_events(fired_events_list, tf_order_map, tf_suffix_map)
        else:
            df_recent_fired_processed = pd.DataFrame()

        save_current_squeeze_list_to_db(current_squeeze_records)

        scan_results = {
            "in_squeeze": df_in_squeeze_processed,
            "formed": df_formed_processed,
            "fired": df_recent_fired_processed
        }

        save_squeeze_context_to_mongodb(scan_results)

        return scan_results

    except Exception as e:
        print(f"An error occurred during scan: {e}")
        return {"in_squeeze": pd.DataFrame(), "formed": pd.DataFrame(), "fired": pd.DataFrame()}