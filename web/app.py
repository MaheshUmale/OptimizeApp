import json
import threading
from datetime import datetime
from flask import Flask, render_template, jsonify, request
import pandas as pd
import simplejson
from pymongo import MongoClient

from config import config
from scanner.squeeze_scanner import run_scan, load_all_day_fired_events_from_db, generate_heatmap_data
from utils.data_store import symbol_URL_LOGO_nameDF

class CustomJSONEncoder(simplejson.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

app = Flask(__name__, template_folder='templates')
app.json_encoder = CustomJSONEncoder

auto_scan_enabled = True
latest_scan_dfs = {
    "in_squeeze": pd.DataFrame(),
    "formed": pd.DataFrame(),
    "fired": pd.DataFrame()
}
data_lock = threading.Lock()

try:
    mongo_client = MongoClient(config.MONGO_URI)
    db = mongo_client[config.DATABASE_NAME]
    alerts_collection = db[config.ALERTS_COLLECTION_NAME]
    print("✅ MongoDB connection successful for Flask API.")
except Exception as e:
    print(f"❌ Synchronous MongoDB connection failed for Flask API: {e}")
    db = None
    alerts_collection = None

@app.route('/')
def index():
    return render_template('SqueezeHeatmap.html')

@app.route('/fired')
def fired_page():
    return render_template('Fired.html')

@app.route('/anomaly_dashboard')
def anomaly_dashboard():
    return render_template('anomaly_dashboard.html')

@app.route('/formed')
def formed_page():
    return render_template('Formed.html')

@app.route('/compact')
def compact_page():
    return render_template('CompactHeatmap.html')

@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    if alerts_collection is None:
        return jsonify({"error": "Database not connected"}), 500
    try:
        alerts_cursor = alerts_collection.find({}).sort('timestamp_utc', -1).limit(50)
        alerts_data = list(alerts_cursor)
        for alert in alerts_data:
            symbolName = alert.get('tradingname')
            matching_row = symbol_URL_LOGO_nameDF.loc[symbol_URL_LOGO_nameDF['name'] == symbolName, ['logo', 'URL']]
            if not matching_row.empty:
                logo = matching_row['logo'].iloc[0]
                url = matching_row['URL'].iloc[0]
                if pd.notna(logo) and isinstance(logo, str):
                    alert['logo'] = logo
                if pd.notna(url) and isinstance(url, str):
                    alert['URL'] = url
        return jsonify(json.loads(json.dumps(alerts_data, default=str))), 200
    except Exception as e:
        print(f"❌ Error fetching alerts from MongoDB: {e}")
        return jsonify({"error": f"Failed to fetch alerts: {e}"}), 500

@app.route('/scan', methods=['POST'])
def scan_endpoint():
    rvol_threshold = request.json.get('rvol', 0) if request.json else 0
    with data_lock:
        current_settings = config.SCANNER_SETTINGS.copy()
    scan_results = run_scan(current_settings)
    if rvol_threshold > 0:
        for key in scan_results:
            if not scan_results[key].empty:
                scan_results[key] = scan_results[key][scan_results[key]['rvol'] > rvol_threshold]
    response_data = {
        "in_squeeze": generate_heatmap_data(scan_results["in_squeeze"]),
        "formed": generate_heatmap_data(scan_results["formed"]),
        "fired": generate_heatmap_data(scan_results["fired"])
    }
    return jsonify(response_data)

@app.route('/get_latest_data', methods=['GET'])
def get_latest_data():
    rvol_threshold = request.args.get('rvol', default=0, type=float)
    with data_lock:
        dfs = {
            "in_squeeze": latest_scan_dfs["in_squeeze"].copy(),
            "formed": latest_scan_dfs["formed"].copy(),
            "fired": latest_scan_dfs["fired"].copy()
        }
    if rvol_threshold > 0:
        for key in dfs:
            if not dfs[key].empty:
                dfs[key] = dfs[key][dfs[key]['rvol'] > rvol_threshold]
    response_data = {
        "in_squeeze": generate_heatmap_data(dfs["in_squeeze"]),
        "formed": generate_heatmap_data(dfs["formed"]),
        "fired": generate_heatmap_data(dfs["fired"])
    }
    return jsonify(response_data)

@app.route('/toggle_scan', methods=['POST'])
def toggle_scan():
    global auto_scan_enabled
    data = request.get_json()
    auto_scan_enabled = data.get('enabled', auto_scan_enabled)
    return jsonify({"status": "success", "auto_scan_enabled": auto_scan_enabled})

@app.route('/get_all_fired_events', methods=['GET'])
def get_all_fired_events():
    fired_events_df = load_all_day_fired_events_from_db()
    return jsonify(fired_events_df.to_dict('records'))

@app.route('/update_settings', methods=['POST'])
def update_settings():
    new_settings = request.get_json()
    with data_lock:
        for key, value in new_settings.items():
            if key in config.SCANNER_SETTINGS:
                try:
                    if isinstance(config.SCANNER_SETTINGS[key], int):
                        config.SCANNER_SETTINGS[key] = int(value)
                    elif isinstance(config.SCANNER_SETTINGS[key], float):
                        config.SCANNER_SETTINGS[key] = float(value)
                    else:
                        config.SCANNER_SETTINGS[key] = value
                except (ValueError, TypeError):
                    pass
    return jsonify({"status": "success", "settings": config.SCANNER_SETTINGS})