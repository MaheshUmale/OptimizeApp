import asyncio
import threading
from time import sleep
from web.app import app, auto_scan_enabled, data_lock, latest_scan_dfs
from scanner.squeeze_scanner import run_scan, init_db
from wss.live_client import main as wss_main
from config import config
from utils.market_hours import is_market_open

def background_scanner():
    """Function to run scans in the background."""
    while True:
        if is_market_open():
            print("Market is open. Performing scan...")
            if auto_scan_enabled:
                print("Auto-scanning...")
                with data_lock:
                    current_settings = config.SCANNER_SETTINGS.copy()
                scan_result_dfs = run_scan(current_settings)
                with data_lock:
                    latest_scan_dfs["in_squeeze"] = scan_result_dfs["in_squeeze"]
                    latest_scan_dfs["formed"] = scan_result_dfs["formed"]
                    latest_scan_dfs["fired"] = scan_result_dfs["fired"]
            sleep(60)
        else:
            print("Market is closed. Scanner is sleeping.")
            sleep(300)

def run_flask():
    print("Starting Flask Web Dashboard on http://127.0.0.1:5001/...")
    app.run(debug=False, port=5001, use_reloader=False)

if __name__ == "__main__":
    init_db()

    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    print(f"Starting the Unified Live WSS Client and Anomaly Detector (Async)...")
    try:
        asyncio.run(wss_main())
    except KeyboardInterrupt:
        print("Program interrupted and shut down.")
    except RuntimeError as e:
        if "cannot run" in str(e):
            asyncio.get_event_loop().run_until_complete(wss_main())
        else:
            raise e