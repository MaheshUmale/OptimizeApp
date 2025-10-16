from datetime import datetime
import pytz
import upstox_client
from config import config

def is_market_open():
    """
    Checks if the market is open using the Upstox API.
    """
    configuration = upstox_client.Configuration()
    configuration.access_token = config.ACCESS_TOKEN
    api_instance = upstox_client.MarketHolidaysAndTimingsApi(upstox_client.ApiClient(configuration))
    exchange = "NSE"
    try:
        api_response = api_instance.get_market_status(exchange)
        status = api_response.data.status
        if status == "NORMAL_CLOSE":
            print(f"\nMarket on {exchange} is currently CLOSED.")
            return False
        else:
            if status == "NORMAL_OPEN":
                print(f"\nMarket on {exchange} is currently OPEN.")
                return True
            else:
                print(f"\nMarket on {exchange} is currently PRE-OPEN.")
                return True
    except ApiException as e:
        print("Exception when calling MarketHolidaysAndTimingsApi->get_market_status: %s\n" % e)
    return False