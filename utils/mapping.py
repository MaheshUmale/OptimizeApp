import csv
from config import config

def create_bidirectional_mapping():
    """
    Creates two dictionaries for forward and reverse mapping of 'instrument_key' and 'name'.

    Returns:
        tuple: A tuple containing the forward and reverse mapping dictionaries.
    """
    file_path = config.INSTRUMENTS_CSV_PATH
    instrument_to_name = {}
    name_to_instrument = {}
    name_to_tradingSymbol = {}
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                val = row.get('instrument_key').split('|')
                instrument = val[1]
                name = row.get('name')
                tradingSymbol = row.get('tradingsymbol')

                if instrument and name and tradingSymbol:
                    instrument_to_name[instrument] = name
                    name_to_instrument[name] = instrument
                    name_to_tradingSymbol[name] = tradingSymbol

        return instrument_to_name, name_to_instrument, name_to_tradingSymbol

    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None, None, None
    except KeyError:
        print("Error: 'instrument_key' or 'name' column not found in the CSV.")
        return None, None, None