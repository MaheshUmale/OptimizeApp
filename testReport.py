import pandas as pd


import csv


def create_bidirectional_mapping():
    """
    Creates two dictionaries for forward and reverse mapping of 'instrument_key' and 'name'.

     .

    Returns:
        tuple: A tuple containing the forward and reverse mapping dictionaries. return instrument_to_name, name_to_instrument
    """


    # Define the CSV file path
    csv_file = './instruments.csv'

    file_path = csv_file
    # Create empty dictionaries for forward and reverse lookup
    instrument_to_name = {}
    name_to_instrument = {}
    name_to_tradingSymbol = {}
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as csv_file:
            # Use DictReader to read each row as a dictionary
            reader = csv.DictReader(csv_file)
            for row in reader:
                # Get the values from the specified columns
                val =  row.get('instrument_key').split('|')
                # print(val[1])
                instrument =val[1]
                name = row.get('name')
                tradingSymbol = row.get('tradingsymbol')  

                # Ensure both values exist and are not empty
                if instrument and name and tradingSymbol:
                    # Map instrument_key to name
                    instrument_to_name[instrument] = name
                    # Map name to instrument_key
                    name_to_instrument[name] = instrument
                    name_to_tradingSymbol[name]=tradingSymbol
        
        return instrument_to_name, name_to_instrument, name_to_tradingSymbol

    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None, None
    except KeyError:
        print("Error: 'instrument_key' or 'name' column not found in the CSV.")
        return None, None


# Create the mapping dictionaries (from your previous code)
instrument_to_name, name_to_instrument , name_to_tradingSymbol= create_bidirectional_mapping()




def generate_stock_time_report(csv_file_path):
    """
    Reads a CSV file containing stock data and generates a report
    grouped by ticker and date, listing all associated times.

    Args:
        csv_file_path (str): The path to the CSV file.

    Returns:
        list: A list of strings, each representing a report line
              in the format 'STOCK SYMBOL DATE TIME(list)'.
    """
    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: The file '{csv_file_path}' was not found.")
        return []
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return []

    # Ensure the timestamp is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Extract date and time components
    df['date'] = df['timestamp'].dt.strftime('%d/%m/%Y') # Format date as DD/MM/YYYY
    df['time'] = df['timestamp'].dt.strftime('%I.%M %p').str.replace('AM', 'am').str.replace('PM', 'pm') # Format time as HH.MM am/pm

    # df["NAME"]=df[['ticker']    
                  
    df['Stock Symbol'] = df['ticker'].map(instrument_to_name)
    # print(df.head())

    # Group by 'ticker' and 'date' and aggregate 'time' into a list
    # grouped_data = df.groupby(['NAME', 'date'])['time'].apply(list).reset_index()
    # print(grouped_data)
    # # Format the output as requested
    # report_lines = []
    # for index, row in grouped_data.iterrows():
    #     times_str = ', '.join(row['time'])
    #     report_line = f"{row['NAME']} {row['date']} {times_str}"
    #     report_lines.append(report_line)

    # return report_lines
    grouped_data = df.groupby(['Stock Symbol', 'date'])['time'].apply(list).reset_index()
    grouped_data['times'] = grouped_data['time'].apply(lambda x: ', '.join(x))
    
    report_df = grouped_data[['Stock Symbol', 'date', 'times']]
    report_df.columns = ['Stock Symbol', 'Date', 'Time (list)']
    
    return report_df.to_html() #report_df.to_markdown(index=False)

def save_report_to_file(report_content, output_file_path):
    """
    Saves the report string to a specified text file.

    Args:
        report_content (str): The string content to write to the file.
        output_file_path (str): The path and name of the output file.
    """
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        print(f"Report successfully saved to '{output_file_path}'")
    except Exception as e:
        print(f"Error saving file: {e}")


if __name__ == "__main__":
    file_name = "D://py_code_workspace//OutFiles//upstox_data.10Std_TICK.csv" # Ensure this matches your actual CSV file name
# 
    # for item in instrument_to_name:
    #     print(item)
    # report_output_markdownHTML = generate_stock_time_report(file_name)
    # if report_output_markdownHTML:
    #     print("Generated Stock Time Report:")
    #     # print(report_output_markdown)
    #     save_report_to_file(report_output_markdownHTML,"D://py_code_workspace//OutFiles//CheckBreak.html")


