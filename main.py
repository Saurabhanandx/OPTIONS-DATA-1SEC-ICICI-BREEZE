#here we will mount the google drive where we want to store the data
from google.colab import drive
drive.mount('/content/drive')

#package needed to install
pip install breeze-connect


#the actual code
import pandas as pd
from datetime import datetime
from breeze_connect import BreezeConnect
import os
from google.colab import drive

# Step 1: Mount Google Drive
drive.mount('/content/drive')

# Step 2: Initialize Breeze API with credentials
api_key = "Your_api_key"
api_secret = "Your_api_secret_code"
session_token = "XXXXXXX"

breeze = BreezeConnect(api_key=api_key)
breeze.generate_session(api_secret=api_secret, session_token=session_token)

# Function to convert date to UTC format
def to_utc(date, time):
    return f"{date.strftime('%Y-%m-%d')}T{time}.000Z"

# Generate strike prices with intervals
def generate_strike_prices(base_strike, num_strikes=12, interval=50):
    return [base_strike + i * interval for i in range(-num_strikes, num_strikes + 1)]

# Fetch and store data for a specific time range and accumulate into respective DataFrames
def fetch_options_data_for_interval(date, start_time, end_time, strike_price, expiry_date, option_type, accumulated_call_data, accumulated_put_data):
    try:
        from_time = to_utc(date, start_time)
        to_time = to_utc(date, end_time)

        # Fetch historical data from the Breeze API for NIFTY
        res = breeze.get_historical_data_v2(
            interval="1second",
            from_date=from_time,
            to_date=to_time,
            stock_code="NIFTY",
            exchange_code="NFO",  #for sensex we use BFO
            product_type="options",
            expiry_date=expiry_date,
            right=option_type,
            strike_price=strike_price
        )

        if "Success" in res and res["Success"]:
            histdf = pd.DataFrame(res['Success'])

            if option_type == "call":
                accumulated_call_data.append(histdf)
            else:
                accumulated_put_data.append(histdf)
        else:
            print(f"No data found for {option_type} options from {start_time} to {end_time} on {date.strftime('%Y-%m-%d')}")
    except Exception as e:
        print(f"An error occurred on {date.strftime('%Y-%m-%d')} between {start_time} and {end_time}: {e}")

# Main function to fetch data for the full day and store data in date-based folders within Call and Put folders
def fetch_full_day_data(date, base_strike_price, expiry_date):
    # Define time intervals
    time_intervals = [
        ("09:15:00", "09:20:00"),
        ("09:20:00", "10:45:00"),
        ("10:45:00", "11:45:00"),
        ("11:45:00", "12:45:00"),
        ("12:45:00", "13:45:00"),
        ("13:45:00", "14:45:00"),
        ("14:45:00", "15:30:00"),
    ]

    # Generate strike prices around the base strike price
    strike_prices = generate_strike_prices(base_strike_price, num_strikes=12, interval=50)

    # Define the base path for storing data
    month_folder = date.strftime('%B').upper()
    index_folder = "NIFTY"

    # Determine folder name based on whether current_date matches expiry_date
    folder_date = expiry_date.split("T")[0] if date.strftime('%Y-%m-%d') == expiry_date.split("T")[0] else date.strftime('%Y-%m-%d')

    for strike_price in strike_prices:
        print(f"Fetching data for strike price: {strike_price}")

        accumulated_call_data = []
        accumulated_put_data = []

        for start_time, end_time in time_intervals:
            fetch_options_data_for_interval(date, start_time, end_time, strike_price, expiry_date, "call", accumulated_call_data, accumulated_put_data)
            fetch_options_data_for_interval(date, start_time, end_time, strike_price, expiry_date, "put", accumulated_call_data, accumulated_put_data)

        # Define folder paths for Call and Put data
        call_folder_path = os.path.join('/content/drive/My Drive', month_folder, index_folder, folder_date, str(strike_price), 'CE')
        put_folder_path = os.path.join('/content/drive/My Drive', month_folder, index_folder, folder_date, str(strike_price), 'PE')

        # Create the folder structure for Call and Put if not exists
        os.makedirs(call_folder_path, exist_ok=True)
        os.makedirs(put_folder_path, exist_ok=True)

        # Save Call Data
        if accumulated_call_data:
            final_call_df = pd.concat(accumulated_call_data, ignore_index=True)
            final_call_df['datetime'] = pd.to_datetime(final_call_df['datetime'], format='%Y-%m-%d %H:%M:%S')
            final_call_df = final_call_df.drop_duplicates(subset=['datetime'])
            final_call_df.set_index('datetime', inplace=True)
            final_call_df = final_call_df.resample('S').ffill()
            final_call_df.reset_index(inplace=True)
            csv_call_filename = os.path.join(call_folder_path, f"Nifty_{folder_date}_{strike_price}_CE.csv")
            final_call_df.to_csv(csv_call_filename, index=False)
            print(f"Call options data saved to {csv_call_filename}")
        else:
            print(f"No Call data for strike price: {strike_price}")

        # Save Put Data
        if accumulated_put_data:
            final_put_df = pd.concat(accumulated_put_data, ignore_index=True)
            final_put_df['datetime'] = pd.to_datetime(final_put_df['datetime'], format='%Y-%m-%d %H:%M:%S')
            final_put_df = final_put_df.drop_duplicates(subset=['datetime'])
            final_put_df.set_index('datetime', inplace=True)
            final_put_df = final_put_df.resample('S').ffill()
            final_put_df.reset_index(inplace=True)
            csv_put_filename = os.path.join(put_folder_path, f"Nifty_{folder_date}_{strike_price}_PE.csv")
            final_put_df.to_csv(csv_put_filename, index=False)
            print(f"Put options data saved to {csv_put_filename}")
        else:
            print(f"No Put data for strike price: {strike_price}")

# Parameters
current_date = datetime(2024, 3, 7)  # Example date
base_strike_price = 22500  # Example base strike price for Nifty
expiry_date = "2024-03-07T07:00:00.000Z"  # Expiry date for the current week

# Run the data fetch and save into respective folders
fetch_full_day_data(current_date, base_strike_price, expiry_date)
