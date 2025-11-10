from fyers_apiv3 import fyersModel
import os

from datetime import datetime, timedelta,date
from datetime import time as dt_time
import pandas as pd
import ta

import datetime as dt
import pytz

import numpy as np
import math
from scipy.stats import rankdata


fyers_client_id = "VXSQNQ67OI-100"
output_path = os.getcwd() + "/output/"

print(f"{output_path  = }")
with open(output_path + 'access.txt') as f:
                access_token = f.read()

fyers = fyersModel.FyersModel(client_id = fyers_client_id, is_async=False, token = access_token, log_path="")
fyers_response = fyers.get_profile()
print(f"{fyers_response = }")

now = datetime.now() + timedelta (days = 9)
next_month_date = now #+ relativedelta(months=1)
current_month = next_month_date.strftime("%y%b").upper()

symbol = f"MCX:NATGASMINI25NOVFUT"
total_positions = 1 #total number of positions


# ===== Fetch Candles =====
def fetch_candles(symbol, resolution="1",duration = 10):
    
    start = dt.date.today()-dt.timedelta(duration)
    end= dt.date.today() #- dt.timedelta (duration - 10)

    def round_to_nearest_hour(dt):
      print(f"{dt = }")
      return dt.replace(minute=0, second=0, microsecond=0)

    # Example usage
    now = datetime.now(pytz.timezone('Asia/Kolkata'))
    rounded = int(round_to_nearest_hour(now).timestamp())

    print(f"{start = } {end = } {rounded = }")

    try:

      data = {
          "symbol": symbol,
          "resolution": resolution,
          "date_format": 1,
          "range_from": start,
          "range_to": end,
          "cont_flag": "0"
      }

      response = fyers.history(data)
      print(f"{response = }")
      if "candles" not in response:
          return None
      print(f"{response['candles'] = }")
      df = pd.DataFrame(response["candles"], columns=["timestamp", "open", "high", "low", "close", "volume"])
      df["date"]=pd.to_datetime(df['timestamp'], unit='s')
      df.date=(df.date.dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata'))
      df['date'] = df['date'].dt.tz_localize(None)
      df = df[df['timestamp']!=rounded]
      return df
    except Exception as e:
      print(f"Error {e = }")
      return pd.DataFrame()

# ===== Calculate EMA =====
def calculate_ema(df, period=50):
    #df[f"EMA_{period}"] = ta.trend.EMAIndicator(df["close"], window=period).ema_indicator()
    df[f'EMA_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
    return df

# ===== Order Placement =====
def place_order(symbol, ltp, qty ,side):
    limitPrice = ltp + 0.1 if side == 1 else ltp - 0.1 # 1 = Buy, -1 = Sell
    order_data = {
        "symbol": symbol,
        "qty": qty,
        "type": 1,        # Market order
        "side": side,     # 1 = Buy, -1 = Sell
        "productType": "MARGIN",
        "limitPrice": limitPrice,
        "stopPrice": 0,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
        "orderTag":"tag1"
    }

    print(f"{order_data = }")
    response = fyers.place_order(data = order_data)
    print("Order Response:", response)
    return response

# ===== Check Current Position =====
def get_current_position(symbol):
    pos = fyers.positions()
    print(f"{pos = }")
    if "netPositions" in pos:
        for p in pos["netPositions"]:
            if p["symbol"] == symbol:
                qty = p["netQty"]
                if qty > 0 or qty < 0:
                    return qty   # Long

    return 0  # No position

history_df = fetch_candles(symbol, resolution = "60" , duration = 100)
ltp = round(float(history_df['close'].iloc[-1]),1)
print(f" {symbol = } {history_df.tail(40) = }\n{ltp = }")

if history_df is not None:
    period=50
    history_df[f'EMA_{period}'] = history_df['close'].ewm(span=period, adjust=False, min_periods=30).mean()
    last = history_df.iloc[-1]
    history_df.to_csv("history_df.csv",index= False)


