from fyers_apiv3 import fyersModel
import os

from datetime import datetime, timedelta,date
from datetime import time as dt_time
import pandas as pd
import ta
from ta.volatility import AverageTrueRange

import datetime as dt
import pytz

import numpy as np
import math
from scipy.stats import rankdata


fyers_client_id = "VXSQNQ67OI-100"
output_path = os.getcwd() + "/output/"


#values
max_order_qty = 3
tp_atr_multiplier = 2.5
ema_period=50
atr_window = 10
supertrend_window = 10
supertrend_atr_window = 10
supertrend_multiplier = 3

print(f"{output_path  = }")
with open(output_path + 'access.txt') as f:
                access_token = f.read()

fyers = fyersModel.FyersModel(client_id = fyers_client_id, is_async=False, token = access_token, log_path="")
fyers_response = fyers.get_profile()
print(f"{fyers_response = }")

now = datetime.now() + timedelta (days = 9)
next_month_date = now #+ relativedelta(months=1)
current_month = next_month_date.strftime("%y%b").upper()

symbol = f"MCX:NATGASMINI25DECFUT"
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
      #print(f"{response = }")
      if "candles" not in response:
          return None
      #print(f"{response['candles'] = }")
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

def supertrend(df, atr_window = 14 , window=10, multiplier=3):
    df = df.copy()

    df[f'atr_{atr_window}'] = AverageTrueRange(df['high'], df['low'], df['close'], window = atr_window).average_true_range()
    
    hl2 = (df['high'] + df['low']) / 2

    df['upperbasic'] = hl2 + multiplier * df[f'atr_{atr_window}']
    df['lowerbasic'] = hl2 - multiplier * df[f'atr_{atr_window}']
    df['upperband'] = df['upperbasic'].copy()
    df['lowerband'] = df['lowerbasic'].copy()

    for i in range(1, len(df)):
        # Upper band propagation
        if df['upperbasic'][i] < df['upperband'][i-1] or df['close'][i-1] > df['upperband'][i-1]:
            df.loc[i, 'upperband'] = df['upperbasic'][i]
        else:
            df.loc[i, 'upperband'] = df['upperband'][i-1]

        # Lower band propagation
        if df['lowerbasic'][i] > df['lowerband'][i-1] or df['close'][i-1] < df['lowerband'][i-1]:
            df.loc[i, 'lowerband'] = df['lowerbasic'][i]
        else:
            df.loc[i, 'lowerband'] = df['lowerband'][i-1]

    # SuperTrend calculation
    df['supertrend'] = 0.0
    df['st_direction'] = ""
    for i in range(1, len(df)):
        prev_st = df['supertrend'][i-1]

        # Determine direction using previous supertrend (TradingView logic)
        if df['close'][i] > prev_st:
            df.loc[i, 'st_direction'] = "buy"
            df.loc[i, 'supertrend'] = df['lowerband'][i]
        else:
            df.loc[i, 'st_direction'] = "sell"
            df.loc[i, 'supertrend'] = df['upperband'][i]

    df.drop(columns=[f'atr_{atr_window}', 'upperbasic', 'lowerbasic' , 'upperband', 'lowerband'], errors='ignore', inplace=True)     
    return df


history_df = fetch_candles(symbol, resolution = "60" , duration = 100)

if history_df is not None:
    
    history_df[f'ema_{ema_period}'] = history_df['close'].ewm(span=ema_period, adjust=False, min_periods=30).mean()
    history_df['atr'] = AverageTrueRange(history_df['high'], history_df['low'], history_df['close'], window = atr_window).average_true_range()
    history_df = supertrend(history_df, atr_window = supertrend_atr_window , window = supertrend_window, multiplier = supertrend_multiplier)

    history_df = history_df[:-12]
    
    prev_open = round(float(history_df['open'].iloc[-2]),2)
    prev_high = round(float(history_df['high'].iloc[-2]),2)
    prev_low = round(float(history_df['low'].iloc[-2]),2)
    prev_close = round(float(history_df['close'].iloc[-2]),2)

    curr_open = round(float(history_df['open'].iloc[-1]),2)
    curr_high = round(float(history_df['high'].iloc[-1]),2)
    curr_low = round(float(history_df['low'].iloc[-1]),2)
    curr_close = round(float(history_df['close'].iloc[-1]),2)
    
    ema = round(float(history_df[f'ema_{ema_period}'].iloc[-1]),2)
    atr = round(float(history_df['atr'].iloc[-1]),2)
    st_value = round(float(history_df['supertrend'].iloc[-1]),2)
    
    print(f" {symbol = } {history_df.tail(15) = }\n{curr_close = } {ema = } {atr = } {st_value = }")

    history_df.to_csv("history_df.csv",index= False)

    open_qty = get_current_position(symbol)

    ema_buy_condition  = curr_close >= ema and prev_close < ema #inorder to hold the positions if prev_close equals to ema
    ema_short_condition = curr_close <= ema and prev_close > ema  #inorder to hold the positions if prev_close equals to ema
    st_direction = history_df['st_direction'].iloc[-1]

    ema_direction = None
    if ema_buy_condition :
        ema_direction = "buy"
    elif ema_short_condition :
        ema_direction = "sell"
        
    print(f"{open_qty = }, {ema_buy_condition = }, {ema_short_condition = } {ema_direction = } {st_direction = }")


    '''if :
        place_order(symbol, curr_close, open_qty ,side)'''
    





