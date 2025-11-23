from fyers_apiv3 import fyersModel
import os

from datetime import datetime, timedelta,date
import pandas as pd
import ta
from ta.volatility import AverageTrueRange

import datetime as dt
import pytz

import numpy as np
import math
from scipy.stats import rankdata
import _credentials as cd
import time
from dateutil.relativedelta import relativedelta
import sys
import logging
import requests


fyers_client_id = cd.fyers_client_id
output_path = os.getcwd() + "/output/"
current_time_str = time.strftime("%d %b %Y %H.%M.%S")

#values
max_order_qty = 3
buffer_price = 0.1
tp_atr_multiplier = 2.5
ema_period=50
atr_window = 10
supertrend_window = 10
supertrend_atr_window = 10
supertrend_multiplier = 3
partials_profits = True 

order_status_dict = {
    1: "Cancelled",
    2: "Traded / Filled",
    3: "For future use",
    4: "Transit",
    5: "Rejected",
    6: "Pending"
}

#telegram updates
telegram_bot_updates = False
bot_token = cd.bot_token
chatID = cd.bot_trades_chatID


# =========== Access token ==========

def get_access_token():
    with open(output_path + 'access.txt') as f:
        return f.read()


# ============= logging ==============

logging.basicConfig(
    filename = f"logs/log {current_time_str}.txt",
    level = logging.INFO,
    format = "%(asctime)s  %(message)s"
)

def log(msg):
    print(msg)              # console
    logging.info(msg)       # file

# ============= fyers login ==============

access_token = get_access_token()
fyers = fyersModel.FyersModel(client_id = fyers_client_id, is_async=False, token = access_token, log_path="")
fyers_response = fyers.get_profile()
#log(f"{fyers_response = }")

# ============= Get FutureContract ==============

def get_future_contract() :
    current_date = date.today()
    current_date_str = current_date.strftime("%d-%m-%Y")

    expiry = time.strftime("%y%b").upper()
    next_expiry = (current_date + relativedelta(months=1)).strftime("%y%b").upper()
    symbol = f"MCX:NATGASMINI{expiry}FUT"
    try:
        data = {"symbol": f"{symbol}","strikecount":1,"timestamp": ""}
        fyers_response = fyers.optionchain(data=data)
        #log(f"{fyers_response = }")
        #log(f"{current_date_str = }  {expiry = } {fyers_response['message']}")
        option_chain_expiry = fyers_response['data']['expiryData'][0]['date']
        monthly_expiry_dt = datetime.strptime(option_chain_expiry, "%d-%m-%Y").strftime("%d%m%y")
        option_chain_expiry_str = datetime.strptime(option_chain_expiry, "%d-%m-%Y").strftime("%y%b").upper()
     
        #log(f"{monthly_expiry_dt = } ")
        #log(f"{option_chain_expiry = } {current_date_str = } ",option_chain_expiry == current_date_str)
        if option_chain_expiry == current_date_str:
            symbol = f"MCX:NATGASMINI{next_expiry}FUT"
            #log("both option_chain_expiry and current_date_str are same,{symbol = }")
            raise StopIteration   # exit the loop and also exit the try block
        else:
            symbol = f"MCX:NATGASMINI{option_chain_expiry_str}FUT"
            #log(f"Else part ,{symbol = }")
            return symbol
                  
    except Exception as e:
        #log(f"Moving to next_expiry,error :  {e}")
        symbol = f"MCX:NATGASMINI{next_expiry}FUT"
        return symbol


# ============ Fetch Candles =============

def fetch_candles(symbol, resolution="1",duration = 10):
    start = dt.date.today()-dt.timedelta(duration)
    end= dt.date.today() #- dt.timedelta (duration - 10)

    def round_to_nearest_hour(dt):
      log(f"{dt = }")
      return dt.replace(minute=0, second=0, microsecond=0)

    # Example usage
    now = datetime.now(pytz.timezone('Asia/Kolkata'))
    rounded = int(round_to_nearest_hour(now).timestamp())

    log(f"{start = } {end = } {rounded = }")

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
      #log(f"{response = }")
      if "candles" not in response:
          return None
      #log(f"{response['candles'] = }")
      df = pd.DataFrame(response["candles"], columns=["timestamp", "open", "high", "low", "close", "volume"])
      df["date"]=pd.to_datetime(df['timestamp'], unit='s')
      df.date=(df.date.dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata'))
      df['date'] = df['date'].dt.tz_localize(None)
      df = df[df['timestamp']!=rounded]
      return df
    except Exception as e:
      log(f"Error {e = }")
      return pd.DataFrame()

# ============== Modify Orders ================

def modify_orders(orderId ,limitPrice, qty):
    data = {
        "id":orderId, 
        "type":1, 
        "limitPrice": limitPrice, 
        "qty" : qty
    }

    order_response = fyers.modify_order(data=data)
    if order_response["s"].lower() == "ok":
        log("Order modification successful, {orderId}")
    else:
        log("Order modification failed, {orderId}")

# ============== Order Placement ================

def place_order(trading_symbol, ltp, qty ,side):
    
    limitPrice = ltp + buffer_price if side.lower() == "buy" else ltp - buffer_price # 1 = Buy, -1 = Sell
    side = 1 if side.lower() == "buy" else -1 # 1 = Buy, -1 = Sell
    
    order_data = {
        "symbol": trading_symbol,
        "qty": qty,
        "type": 1,        # 1 Limit order, 2 Market Or
        "side": side,     # 1 = Buy, -1 = Sell
        "productType": "MARGIN",
        "limitPrice": limitPrice,
        "stopPrice": 0,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
        "orderTag":"tag1"
    }

    log(f"{order_data = }")
    
    try:
        order_response = fyers.place_order(data = order_data)
        
        if order_response['s'].lower() == "ok" :
            orderId = order_response['id']

            #getting status of the order
            data = {"id" : orderId}
            order_response = fyers.orderbook(data=data)
            orderBook = order_response['orderBook'][0]
            log(f"{orderBook = }")
            order_status = orderBook['status']
            log(f"{order_status = }")

            if order_status_dict[str(order_status)].lower() == 'pending' :
                time.sleep(15)
                modify_orders(orderId ,limitPrice, qty)
                log(f"order rejected : {order_status}")
        else :
            log(f"order rejected : {order_response['message']}")
            
    except Exception as e:
        log(f"Unable to place order, {e}")
        
    return order_response

# ================ Check Current Position ===============

def get_current_position(symbol):
    pos = fyers.positions()
    log(f"{pos = }")
    if "netPositions" in pos:
        for p in pos["netPositions"]:
            if p["symbol"] == symbol:
                qty = p["netQty"]
                if qty > 0 or qty < 0:
                    return qty   # Long
                
    return 0  # No position


# =================== Supertrend Calculation ================

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

# ================== telegram updates ====================

def bot_trade(message):
    apiURL=f'https://api.telegram.org/bot{bot_token}/sendmessage'
    message = message.replace(" ,", "\n")

    if telegram_bot_updates :
        try:
            response = requests.post(apiURL,{'chat_id':chatID,'text': message })
            log(f'Bot_Trade_response : {response}')

        except Exception as e:  
            log(f'Bot Trade Message : {e}')
    else:
        log('Telegram bot updates diabled')

        
# ==================          =====================

def order_execute(symbol, data_df):
    prev_open = round(float(data_df['open'].iloc[-2]),2)
    prev_high = round(float(data_df['high'].iloc[-2]),2)
    prev_low = round(float(data_df['low'].iloc[-2]),2)
    prev_close = round(float(data_df['close'].iloc[-2]),2)

    curr_open = round(float(data_df['open'].iloc[-1]),2)
    curr_high = round(float(data_df['high'].iloc[-1]),2)
    curr_low = round(float(data_df['low'].iloc[-1]),2)
    curr_close = round(float(data_df['close'].iloc[-1]),2)
    
    ema = round(float(data_df[f'ema_{ema_period}'].iloc[-1]),2)
    atr = round(float(data_df['atr'].iloc[-1]),2)
    st_value = round(float(data_df['supertrend'].iloc[-1]),2)
    current_candle_date = data_df['date'].iloc[-1]

    #log(f"{data_df[['close',f'ema_{ema_period}']].tail(5) = }\n")
    log(f"\n{symbol = }, {current_candle_date = }, {curr_close = }, {ema = }, {atr = }, {st_value = }")
    bot_trade(f"\n{symbol = }, {current_candle_date = }, {curr_close = }, {ema = }, {atr = }, {st_value = }")

    data_df.to_csv(f"{symbol}.csv",index= False)

    current_position = get_current_position(symbol)

    #data = { "symbols": f"MCX:NATGASMINI" , "ohlcv_flag":"1"}
    #response = fyers.quotes(data)
    #print(response)

    ema_buy_condition  = curr_close >= ema and prev_close < ema #inorder to hold the positions if prev_close equals to ema
    ema_short_condition = curr_close <= ema and prev_close > ema  #inorder to hold the positions if prev_close equals to ema
    st_direction = data_df['st_direction'].iloc[-1]

    ema_direction = None
    if ema_buy_condition :
        ema_direction = "buy"
    elif ema_short_condition :
        ema_direction = "sell"
        
    log(f"{current_position = }\n{ema_buy_condition = }, {ema_short_condition = }, {ema_direction = }, {st_direction = }")

    long_condition = (ema_direction == "buy") # and (st_direction == "buy")
    short_condition = (ema_direction == "sell") # and (st_direction == "sell")
    
    if long_condition or short_condition :
        log(f"{current_time_str}, placing limit {ema_direction} order")
        bot_trade(f"{current_time_str}, placing limit {ema_direction} order")
        place_order(trading_symbol = symbol ,ltp = curr_close,qty = max_order_qty + current_position, side = ema_direction)
    else:
        log(f"{current_time_str}, No position change right now, maintaining old position")
        bot_trade(f"{current_time_str}, No position change right now, maintaining old position")

                
# ================== main function ====================

if __name__ == "__main__":
    symbol = get_future_contract()
    history_df = fetch_candles(symbol, resolution = "60" , duration = 100)

    if history_df is not None:
        
        history_df[f'ema_{ema_period}'] = history_df['close'].ewm(span=ema_period, adjust=False, min_periods=30).mean()
        history_df['atr'] = AverageTrueRange(history_df['high'], history_df['low'], history_df['close'], window = atr_window).average_true_range()
        history_df = supertrend(history_df, atr_window = supertrend_atr_window , window = supertrend_window, multiplier = supertrend_multiplier)

        '''price_df = history_df.copy() 
        for value in range(-14,0,1):
            history_df = price_df[:value]
            order_execute(symbol, history_df)
            os.system("pause")'''

        order_execute(symbol, history_df)
            
            


 

    

    




