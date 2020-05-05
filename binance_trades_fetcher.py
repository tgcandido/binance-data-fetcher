import requests
import json
import time
import sys
import pandas as pd
import calendar
from datetime import datetime, timedelta

def get_unix_ms_from_date(date):
    return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond/1000)

def get_first_trade_id_from_start_date(symbol, from_date):
    new_end_date = from_date + timedelta(seconds=60)
    r = requests.get('https://api.binance.com/api/v3/aggTrades', 
        params = {
            "symbol" : symbol,
            "startTime": get_unix_ms_from_date(from_date),
            "endTime": get_unix_ms_from_date(new_end_date)
        })
        
    if r.status_code != 200:
        print('somethings wrong!', r.status_code)
        print('sleeping for 10s... will retry')
        time.sleep(10)
        get_first_trade_id_from_start_date(symbol, from_date)
    
    response = r.json()
    if len(response) > 0:
        return response[0]['a']
    else:
        raise Exception('no trades found')

def get_trades(symbol, from_id):
    r = requests.get("https://api.binance.com/api/v3/aggTrades",
        params = {
            "symbol": symbol,
            "limit": 1000,
            "fromId": from_id
            })

    if r.status_code != 200:
        print('somethings wrong!', r.status_code)
        print('sleeping for 10s... will retry')
        time.sleep(10)
        get_historical_trades(symbol, from_id)

    return r.json()

def trim(df, to_date):
    return df[df['T'] <= get_unix_ms_from_date(to_date)]

def fetch_binance_trades(symbol, from_date, to_date):
    from_id = get_first_trade_id_from_start_date(symbol, from_date)
    current_time = 0
    df = pd.DataFrame()

    while current_time < get_unix_ms_from_date(to_date):
        try:
            trades = get_trades(symbol, from_id)
        
            from_id = trades[-1]['a']
            current_time = trades[-1]['T']
            
            print(f'fetched {len(trades)} trades from id {from_id} @ {datetime.utcfromtimestamp(current_time/1000.0)}')
            
            df = pd.concat([df, pd.DataFrame(trades)])
        
            #dont exceed request limits
            time.sleep(0.5)
        except Exception:
            print('somethings wrong....... sleeping for 15s')
            time.sleep(15)

    df.drop_duplicates(subset='a', inplace=True)
    df = trim(df, to_date)
    
    filename = f'binance__{symbol}__trades__from__{sys.argv[2].replace("/", "_")}__to__{sys.argv[3].replace("/", "_")}.csv'
    df.to_csv(filename)

    print(f'{filename} file created!')

if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise Exception('arguments format: <symbol> <start_date> <end_date>')
        exit()

    symbol = sys.argv[1]

    from_date = datetime.strptime(sys.argv[2], '%m/%d/%Y')
    to_date = datetime.strptime(sys.argv[3], '%m/%d/%Y') + timedelta(days=1) - timedelta(microseconds=1)

    fetch_binance_trades(symbol, from_date, to_date)