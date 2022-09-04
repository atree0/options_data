import pandas as pd
import numpy as np
import requests
import json
from matplotlib import pyplot as plt
from polygon import RESTClient
import psycopg2
from sqlalchemy import all_, create_engine
import time
import datetime
import asyncio
from http.client import responses
import aiohttp

start_full = time.time()

### insert database info
dbstring = ""
connection = create_engine(dbstring).connect()
connection

### insert api key for polygon.io
api_key = ""

### retrieving tickers known to have an options chain
query = "SELECT distinct(ticker) from public.optionable_stocks"
tickers = pd.read_sql(query, connection)['ticker']
additional = ['SPX', 'NDX', 'RUT', 'DJX', 'VIX']
tickers = tickers.append(pd.Series(additional)).reset_index(drop=True)

for symbol in tickers:
    try:
        ticker = symbol

        url = 'https://api.polygon.io/v3/snapshot/options/{}/{}?apiKey={}'

        req = requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={ticker}&limit=1000&apiKey={api_key}").json()
        symbols = pd.DataFrame(req['results'])
        

        while len(req['results']) == 1000:
            req = requests.get(str(req['next_url']) + f"&apiKey={api_key}").json()
            symbols = symbols.append(req['results'])
        symbols = symbols.reset_index().drop('index', axis=1)['ticker']

        start = time.time()
        results = []

        def get_tasks(session):
            tasks = []
            for symbol in symbols:
                tasks.append(asyncio.create_task(session.get(url.format(ticker, symbol, api_key), ssl=False)))
            return tasks

        async def get_symbols():
            async with aiohttp.ClientSession() as session:
                tasks = get_tasks(session)
                responses = await asyncio.gather(*tasks)
                for response in responses:
                    results.append(await (response.json()))

        asyncio.run(get_symbols())
        # loop = asyncio.get_event_loop()
        # loop.run_until_complete(get_symbols())
        # loop.close()
        end = time.time()
        total_time = end - start
        
        results = pd.DataFrame(results)['results']
        results = pd.json_normalize(results)

        results.to_csv("/Users/adam/Documents/all_options/{}options{}{}.csv".format(ticker, datetime.date.today().strftime('%m'), datetime.date.today().strftime('%d')))

        print("{} seconds to make {} API calls for {}".format(total_time, len(symbols), ticker))

    except:
        print("ERROR FOR {}".format(ticker))

all_options = pd.DataFrame()

for symbol in tickers:
    try:
        all_options = all_options.append(pd.read_csv("/Users/adam/Documents/all_options/{}options{}{}.csv".format(symbol, datetime.date.today().strftime('%m'), datetime.date.today().strftime('%d'))))
    except:
        print("could not find {}".format(symbol))

all_options['day.last_updated'] = pd.to_datetime(all_options['day.last_updated'])
all_options['last_quote.last_updated'] = pd.to_datetime(all_options['last_quote.last_updated'])
all_options['date'] = all_options['day.last_updated'].max()
all_options = all_options.set_index('details.ticker')
all_options = all_options.drop('Unnamed: 0', axis=1)

all_options.to_csv('/Users/adam/Downloads/ALLoptions{}{}.csv'.format(datetime.date.today().strftime('%m'), datetime.date.today().strftime('%d')))

all_options.to_sql('all_options_data', connection, if_exists='append')

end_full = time.time()
print(end_full - start_full)
