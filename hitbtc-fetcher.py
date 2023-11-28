import json
import requests
import websockets
import time
import asyncio
import os
import sys
from CommonFunctions.CommonFunctions import get_unix_time, stats

CURRENT_MODE = 'SPOT'

# Extract command-line arguments starting from index 1
args = sys.argv[1:]

# Check if there are any arguments
if len(args) > 0:
    # Iterate through the arguments
    for arg in args:
        # Check if the argument starts with '-'
        if arg.startswith('-') and arg[1:] == 'perpetual':
            CURRENT_MODE = 'FUTURES'
            break
else:
    CURRENT_MODE = "SPOT"

# get all available symbol pairs from exchange
currency_url = 'https://api.hitbtc.com/api/3/public/symbol'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
trades_count_5min = {}
orders_count_5min = {}
# base web socket url
WS_URL = 'wss://api.hitbtc.com/api/3/ws/public'

# fill the list with all available symbol pairs on exchange
for pair_s in currencies:
    if CURRENT_MODE == 'SPOT' and currencies[pair_s]['type'] == 'spot':
        list_currencies.append(pair_s)
    elif CURRENT_MODE == 'FUTURES' and currencies[pair_s]['type'] == 'futures':
        list_currencies.append(pair_s)


async def metadata():
    for pair in list_currencies:
        curr = currencies[pair]
        if CURRENT_MODE == 'SPOT' and curr['type'] == 'spot' and curr['status'] == 'working':
            pair_data = '@MD ' + pair + ' spot ' + curr['base_currency'] + ' ' + curr['quote_currency'] + ' ' + \
                        str(curr['tick_size'].count('0')) + ' 1 1 0 0'
            print(pair_data, flush=True)
            trades_count_5min[pair] = 0
            orders_count_5min[pair] = 0
        elif CURRENT_MODE == 'FUTURES' and curr['type'] == 'futures' and curr['status'] == 'working':
            pair_data = '@MD ' + pair + ' perpetual ' + curr['underlying'] + ' ' + curr['quote_currency'] + ' ' + \
                        str(curr['tick_size'].count('0')) + ' 1 1 0 0'
            print(pair_data, flush=True)
            trades_count_5min[pair] = 0
            orders_count_5min[pair] = 0
    print('@MDEND')


# function to format the trades output
def get_trades(message):
    if 'snapshot' in message:
        coin_name = str(message['snapshot'].keys()).replace("dict_keys(['", '').replace("'])", '')
        for elem in list(message['snapshot'].values())[0]:
            print('!', get_unix_time(), coin_name,
                  elem['s'][0].upper(), elem['p'],
                  elem['q'], flush=True)
    elif 'update' in message:
        coin_name = str(message['update'].keys()).replace("dict_keys(['", '').replace("'])", '')
        trades_count_5min[coin_name] += len(list(message['update'].values())[0])
        for elem in list(message['update'].values())[0]:
            print('!', get_unix_time(), coin_name,
                  elem['s'][0].upper(), elem['p'],
                  elem['q'], flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, coin_name, update):
    orders_count_5min[coin_name] += len(message['b']) + len(message['a'])
    # check if bids array is not Null
    if message['b']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + coin_name + ' B '
        pq = '|'.join(f"{elem[1]}@{elem[0]}"
                      for elem in message['b'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)

    order_answer = ''
    pq = ''
    # check if asks array is not Null
    if message['a']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + coin_name + ' S '
        pq = '|'.join(f"{elem[1]}@{elem[0]}"
                      for elem in message['a'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
            "event": "pong"
        }))
        await asyncio.sleep(5)


async def subscribe(ws):
    # subscribe for listed topics
    await ws.send(json.dumps({
        "method": "subscribe",
        "ch": "trades",
        "params": {
            "symbols": list_currencies,
            "limit": 0
        },
        "id": 123
    }))
    if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
        await ws.send(json.dumps({
            "method": "subscribe",
            "ch": "orderbook/full",
            "params": {
                "symbols": list_currencies
            },
            "id": 123
        }))


# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
    time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
    if time_to_wait != 300:
        await asyncio.sleep(time_to_wait)
    while True:
        stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
        time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
        await asyncio.sleep(time_to_wait)


async def main():
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL, ping_interval=None):
        try:
            sub_task = asyncio.create_task(subscribe(ws))
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))
            # print metadata about each pair symbols
            meta_data = asyncio.create_task(metadata())
            # print stats for trades and orders
            statistics = asyncio.create_task(print_stats(trades_count_5min, orders_count_5min))
            while True:
                # receiving data from server
                data = await ws.recv()
                try:
                    # change format of received data to json format
                    dataJSON = json.loads(data)
                    if 'ch' in dataJSON:
                        # check if received data is about trades
                        if dataJSON['ch'] == 'trades':
                            get_trades(dataJSON)
                        # check if received data is about updates on order book
                        elif dataJSON['ch'] == 'orderbook/full' and 'update' in dataJSON:
                            get_order_books_and_deltas(list(dataJSON['update'].values())[0],
                                                       str(dataJSON['update'].keys()).replace("dict_keys(['", '').replace("'])", ''),
                                                       update=True)
                        # check if received data is about order books
                        elif dataJSON['ch'] == 'orderbook/full' and 'snapshot' in dataJSON:
                            get_order_books_and_deltas(list(dataJSON['snapshot'].values())[0],
                                                       str(dataJSON['snapshot'].keys()).replace("dict_keys(['", '').replace("'])", ''),
                                                       update=False)
                        else:
                            print(dataJSON)
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred")
                    time.sleep(1)
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")
            time.sleep(1)


# run main function
asyncio.run(main())
