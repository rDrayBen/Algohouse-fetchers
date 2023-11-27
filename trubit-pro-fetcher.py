import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

# get all available symbol pairs from exchange
currency_url = 'https://api.mexo.io/openapi/v1/exchange'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
trades_count_5min = {}
orders_count_5min = {}
# base web socket url
WS_URL = 'wss://ws.trubit.com/openapi/quote/ws/v1'
precision = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000,
             1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000]

# fill the list with all available symbol pairs on exchange
for pair_s in currencies['symbols']:
    if pair_s['status'] == 'TRADING':
        list_currencies.append(pair_s['symbol'])


async def metadata():
    for pair in currencies['symbols']:
        if pair['status'] == 'TRADING':
            prec = 11
            for i in range(len(precision)):
                if float(pair['quotePrecision']) * precision[i] == 1:
                    prec = i
            trades_count_5min[pair['symbol']] = 0
            orders_count_5min[pair['symbol']] = 0
            pair_data = '@MD ' + pair['symbol'] + ' spot ' + pair['baseAsset'] + ' ' + pair['quoteAsset'] + ' ' \
                        + str(prec) + ' 1 1 0 0'
            print(pair_data, flush=True)
    print('@MDEND')


# function to format the trades output
def get_trades(message):
    trades_count_5min[message['symbol']] += len(message['data'])
    for elem in message['data']:
        print('!', get_unix_time(), message['symbol'],'B' if elem['m'] else 'S', elem['p'], elem['q'], flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    orders_count_5min[message['symbol']] += len(message['data'][0]['b']) + len(message['data'][0]['a'])
    # check if bids array is not Null
    if 'b' in message['data'][0] and message['data'][0]['b']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['symbol'] + ' B '
        pq = '|'.join(f"{elem[1]}@{elem[0]}"
                      for elem in message['data'][0]['b'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)

    # check if asks array is not Null
    if 'a' in message['data'][0] and message['data'][0]['a']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['symbol'] + ' S '
        pq = '|'.join(f"{elem[1]}@{elem[0]}"
                      for elem in message['data'][0]['a'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
            "ping": get_unix_time()
        }))
        print(json.dumps({
            "topic": "ping"
        }))
        await asyncio.sleep(10)


async def subscribe(ws):
    # subscribe for listed topics
    for symbol in list_currencies:
        await ws.send(json.dumps({
          "symbol": f"{symbol}",
          "topic": "trade",
          "event": "sub",
          "params": {
            "binary": False
          }
        }))
        if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
            await ws.send(json.dumps({
              "symbol": f"{symbol}",
              "topic": "diffDepth",
              "event": "sub",
              "params": {
                "binary": False
              }
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
                    # check if received data is about trades
                    if 'topic' in dataJSON and 'data' in dataJSON:
                        if dataJSON['topic'] == 'trade' and not dataJSON['f']:
                            get_trades(dataJSON)
                        # skip history trade data
                        elif dataJSON['topic'] == 'trade' and dataJSON['f']:
                            pass
                        # check if received data is about updates on order book
                        elif dataJSON['topic'] == 'diffDepth' and not dataJSON['f']:
                            get_order_books_and_deltas(dataJSON,
                                                       update=True)
                        # check if received data is about order books
                        elif dataJSON['topic'] == 'diffDepth' and dataJSON['f']:
                            get_order_books_and_deltas(dataJSON,
                                                       update=False)
                        else:
                            print(dataJSON)
                    elif 'error' in dataJSON:
                        print(f"Exception {e} occurred", data)
                        time.sleep(1)
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred", data)
                    time.sleep(1)
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")
            time.sleep(1)


# run main function
asyncio.run(main())
