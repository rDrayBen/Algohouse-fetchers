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
currency_url = 'https://api.woo.org/v4/public/info'
# base web socket url
WS_URL = 'wss://wss.woo.org/ws/stream/1fbac7b8-d849-4f95-b2ef-cf988a95f4d3'
# user id
UI = '91648'

answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
check_activity = {}
trades_count_5min = {}
orders_count_5min = {}


# fill the list with available pairs
for currency in currencies['data']['rows']:
    if CURRENT_MODE == 'SPOT' and "SPOT" in currency['symbol']:
        list_currencies.append(currency['symbol'])
    elif CURRENT_MODE == 'FUTURES' and "PERP" in currency['symbol']:
        list_currencies.append(currency['symbol'])


async def metadata():
    for pair in currencies['data']['rows']:
        if CURRENT_MODE == 'SPOT' and "SPOT" in pair['symbol']:
            pair_data = '@MD ' + pair['symbol'] + ' spot ' + pair['symbol'].split('_')[1] + \
                        ' ' + pair['symbol'].split('_')[2] + ' ' + str(str(pair['precisions'][-1]).count('0')) + ' 1 1 0 0'
            check_activity[pair['symbol']] = False
            trades_count_5min[pair['symbol']] = 0
            orders_count_5min[pair['symbol']] = 0
            print(pair_data, flush=True)
        elif CURRENT_MODE == 'FUTURES' and "PERP" in pair['symbol']:
            pair_data = '@MD ' + pair['symbol'] + ' perpetual ' + pair['symbol'].split('_')[1] + \
                        ' ' + pair['symbol'].split('_')[2] + ' ' + str(str(pair['precisions'][-1]).count('0')) + ' 1 1 0 0'
            check_activity[pair['symbol']] = False
            trades_count_5min[pair['symbol']] = 0
            orders_count_5min[pair['symbol']] = 0
            print(pair_data, flush=True)
    print('@MDEND')


def chunk_array(array, chunk_size):
    result = [array[i:i + chunk_size] for i in range(0, len(array), chunk_size)]
    return result


# function to format the trades output
def get_trades(message):
    for trade in message['data']:
        check_activity[trade['s']] = True
        trades_count_5min[trade['s']] += 1
        print('!', get_unix_time(), trade['s'],
              trade['b'][0].upper(), str('{0:.9f}'.format(trade['p'])),
              str('{0:.9f}'.format(trade['a'])), flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    check_activity[message['data']['symbol']] = True
    orders_count_5min[message['data']['symbol']] += len(message['data']['bids']) + len(message['data']['asks'])
    # check if bids array is not Null
    if message['data']['bids'] and len(message['data']['bids']):
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['data']['symbol'] + ' B '
        pq = '|'.join(f"{str('{0:.9f}'.format(elem[1]))}@{str('{0:.9f}'.format(elem[0]))}"
                      for elem in message['data']['bids'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)

    order_answer = ''
    pq = ''
    # check if asks array is not Null
    if message['data']['asks'] and len(message['data']['asks']):
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['data']['symbol'] + ' S '
        pq = '|'.join(f"{str('{0:.9f}'.format(elem[1]))}@{str('{0:.9f}'.format(elem[0]))}"
                      for elem in message['data']['asks'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
            'event': "ping"
        }))
        await asyncio.sleep(9)


async def subscribe(ws, symb_chunk):
    # subscribe for listed topics
    for symb in symb_chunk:
        # subscribe to all trades
        await ws.send(json.dumps({
            "id": f"{symb}@trades",
            "topic": f"{symb}@trades",
            "event": "subscribe"
        }))
        if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
            # subscribe to all order books
            await ws.send(json.dumps({
                "id": "orderbook",
                "event": "request",
                "params": {
                    "type": "orderbook",
                    "symbol": symb
                }
            }))
            # subscribe to all order book updates
            await ws.send(json.dumps({
                "id": f"{symb}@orderbookupdate",
                "topic": f"{symb}@orderbookupdate",
                "event": "subscribe"
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


async def connect(symbol_chunk):
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL, ping_interval=9):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol_chunk))
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))

            while True:
                # receiving data from server
                data = await ws.recv()
                # change format of received data to json format
                dataJSON = json.loads(data)
                try:
                    if 'topic' in dataJSON:
                        # check if received data is about trades
                        if 'trades' in dataJSON['topic']:
                            get_trades(dataJSON)
                        # check if received data is about updates on order book
                        elif 'orderbookupdate' in dataJSON['topic']:
                            get_order_books_and_deltas(dataJSON, update=True)
                        else:
                            print(dataJSON)
                    # sending ping to keep connection alive
                    elif 'event' in dataJSON and dataJSON['event'] == 'ping':
                        await ws.send(json.dumps({
                            'event': "pong",
                            "ts": get_unix_time()
                        }))
                    # check if received data is about order books
                    elif dataJSON['id'] == 'orderbook' and dataJSON['event'] == 'request':
                        get_order_books_and_deltas(dataJSON, update=False)
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred", data)
                    time.sleep(1)
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")
            time.sleep(1)


async def connectionHandler():
    # print metadata about each pair symbols
    meta_data = asyncio.create_task(metadata())
    # print stats for trades and orders
    statistics = asyncio.create_task(print_stats(trades_count_5min, orders_count_5min))
    chunked_array = chunk_array(list_currencies, 10)
    tasks = []

    for chunk in chunked_array:
        task = asyncio.create_task(connect(chunk))
        tasks.append(task)
        await asyncio.sleep(1)
    await asyncio.gather(*tasks)


async def main():
    await connectionHandler()

# run main function
asyncio.run(main())
