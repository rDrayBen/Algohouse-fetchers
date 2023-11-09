import json
import requests
import websockets
import time
import asyncio
import os

# get all available symbol pairs from exchange
currency_url = 'https://api.exmo.com/v1.1/pair_settings'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
check_activity = {}
trades_count_5min = {}
orders_count_5min = {}
# base web socket url
WS_URL = 'wss://ws-api.exmo.com:443/v1/public'

# fill the list with all available symbols on the exchange
list_currencies = list(currencies.keys())


async def metadata():
    for pair in list_currencies:
        check_activity[pair] = False
        trades_count_5min[pair.split('_')[0] + '-' + pair.split('_')[1]] = 0
        orders_count_5min[pair.split('_')[0] + '-' + pair.split('_')[1]] = 0
        pair_data = '@MD ' + pair.split('_')[0] + '-' + pair.split('_')[1] + ' spot ' + \
                    pair.split('_')[0] + ' ' + pair.split('_')[1] + ' ' + str(currencies[pair]['price_precision']) \
                    + ' 1 1 0 0'
        print(pair_data, flush=True)
    print('@MDEND')


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# function to format the trades output
def get_trades(message):
    check_activity[message['topic'].split(':')[1]] = True
    coin_name = message['topic'].split(':')[1].split('_')[0] + '-' + message['topic'].split(':')[1].split('_')[1]
    trades_count_5min[coin_name] += len(message['data'])
    if 'data' in message:
        for elem in message['data']:
            print('!', get_unix_time(), coin_name,
                  elem['type'][0].upper(), elem['price'],
                  elem['amount'], flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    check_activity[message['topic'].split(':')[1]] = True
    coin_name = message['topic'].split(':')[1].split('_')[0] + '-' + message['topic'].split(':')[1].split('_')[1]
    orders_count_5min[coin_name] += len(message['data']['bid']) + len(message['data']['ask'])
    # check if bids array is not Null
    if 'data' in message and 'bid' in message['data'] and message['data']['bid']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + coin_name + ' B '
        pq = '|'.join(f"{elem[2]}@{elem[0]}"
                      for elem in message['data']['bid'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)

    order_answer = ''
    pq = ''
    # check if asks array is not Null
    if 'data' in message and 'ask' in message['data'] and message['data']['ask']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + coin_name + ' S '
        pq = '|'.join(f"{elem[2]}@{elem[0]}"
                      for elem in message['data']['ask'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
            "event": "ping"
        }))
        await asyncio.sleep(5)


async def subscribe(ws):
    while True:
        # subscribe for listed topics
        for key, value in check_activity.items():
            # subscribe to all trades
            if not value:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "topics": [
                        f"spot/trades:{key}"
                    ]
                }))
                if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "topics": [
                            f"spot/order_book_updates:{key}"
                        ]
                    }))
        for symbol in list(check_activity):
            check_activity[symbol] = False
        await asyncio.sleep(3000)


async def stats():
    time_to_wait = float(5 - ((time.time() / 60) % 5)) * 60
    await asyncio.sleep(time_to_wait)
    while True:
        stat_line = '# LOG:CAT=trades_stats:MSG= '
        for symbol, amount in trades_count_5min.items():
            if amount != 0:
                stat_line += f"{symbol}:{amount} "
            trades_count_5min[symbol] = 0
        if stat_line != '# LOG:CAT=trades_stats:MSG= ':
            print(stat_line)

        stat_line = '# LOG:CAT=orderbook_stats:MSG= '
        for symbol, amount in orders_count_5min.items():
            if amount != 0:
                stat_line += f"{symbol}:{amount} "
            orders_count_5min[symbol] = 0
        if stat_line != '# LOG:CAT=orderbook_stats:MSG= ':
            print(stat_line)
        await asyncio.sleep(300)


async def main():
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL, ping_interval=None):
        try:
            # print metadata about each pair symbols
            meta_data = asyncio.create_task(metadata())
            sub_task = asyncio.create_task(subscribe(ws))
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))
            # print stats for trades and orders
            statistics = asyncio.create_task(stats())
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
                        elif 'order_book_updates' in dataJSON['topic'] and dataJSON['event'] == 'snapshot':
                            get_order_books_and_deltas(dataJSON, update=False)
                        # check if received data is about order books
                        elif 'order_book_updates' in dataJSON['topic'] and dataJSON['event'] == 'update':
                            get_order_books_and_deltas(dataJSON, update=True)
                        else:
                            print(dataJSON)
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred")
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")


# run main function
asyncio.run(main())
