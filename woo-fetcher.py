import json
import requests
import websockets
import time
import asyncio
import os

# get all available symbol pairs from exchange
currency_url = 'https://api.woo.org/v1/public/info'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
check_activity = {}
trades_count_5min = {}
orders_count_5min = {}
# base web socket url
WS_URL = 'wss://wss.woo.org/ws/stream/1fbac7b8-d849-4f95-b2ef-cf988a95f4d3'
# user id
UI = '91648'
precision = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000]

# fill the list with available pairs
for currency in currencies['rows']:
    list_currencies.append(currency['symbol'][5:])


async def metadata():
    for pair in currencies['rows']:
        prec = 0
        for i in range(len(precision)):
            if pair['base_tick'] * precision[i] == 1:
                prec = i
        pair_data = '@MD ' + pair['symbol'].split('_')[1] + '-' + pair['symbol'].split('_')[2] + ' spot ' + \
                    pair['symbol'].split('_')[1] + ' ' + pair['symbol'].split('_')[2] + ' ' + \
                    str(prec) + ' 1 1 0 0'
        check_activity[pair['symbol'][5:]] = False
        trades_count_5min[pair['symbol'].split('_')[1] + '-' + pair['symbol'].split('_')[2]] = 0
        orders_count_5min[pair['symbol'].split('_')[1] + '-' + pair['symbol'].split('_')[2]] = 0
        print(pair_data, flush=True)
    print('@MDEND')
    # print(check_activity)


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# function to format the trades output
def get_trades(message):
    check_activity[message['symbol'][5:]] = True
    trades_count_5min[message['symbol'].split('_')[1] + '-' + message['symbol'].split('_')[2]] += 1
    print('!', get_unix_time(), message['symbol'].split('_')[1] + '-' + message['symbol'].split('_')[2],
          message['side'][0].upper(), str('{0:.9f}'.format(message['price'])),
          str('{0:.9f}'.format(message['size'])), flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    check_activity[message['symbol'][5:]] = True
    orders_count_5min[message['symbol'].split('_')[1] + '-' + message['symbol'].split('_')[2]] += len(message['bids']) + len(message['asks'])
    # check if bids array is not Null
    if message['bids']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['symbol'].split('_')[1] + '-' +\
                       message['symbol'].split('_')[2] + ' B '
        pq = '|'.join(f"{str('{0:.9f}'.format(elem[1]))}@{str('{0:.9f}'.format(elem[0]))}"
                      for elem in message['bids'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)

    order_answer = ''
    pq = ''
    # check if asks array is not Null
    if message['asks']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['symbol'].split('_')[1] + '-' +\
                       message['symbol'].split('_')[2] + ' S '
        pq = '|'.join(f"{str('{0:.9f}'.format(elem[1]))}@{str('{0:.9f}'.format(elem[0]))}"
                      for elem in message['asks'])
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


async def subscribe(ws, symb):
    # subscribe for listed topics
    # subscribe to all trades
    await ws.send(json.dumps({
        "id": f"{UI}",
        "topic": f"SPOT_{symb}@trade",
        "event": "subscribe"
    }))
    if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
        # subscribe to all order books
        await ws.send(json.dumps({
            "id": f"{UI}",
            "topic": f"SPOT_{symb}@orderbook",
            "event": "subscribe"
        }))
        # subscribe to all order book updates
        await ws.send(json.dumps({
            "id": f"{UI}",
            "topic": f"SPOT_{symb}@orderbookupdate",
            "event": "subscribe"
        }))


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


async def connect(symbol):
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL, ping_interval=9):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
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
                        if 'trade' in dataJSON['topic']:
                            get_trades(dataJSON['data'])
                        # check if received data is about order books
                        elif 'orderbookupdate' in dataJSON['topic']:
                            get_order_books_and_deltas(dataJSON['data'], update=True)
                        # check if received data is about updates on order book
                        elif 'orderbook' in dataJSON['topic']:
                            get_order_books_and_deltas(dataJSON['data'], update=False)
                        else:
                            print(dataJSON)
                    # sending ping to keep connection alive
                    elif 'event' in dataJSON and dataJSON['event'] == 'ping':
                        await ws.send(json.dumps({
                            'event': "pong",
                            "ts": get_unix_time()
                        }))
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred")
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")


async def connectionHandler():
    # print metadata about each pair symbols
    meta_data = asyncio.create_task(metadata())
    # print stats for trades and orders
    statistics = asyncio.create_task(stats())
    tasks = []

    for symbol in list_currencies:
        task = asyncio.create_task(connect(symbol))
        tasks.append(task)
        await asyncio.sleep(1)
        # print(check_activity, 'bababa')
    await asyncio.gather(*tasks)


async def main():
    await connectionHandler()

# run main function
asyncio.run(main())
