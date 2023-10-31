import json
import requests
import websockets
import time
import asyncio
import os

# get all available symbol pairs from exchange
currency_url = 'https://api.bitforex.com/api/v1/market/symbols'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
check_activity = {}
trades_count_5min = {}
orders_count_5min = {}
# base web socket url
WS_URL = 'wss://www.bitforex.com/mkapi/coinGroup1/ws'

# fill the list with all available symbol pairs on exchange
for pair_s in currencies['data']:
    list_currencies.append(pair_s['symbol'])
    check_activity[pair_s['symbol']] = False
    trades_count_5min[pair_s['symbol'].split('-')[2].upper() + '-' + pair_s['symbol'].split('-')[1].upper()] = 0
    orders_count_5min[pair_s['symbol'].split('-')[2].upper() + '-' + pair_s['symbol'].split('-')[1].upper()] = 0


async def metadata():
    for pair in currencies['data']:
        pair_data = '@MD ' + pair['symbol'].split('-')[2].upper() + '-' + pair['symbol'].split('-')[1].upper() + \
                    ' spot ' + pair['symbol'].split('-')[2].upper() + ' ' + pair['symbol'].split('-')[1].upper() + \
                    ' ' + str(pair['pricePrecision']) + ' 1 1 0 0'
        print(pair_data, flush=True)
    print('@MDEND')


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# function to format the trades output
def get_trades(message):
    check_activity[message['param']['businessType']] = True
    trades_count_5min[
        message['param']['businessType'].split('-')[2].upper() + '-' + message['param']['businessType'].split('-')[1].upper()
    ] += len(message['data'])
    for elem in message['data']:
        print('!', get_unix_time(), message['param']['businessType'].split('-')[2].upper() \
              + '-' + message['param']['businessType'].split('-')[1].upper(),
              'B' if elem['direction'] == 1 else 'S', str('{0:.9f}'.format(elem['price'])),
              str('{0:.7f}'.format(elem['amount'])), flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    check_activity[message['param']['businessType']] = True
    orders_count_5min[
        message['param']['businessType'].split('-')[2].upper() + '-' + message['param']['businessType'].split('-')[1].upper()
    ] += len(message['data']['bids']) + len(message['data']['asks'])
    # check if bids array is not Null
    if 'bids' in message['data'] and message['data']['bids']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['param']['businessType'].split('-')[2].upper() \
                       + '-' + message['param']['businessType'].split('-')[1].upper() + ' B '
        pq = '|'.join(f"{str('{0:.9f}'.format(elem['amount']))}@{str('{0:.9f}'.format(elem['price']))}"
                      for elem in message['data']['bids'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)

    order_answer = ''
    pq = ''
    # check if asks array is not Null
    if 'asks' in message['data'] and message['data']['asks']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['param']['businessType'].split('-')[2].upper() \
                       + '-' + message['param']['businessType'].split('-')[1].upper() + ' S '
        pq = '|'.join(f"{str('{0:.9f}'.format(elem['amount']))}@{str('{0:.9f}'.format(elem['price']))}"
                      for elem in message['data']['asks'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(
            "ping_p"
        )
        print('Ping sent')
        await asyncio.sleep(5)


async def subscribe(ws):
    while True:
        # subscribe for listed topics
        for key, value in check_activity.copy().items():
            if not value:
                # subscribe to all trades
                await ws.send(json.dumps([
                    {"type": "subHq",
                     "event": "trade",
                     "param": {"businessType": key,
                               "size": 1}
                     }
                ]))
                await asyncio.sleep(0.1)
                if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
                    await ws.send(json.dumps([
                        {"type": "subHq",
                         "event": "depth10",
                         "param": {"businessType": key,
                                   "dType": 1}},
                        {"type": "subHq",
                         "event": "depth10",
                         "param": {"businessType": key,
                                   "dType": 0}}
                    ]))
                    await asyncio.sleep(0.1)
        # print(check_activity)
        for symbol in list(check_activity):
            check_activity[symbol] = False
        # print(check_activity)
        await asyncio.sleep(1800)


async def stats():
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
            sub_task = asyncio.create_task(subscribe(ws))
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))
            # print metadata about each pair symbols
            meta_data = asyncio.create_task(metadata())
            # print stats for trades and orders
            statistics = asyncio.create_task(stats())
            while True:

                try:
                    # receiving data from server
                    data = await ws.recv()
                    # change format of received data to json format
                    dataJSON = json.loads(data)
                    if 'event' in dataJSON and 'data' in dataJSON:
                        # check if received data is about trades
                        if dataJSON['event'] == 'trade' and len(dataJSON['data']) < 10:
                            get_trades(dataJSON)
                        # check if received data is about updates on order book
                        elif dataJSON['event'] == 'depth10' and dataJSON['param']['dType'] == 1:
                            get_order_books_and_deltas(dataJSON, update=True)
                        # check if received data is about order books
                        elif dataJSON['event'] == 'depth10' and dataJSON['param']['dType'] == 0:
                            get_order_books_and_deltas(dataJSON, update=False)
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred")
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")


# run main function
asyncio.run(main())
