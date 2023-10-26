import json
import requests
import websockets
import time
import asyncio
import os

currency_url = 'https://api.poloniex.com/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
check_activity = {}
WS_URL = 'wss://ws.poloniex.com/ws/public'
for currency in currencies:
    if currency['state'] == 'NORMAL':
        list_currencies.append(currency['symbol'])
        check_activity[currency['symbol']] = False


async def metadata():
    for pair in currencies:
        if pair['state'] == 'NORMAL':
            pair_data = '@MD ' + pair['baseCurrencyName'] + '-' + pair['quoteCurrencyName'] + ' spot ' + \
                        pair['baseCurrencyName'] + ' ' + pair['quoteCurrencyName'] + ' ' + \
                        str(pair['symbolTradeLimit']['priceScale']) + ' 1 1 0 0'
            print(pair_data, flush=True)
    print('@MDEND')


def get_unix_time():
    return round(time.time() * 1000)


def get_trades(message):
    if 'data' in message:
        for elem in message['data']:
            check_activity[elem['symbol']] = True
            print('!', get_unix_time(), elem['symbol'].split('_')[0] + '-' + elem['symbol'].split('_')[1],
                  elem['takerSide'][0].upper(), elem['price'],
                  float(elem['quantity']), flush=True)


def get_order_books_and_deltas(message, update):
    check_activity[message['data'][0]['symbol']] = True
    if message['data'][0]['bids'] and len(message['data'][0]['bids']) > 0:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['data'][0]['symbol'].split('_')[0] + '-' + \
                       message['data'][0]['symbol'].split('_')[1] + ' B '
        pq = '|'.join(f"{elem[1]}@{elem[0]}"
                      for elem in message['data'][0]['bids'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)

    if message['data'][0]['asks'] and len(message['data'][0]['asks']) > 0:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['data'][0]['symbol'].split('_')[0] + '-' + \
                       message['data'][0]['symbol'].split('_')[1] + ' B '
        pq = '|'.join(f"{elem[1]}@{elem[0]}"
                      for elem in message['data'][0]['asks'])
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
        await asyncio.sleep(20)


async def subscribe(ws):
    while True:
        resub_list_books = [key.lower() for key, value in check_activity.items() if value == False]
        resub_list_trades = [key for key, value in check_activity.items() if value == False]
        await ws.send(json.dumps({
            "event": "subscribe",
            "channel": ["trades"],
            "symbols": resub_list_trades
        }))
        await asyncio.sleep(1.01) # A single IP is limited to 2000 simultaneous connections on each of the public and private channels.
        if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
            await ws.send(json.dumps({
                "event": "subscribe",
                "channel": ["book_lv2"],
                "symbols": resub_list_books,
                "depth": 20
            }))
        # print(check_activity)
        for symbol in list(check_activity):
            check_activity[symbol] = False
        # print(check_activity)
        await asyncio.sleep(3000)


async def main():
    async for ws in websockets.connect(WS_URL, ping_interval=None):
        try:
            # create task to subscribe to all trades, order books and order book updates
            sub_task = asyncio.create_task(subscribe(ws))
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))
            # print metadata about each pair symbols
            meta_data = asyncio.create_task(metadata())
            while True:
                data = await ws.recv()
                try:
                    dataJSON = json.loads(data)
                    if 'channel' in dataJSON:
                        if dataJSON['channel'] == "trades":
                            get_trades(dataJSON)
                        elif dataJSON['channel'] == "book_lv2" and dataJSON['action'] == 'update':
                            get_order_books_and_deltas(dataJSON, update=True)
                        elif dataJSON['channel'] == "book_lv2" and dataJSON['action'] == 'snapshot':
                            get_order_books_and_deltas(dataJSON, update=False)
                        else:
                            print(dataJSON)
                    else:
                        print(dataJSON)
                except Exception as e:
                    # print(f"Exception {e} occurred", dataJSON)
                    pass
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")


asyncio.run(main())
