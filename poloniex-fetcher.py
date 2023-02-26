import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.poloniex.com/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://ws.poloniex.com/ws/public'
for currency in currencies:
    if currency['state'] == 'NORMAL':
        list_currencies.append(currency['symbol'])


async def metadata():
    for pair in currencies:
        if pair['state'] == 'NORMAL':
            pair_data = '@MD ' + pair['baseCurrencyName'] + '-' + pair['quoteCurrencyName'] + ' spot ' + \
                        pair['baseCurrencyName'] + ' ' + pair['quoteCurrencyName'] + ' ' + \
                        str(pair['symbolTradeLimit']['priceScale']) + ' 1 1'
            print(pair_data, flush=True)
    print('@MDEND')


def get_unix_time():
    return round(time.time() * 1000)


def get_trades(message):
    trade_data = message
    if 'data' in trade_data:
        for elem in trade_data['data']:
            print('!', get_unix_time(), elem['symbol'].split('_')[0] + '-' + elem['symbol'].split('_')[1],
                  elem['takerSide'][0].upper(), elem['price'],
                  float(elem['amount']) + float(elem['quantity']), flush=True)


def get_order_books_and_deltas(message, update):
    if 'action' in message and message['action'] == 'snapshot':
        return
    elif 'action' in message and message['action'] == 'update':
        update = True
    if update:
        if 'data' in message:
            if 'bids' in message['data'][0] and message['data'][0]['bids']:
                order_answer = '$ ' + str(get_unix_time()) + ' ' + message['data'][0]['symbol'].split('_')[0] + '-' + \
                               message['data'][0]['symbol'].split('_')[1] + ' B '
                pq = '|'.join(f"{elem[1]}@{elem[0]}"
                              for elem in message['data'][0]['bids'])
                print(order_answer + pq, flush=True)

            if 'asks' in message['data'][0] and message['data'][0]['asks']:
                order_answer = '$ ' + str(get_unix_time()) + ' ' + message['data'][0]['symbol'].split('_')[0] + '-' + \
                               message['data'][0]['symbol'].split('_')[1] + ' S '
                pq = '|'.join(f"{elem[1]}@{elem[0]}"
                              for elem in message['data'][0]['asks'])
                print(order_answer + pq, flush=True)
    else:
        if 'data' in message:
            for i in range(len(message['data'])):
                if 'bids' in message['data'][i] and message['data'][i]['bids']:
                    order_answer = '$ ' + str(get_unix_time()) + ' ' + message['data'][i]['symbol'].split('_')[0] + '-' + \
                                   message['data'][i]['symbol'].split('_')[1] + ' B '
                    pq = '|'.join(f"{elem[1]}@{elem[0]}"
                                  for elem in message['data'][i]['bids'])
                    print(order_answer + pq + ' R', flush=True)

                if 'asks' in message['data'][i] and message['data'][i]['asks']:
                    order_answer = '$ ' + str(get_unix_time()) + ' ' + message['data'][0]['symbol'].split('_')[0] + '-' + \
                                   message['data'][i]['symbol'].split('_')[1] + ' S '
                    pq = '|'.join(f"{elem[1]}@{elem[0]}"
                                  for elem in message['data'][i]['asks'])
                    print(order_answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
                    "event": "ping"
                }))
        await asyncio.sleep(5)


async def subscribe(ws):
    await ws.send(json.dumps({
        "event": "subscribe",
        "channel": ["trades"],
        "symbols": ["all"]
    }))

    for symbol in list_currencies:
        await ws.send(json.dumps({
            "event": "subscribe",
            "channel": ["book"],
            "symbols": [f"{symbol.split('_')[0].lower()}_{symbol.split('_')[1].lower()}"],
            "depth": 20
        }))
        await ws.send(json.dumps({
            "event": "subscribe",
            "channel": ["book_lv2"],
            "symbols": [f"{symbol.split('_')[0].lower()}_{symbol.split('_')[1].lower()}"]
        }))


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
                        elif dataJSON['channel'] == "book_lv2":
                            get_order_books_and_deltas(dataJSON, update=True)
                        elif dataJSON['channel'] == "book":
                            get_order_books_and_deltas(dataJSON, update=False)
                        else:
                            print(dataJSON)
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred")
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")


asyncio.run(main())
