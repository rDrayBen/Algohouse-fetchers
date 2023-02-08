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


def get_unix_time():
    return round(time.time() * 1000)


def get_trades(message):
    trade_data = message
    if 'data' in trade_data:
        for elem in trade_data['data']:
            print('!', get_unix_time(), elem['symbol'].split('_')[0] + '-' + elem['symbol'].split('_')[1],
                  elem['takerSide'][0].upper(), elem['price'],
                  float(elem['amount']) + float(elem['quantity']), flush=True)


def get_order_books_and_deltas(message):
    order_data = message
    if 'data' in order_data:
        if order_data['data'][0]['bids']:
            order_answer = '$ ' + str(get_unix_time()) + ' ' + order_data['data'][0]['symbol'].split('_')[0] + '-' + \
                           order_data['data'][0]['symbol'].split('_')[1] + ' B '
            pq = '|'.join(f"{elem[1]}@{elem[0]}"
                          for elem in order_data['data'][0]['bids'])
            if order_data['action'] == "snapshot":
                print(order_answer + pq + ' R', flush=True)
            elif order_data['action'] == "update":
                print(order_answer + pq, flush=True)

        order_answer = ''
        pq = ''
        if order_data['data'][0]['asks']:
            order_answer = '$ ' + str(get_unix_time()) + ' ' + order_data['data'][0]['symbol'].split('_')[0] + '-' + \
                           order_data['data'][0]['symbol'].split('_')[1] + ' S '
            pq = '|'.join(f"{elem[1]}@{elem[0]}"
                          for elem in order_data['data'][0]['asks'])
            if order_data['action'] == "snapshot":
                print(order_answer + pq + ' R', flush=True)
            elif order_data['action'] == "update":
                print(order_answer + pq, flush=True)


async def main():
    amount = 0

    async with websockets.connect(WS_URL, ping_interval=None) as ws:
        await ws.send(json.dumps({
            "event": "subscribe",
            "channel": ["trades"],
            "symbols": ["all"]
        }))

        for symbol in list_currencies:

            await ws.send(json.dumps({
                "event": "subscribe",
                "channel": ["book_lv2"],
                "symbols": [f"{symbol.split('_')[0].lower()}_{symbol.split('_')[1].lower()}"]
            }))

        while True:
            time.sleep(0.01)
            data = await ws.recv()
            try:

                dataJSON = json.loads(data)
                if dataJSON['channel'] and dataJSON['channel'] == "trades":
                    get_trades(dataJSON)
                elif dataJSON['channel'] and dataJSON['channel'] == "book_lv2":
                    get_order_books_and_deltas(dataJSON)
                await ws.send(json.dumps({
                    "event": "ping"
                }))
            except:
                pass


asyncio.run(main())
