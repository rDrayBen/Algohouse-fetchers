import json
import requests
import websockets
import time
import asyncio

# get all available symbol pairs from exchange
currency_url = 'https://api.exmo.com/v1.1/pair_settings'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
# base web socket url
WS_URL = 'wss://ws-api.exmo.com:443/v1/public'

# fill the list with all available symbols on the exchange
list_currencies = list(currencies.keys())


async def metadata():
    for pair in list_currencies:
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
    coin_name = message['topic'].split(':')[1].split('_')[0] + '-' + message['topic'].split(':')[1].split('_')[1]
    if 'data' in message:
        for elem in message['data']:
            print('!', get_unix_time(), coin_name,
                  elem['type'][0].upper(), elem['price'],
                  elem['amount'], flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    coin_name = message['topic'].split(':')[1].split('_')[0] + '-' + message['topic'].split(':')[1].split('_')[1]
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
    # subscribe for listed topics
    for symbol in list_currencies:
        # subscribe to all trades
        await ws.send(json.dumps({
            "method": "subscribe",
            "topics": [
                f"spot/trades:{symbol}",
                f"spot/order_book_snapshots:{symbol}",
                f"spot/order_book_updates:{symbol}"
            ]
        }))


async def main():
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL, ping_interval=None):
        try:
            sub_task = asyncio.create_task(subscribe(ws))
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))
            # print metadata about each pair symbols
            meta_data = asyncio.create_task(metadata())
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
                        elif 'order_book_snapshots' in dataJSON['topic']:
                            get_order_books_and_deltas(dataJSON, update=True)
                        # check if received data is about order books
                        elif 'order_book_updates' in dataJSON['topic']:
                            get_order_books_and_deltas(dataJSON, update=False)
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
