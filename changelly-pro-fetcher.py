import json
import requests
import websockets
import time
import asyncio
import os

# get all available symbol pairs from exchange
currency_url = 'https://api.pro.changelly.com/api/3/public/symbol'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
# base web socket url
WS_URL = 'wss://api.pro.changelly.com/api/3/ws/public'
precision = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000,
             1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000]


# fill the list with all available symbol pairs on exchange
for pair_s in currencies:
    list_currencies.append(pair_s)


async def metadata():
    for pair in list_currencies:
        curr = currencies[pair]
        if curr['type'] == 'spot' and curr['status'] == 'working':
            prec = 11
            for i in range(len(precision)):
                if float(curr['tick_size']) * precision[i] == 1:
                    prec = i
            pair_data = '@MD ' + pair + ' spot ' + curr['base_currency'] + ' ' + curr['quote_currency'] + ' ' + \
                        str(prec) + ' 1 1 0 0'
            print(pair_data, flush=True)
    print('@MDEND')


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


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
        for elem in list(message['update'].values())[0]:
            print('!', get_unix_time(), coin_name,
                  elem['s'][0].upper(), elem['p'],
                  elem['q'], flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, coin_name, update):
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
                    elif 'error' in dataJSON:
                        pass
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred")
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")


# run main function
asyncio.run(main())
