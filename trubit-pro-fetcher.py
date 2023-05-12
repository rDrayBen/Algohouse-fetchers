import json
import requests
import websockets
import time
import asyncio

# get all available symbol pairs from exchange
currency_url = 'https://api.mexo.io/openapi/v1/exchange'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
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
            pair_data = '@MD ' + pair['symbol'] + ' spot ' + pair['baseAsset'] + ' ' + pair['quoteAsset'] + ' ' \
                        + str(prec) + ' 1 1 0 0'
            print(pair_data, flush=True)
    print('@MDEND')


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# function to format the trades output
def get_trades(message):
    for elem in message['data']:
        print('!', get_unix_time(), message['symbol'],'B' if elem['m'] else 'S', elem['p'], elem['q'], flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
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
        await ws.send(json.dumps({
          "symbol": f"{symbol}",
          "topic": "diffDepth",
          "event": "sub",
          "params": {
            "binary": False
          }
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
                        pass
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred")
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")


# run main function
asyncio.run(main())
