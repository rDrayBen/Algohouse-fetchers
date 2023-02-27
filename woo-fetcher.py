import json
import requests
import websockets
import time
import asyncio

# get all available symbol pairs from exchange
currency_url = 'https://api.woo.org/v1/public/info'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
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
        print(pair_data, flush=True)
    print('@MDEND')


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# function to format the trades output
def get_trades(message):
    print('!', get_unix_time(), message['symbol'].split('_')[1] + '-' + message['symbol'].split('_')[2],
          message['side'][0].upper(), str('{0:.9f}'.format(message['price'])),
          str('{0:.9f}'.format(message['size'])), flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
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
        await asyncio.sleep(5)


async def subscribe(ws):
    # subscribe for listed topics
    for symbol in list_currencies:
        # subscribe to all trades
        await ws.send(json.dumps({
            "id": f"{UI}",
            "topic": f"SPOT_{symbol}@trade",
            "event": "subscribe"
        }))
        # subscribe to all order books
        await ws.send(json.dumps({
            "id": f"{UI}",
            "topic": f"SPOT_{symbol}@orderbook",
            "event": "subscribe"
        }))
        # subscribe to all order book updates
        await ws.send(json.dumps({
            "id": f"{UI}",
            "topic": f"SPOT_{symbol}@orderbookupdate",
            "event": "subscribe"
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

# run main function
asyncio.run(main())
