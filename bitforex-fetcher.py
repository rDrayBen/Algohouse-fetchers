import json
import requests
import websockets
import time
import asyncio
from datetime import datetime

# get all available symbol pairs from exchange
currency_url = 'https://api.bitforex.com/api/v1/market/symbols'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
# base web socket url
WS_URL = 'wss://www.bitforex.com/mkapi/coinGroup1/ws'

# fill the list with all available symbol pairs on exchange
for pair in currencies['data']:
    list_currencies.append(pair['symbol'])


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# function to format the trades output
def get_trades(message):
    for elem in message['data']:
        print('!', get_unix_time(), message['param']['businessType'].split('-')[2].upper() \
              + '-' + message['param']['businessType'].split('-')[1].upper(),
              'B' if elem['direction'] == 1 else 'S', str('{0:.9f}'.format(elem['price'])),
              str('{0:.7f}'.format(elem['amount'])), flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
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
        await ws.send(json.dumps({
            "event": "ping"
        }))
        await asyncio.sleep(5)


async def subscribe(ws):
    # subscribe for listed topics
    for symbol in list_currencies:
        # subscribe to all trades
        await ws.send(json.dumps([
            {"type": "subHq",
             "event": "trade",
             "param": {"businessType": symbol,
                       "size": 1}
             },
            {"type": "subHq",
             "event": "depth10",
             "param": {"businessType": symbol,
                       "dType": 1}},
            {"type": "subHq",
             "event": "depth10",
             "param": {"businessType": symbol,
                       "dType": 0}}
        ]))
        await asyncio.sleep(0.1)


async def main():
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL, ping_interval=None):
        try:
            sub_task = asyncio.create_task(subscribe(ws))
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))
            while True:
                # receiving data from server
                data = await ws.recv()
                # change format of received data to json format
                dataJSON = json.loads(data)

                try:
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
