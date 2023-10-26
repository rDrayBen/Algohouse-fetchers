import json
import requests
import websockets
import time
import asyncio
import os

# get all available symbol pairs from exchange
currency_url = 'https://api.phemex.com/public/products'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
# base web socket url
WS_URL = 'wss://ws.phemex.com'


# fill the list with all available symbol pairs on exchange
for pair_s in currencies['data']['products']:
    list_currencies.append(pair_s['symbol'])


async def metadata():
    for pair in currencies['data']['products']:
        if pair['type'] == 'Spot':
            pair_data = '@MD ' + pair['displaySymbol'].split(' / ')[0] + pair['displaySymbol'].split(' / ')[1] + \
                        ' spot ' + pair['displaySymbol'].split(' / ')[0] + ' ' + \
                        pair['displaySymbol'].split(' / ')[1] + ' ' + str(pair['pricePrecision']) + ' 1 1 0 0'
            print(pair_data, flush=True)
    print('@MDEND')


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# function to format the trades output
def get_trades(message):
    # check if coin name has some trash symbols in it and removing them if occurs
    s_name = message['symbol'].replace('u', '')
    s_name = s_name.replace('с', '')
    s_name = s_name.replace('1', '')
    s_name = s_name.replace('0', '')

    for elem in message['trades']:
        print('!', get_unix_time(), s_name.replace('c', ''),
              elem[1][0], str('{0:.4f}'.format(elem[2]/10000)),
              str('{0:.4f}'.format(elem[3]/1000)), flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    # check if coin name has some trash symbols in it and removing them if occurs
    s_name = message['symbol'].replace('u', '')
    s_name = s_name.replace('с', '')
    s_name = s_name.replace('1', '')
    s_name = s_name.replace('0', '')

    # check if bids array is not Null
    if 'bids' in message['book'] and message['book']['bids']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + s_name.replace('c', '') + ' B '
        pq = '|'.join(f"{str('{0:.5f}'.format(elem[1] / 1000))}@{str('{0:.5f}'.format(elem[0] / 10000))}"
                      for elem in message['book']['bids'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


    # check if asks array is not Null
    if 'asks' in message['book'] and message['book']['asks']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + s_name.replace('c', '') + ' S '
        pq = '|'.join(f"{str('{0:.5f}'.format(elem[1] / 1000))}@{str('{0:.5f}'.format(elem[0] / 10000))}"
                      for elem in message['book']['asks'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
          "id": 0,
          "method": "server.ping",
          "params": []
        }))
        print(json.dumps({
            "topic": "ping"
        }))
        await asyncio.sleep(10)


async def subscribe(ws):
    # subscribe for listed topics
    for symbol in list_currencies:
        await ws.send(json.dumps({
            "id": 1,
            "method": "trade.subscribe",
            "params": [
                symbol
            ]
        }))
        if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
            await ws.send(json.dumps({
              "id": 1,
              "method": "orderbook.subscribe",
              "params": [
                symbol,
                True
              ]
            }))


async def main():
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL):
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
                    # print(dataJSON)
                    # check if received data is about trades
                    if 'type' in dataJSON:
                        if 'trades' in dataJSON and dataJSON['type'] == 'incremental':
                            get_trades(dataJSON)
                        # skip history trade data
                        elif 'trades' in dataJSON and dataJSON['type'] == 'snapshot':
                            pass
                        # check if received data is about updates on order book
                        elif 'book' in dataJSON and dataJSON['type'] == 'incremental':
                            get_order_books_and_deltas(dataJSON,
                                                       update=True)
                        # check if received data is about order books
                        elif 'book' in dataJSON and dataJSON['type'] == 'snapshot':
                            get_order_books_and_deltas(dataJSON,
                                                       update=False)
                        else:
                            print(dataJSON)
                    elif 'error' in dataJSON:
                        pass
                    else:
                        print(dataJSON)
                except Exception as e:
                    pass
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")


# run main function
asyncio.run(main())
