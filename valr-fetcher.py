import json
import requests
import websockets
import time
import asyncio

# get all available symbol pairs from exchange
currency_url = 'https://api.valr.com/v1/public/pairs'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
# base web socket url
WS_URL = 'wss://api.valr.com/ws/trade'

# fill the list with all available symbol pairs on exchange
for pair_s in currencies:
    if pair_s['active']:
        list_currencies.append(pair_s['symbol'])


async def metadata():
    for pair in currencies:
        if pair['active']:
            pair_data = '@MD ' + pair['symbol'] + ' spot ' + pair['baseCurrency'] + ' ' + pair['quoteCurrency'] + \
                        ' ' + pair['baseDecimalPlaces'] + ' 1 1'
            print(pair_data, flush=True)
    print('@MDEND')


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# function to format the trades output
def get_trades(message):
    print('!', get_unix_time(), message['currencyPairSymbol'],
          message['data']['takerSide'][0].upper(), message['data']['price'],
          message['data']['quantity'], flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    # check if bids array is not Null
    if 'Bids' in message['data'] and message['data']['Bids']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['currencyPairSymbol'] + ' B '
        pq = ''
        for order in message['data']['Bids']:
            for quan in order['Orders']:
                pq += order['Price'] + '@' + quan['quantity'] + '|'
        pq = pq[:-1]
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


    # check if asks array is not Null
    if 'Asks' in message['data'] and message['data']['Asks']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['currencyPairSymbol'] + ' S '
        pq = ''
        for order in message['data']['Asks']:
            for quan in order['Orders']:
                pq += order['Price'] + '@' + quan['quantity'] + '|'
        pq = pq[:-1]
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
            "type": "PING"
        }))
        await asyncio.sleep(20)


async def subscribe(ws):
    # subscribe for listed topics
    await ws.send(json.dumps({
        "type": "SUBSCRIBE",
        "subscriptions": [
            {
                "event": "NEW_TRADE",
                "pairs": list_currencies
            },
            {
                "event": "FULL_ORDERBOOK_UPDATE",
                "pairs": list_currencies
            }
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
                        if dataJSON['type'] == 'NEW_TRADE':
                            get_trades(dataJSON)
                        # check if received data is about updates on order book
                        elif dataJSON['type'] == 'FULL_ORDERBOOK_UPDATE':
                            get_order_books_and_deltas(dataJSON,
                                                       update=True)
                        # check if received data is about order books
                        elif dataJSON['type'] == 'FULL_ORDERBOOK_SNAPSHOT':
                            get_order_books_and_deltas(dataJSON,
                                                       update=False)
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
