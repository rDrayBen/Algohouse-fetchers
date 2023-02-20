import requests
import json
import asyncio
import websockets
import time

# get all available instruments from exchange
WS_URL = 'wss://socket.delta.exchange'
currency_url = 'https://api.delta.exchange/v2/indices'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()


# list of all instruments
for elem in currencies['result']:
    if elem['config'] is not None and 'quoting_asset' in elem['config'] and 'underlying_asset' in elem['config']:
        list_currencies.append(elem['config']['underlying_asset'] + elem['config']['quoting_asset'])


def get_unix_time():
    return round(time.time() * 1000)


# format trades output
def get_trades(message):
    trade_data = message
    def_str = "! " + str(get_unix_time()) + f" {trade_data['symbol']}" + \
              f"{ ' B' if trade_data['seller_role'] == 'maker' else ' S'}" + f" {trade_data['price']}" + f" {trade_data['size']}"
    print(def_str)


# format order books and deltas output
def get_order_books_and_deltas(message):
    order_data = message
    if 'asks' in order_data and order_data['asks']:
        def_str = f"$ {get_unix_time()} {order_data['symbol']} S "
        pq = "|".join(f"{elem[0]}@{elem[1]}" for elem in order_data['asks'])
        if order_data['action'] == 'snapshot':
            print(def_str + pq + ' R')
        elif order_data['action'] == 'update':
            print(def_str + pq)

    if 'bids' in order_data and order_data['bids']:
        def_str = f"$ {get_unix_time()} {order_data['symbol']} B "
        pq = "|".join(f"{elem[0]}@{elem[1]}" for elem in order_data['bids'])
        if order_data['action'] == 'snapshot':
            print(def_str + pq + ' R')
        elif order_data['action'] == 'update':
            print(def_str + pq)


# subscribe to all trades and order books
async def subscribe(ws):
    await ws.send(json.dumps({
        "type": "subscribe",
        "payload": {
            "channels": [
                {
                    "name": "all_trades",
                    "symbols": ["all"]
                }
            ]
        }
    }))

    for curr in list_currencies:
        await ws.send(json.dumps({
            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name": "l2_updates",
                        "symbols": [curr]
                    }
                ]
            }
        }))


async def main():
    # create connection with server
    async for ws in websockets.connect(WS_URL, ping_interval=None):

        task = asyncio.create_task(subscribe(ws))

        try:
            while True:
                # receive data
                data = await ws.recv()
                try:
                    dataJSON = json.loads(data)
                    # check if data is valid
                    if 'type' in dataJSON:
                        # check if data is about trades
                        if dataJSON['type'] == 'all_trades':
                            get_trades(dataJSON)
                        # check if data is about order books
                        if dataJSON['type'] == 'l2_updates':
                            get_order_books_and_deltas(dataJSON)
                    else:
                        print(dataJSON)

                except Exception as e:
                    print(f"Error: {e}")

        # keep connection alive
        except websockets.exceptions.ConnectionClosedOK as e:
            print(f"Warning: {e}")


asyncio.run(main())
