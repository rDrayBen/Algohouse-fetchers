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


# function to get current time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# function to format the trades output
def get_trades(message):
    coin_name = message['topic'].split(':')[1].split('_')[0] + '-' + message['topic'].split(':')[1].split('_')[1]
    for elem in message['data']:
        print('!', get_unix_time(), coin_name,
              elem['type'][0].upper(), elem['price'],
              elem['amount'], flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    coin_name = message['topic'].split(':')[1].split('_')[0] + '-' + message['topic'].split(':')[1].split('_')[1]
    # check if bids array is not Null
    if message['data']['bid']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + coin_name + ' B '
        pq = '|'.join(f"{elem[0]}@{elem[2]}"
                      for elem in message['data']['bid'])
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)

    order_answer = ''
    pq = ''
    # check if asks array is not Null
    if message['data']['ask']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + coin_name + ' S '
        pq = '|'.join(f"{elem[0]}@{elem[2]}"
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
    try:
        # create connection with server via base ws url
        async with websockets.connect(WS_URL, ping_interval=None) as ws:
            sub_task = asyncio.create_task(subscribe(ws))
            await sub_task
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))
            while True:
                # receiving data from server
                data = await ws.recv()
                # change format of received data to json format
                dataJSON = json.loads(data)
                try:
                    # check if received data is about trades
                    if 'trades' in dataJSON['topic']:
                        get_trades(dataJSON)
                    # check if received data is about updates on order book
                    elif dataJSON['event'] == 'update':
                        get_order_books_and_deltas(dataJSON, update=True)
                    # check if received data is about order books
                    elif dataJSON['event'] == 'snapshot':
                        get_order_books_and_deltas(dataJSON, update=False)
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred")
                    ws.close()
    except Exception as conn_e:
        print(f"WARNING: connection exception {conn_e} occurred")


# run main function
asyncio.run(main())
