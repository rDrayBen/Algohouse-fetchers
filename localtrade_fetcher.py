import requests
import json
import asyncio
import websockets
import time

# get all available instruments from exchange
currency_url = 'https://api.localtrade.cc/api/v1/public/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://localtrade.cc/ws'

# list of all instruments
for currency in currencies['result']:
    list_currencies.append(currency['name'])
print(list_currencies)


def get_unix_time():
    return round(time.time() * 1000)


# format trades output
def get_trades(message):
    trade_data = message
    if 'params' in trade_data and len(trade_data['params']) > 1 and 'params'[1]:
        default_str = '!' + ' ' + str(get_unix_time()) + ' ' + trade_data['params'][0].split('_')[0] + '-' + trade_data['params'][0].split('_')[1]
        for elem in trade_data['params'][1]:
            res_str = default_str + f" {elem['type'][0].upper()} {elem['price']} {elem['amount']}"
            print(res_str)


# format order books and deltas output
def get_order_books_and_deltas(message):
    order_data = message
    # check if data is valid
    if 'params' in order_data and len(order_data['params']) > 0:
        # check if there is orders for SALE
        if 'asks' in order_data['params'][1]:
            # format output
            order_answer = '$ ' + str(get_unix_time()) + ' ' + order_data['params'][-1] + ' S '
            pq = '|'.join(f"{elem[0]}@{elem[1]}" for elem in order_data['params'][1]['asks'])
            if order_data['params'][0]:
                print(order_answer + pq + ' R')
            else:
                print(order_answer + pq)

        # check if there is orders for BUY
        if 'bids' in order_data['params'][1]:
            # format output
            order_answer = '$ ' + str(get_unix_time()) + ' ' + order_data['params'][-1] + ' B '
            pq = '|'.join(f"{elem[0]}@{elem[1]}" for elem in order_data['params'][1]['bids'])
            if order_data['params'][0]:
                print(order_answer + pq + ' R')
            else:
                print(order_answer + pq)


# subscribe to all trades and order books
async def subscribe(ws):
    await ws.send(json.dumps({
        "method": "deals.subscribe",
        "params": list_currencies
    }))

    for currency in list_currencies:
        await ws.send(json.dumps({
            "method": "depth.subscribe",
            "params":
                [
                    currency,  # market
                    100,  # limit
                    "0"  # interval
                ]
        }))
        await asyncio.sleep(0.1)


async def main():
    # create connection with server
    async for ws in websockets.connect(WS_URL):
        
        task = asyncio.create_task(subscribe(ws))

        try:
            while True:
                # receive data
                data = await ws.recv()
                try:
                    dataJSON = json.loads(data)
                    # check if data is valid
                    if 'method' in dataJSON:
                        # check if data is about trades
                        if dataJSON['method'] == 'deals.update':
                            get_trades(dataJSON)
                        # check if data is about order books
                        if dataJSON['method'] == 'depth.update':
                            get_order_books_and_deltas(dataJSON)

                except Exception as e:
                    print(f"Error: {e}")

        # keep connection alive
        except websockets.exceptions.ConnectionClosedOK as e:
            continue


asyncio.run(main())
