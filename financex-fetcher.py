import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://financex.trade/api/v2'
API_SPOT_SYMBOLS_URL = '/trade/public/markets'
WS_URL = 'wss://financex.trade/api/v2/ranger/public/'
WS_PUBLIC_SPOT_TRADE = '.trades'
WS_PUBLIC_SPOT_DEPTH_DELTA = '.ob-inc'
WS_PUBLIC_SPOT_DEPTH_WHOLE = '.ob-snap'
TIMEOUT = 0.1
PING_TIMEOUT = 25
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['id'] for x in response.json()]
symbol_names = {x['id']: x['name'] for x in response.json()}
trades = [i + WS_PUBLIC_SPOT_TRADE for i in symbols]
deltas = [i + WS_PUBLIC_SPOT_DEPTH_DELTA for i in symbols]
snapshots = [i + WS_PUBLIC_SPOT_DEPTH_WHOLE for i in symbols]


async def meta(response):
    for i in response.json():
        print("@MD", i['name'], "spot", i['base_unit'].upper(), i['quote_unit'].upper(), i['price_precision'],
              1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message='ping')
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    try:
        path = list(data.keys())[0]
        symbol = symbol_names[path.split('.')[0]]
        for i in data[path]['trades']:
            if i['taker_type'] == 'buy':
                bs = 'B'
            else:
                bs = 'S'
            print('!', round(time.time() * 1000), symbol, bs, i["price"], i["amount"])
    except:
        pass


def print_orderbooks(data, is_snapshot):
    try:
        path = list(data.keys())[0]
        symbol = symbol_names[path.split('.')[0]]
        if is_snapshot:
            if 'asks' in list(data[path].keys()):
                print('$', round(time.time() * 1000), symbol, 'S', '|'.join(
                    i[1] + '@' + i[0] for i in data[path]["asks"]), 'R')
            if 'bids' in list(data[path].keys()):
                print('$', round(time.time() * 1000), symbol, 'B', '|'.join(
                    i[1] + '@' + i[0] for i in data[path]["bids"]), 'R')
        else:
            if 'asks' in list(data[path].keys()):
                price = data[path]['asks'][0] if data[path]['asks'][0] != '' else '0'
                amount = data[path]['asks'][1] if data[path]['asks'][1] != '' else '0'
                print('$', round(time.time() * 1000), symbol, 'S', amount + '@' + price)
            if 'bids' in list(data[path].keys()):
                price = data[path]['bids'][0] if data[path]['bids'][0] != '' else '0'
                amount = data[path]['bids'][1] if data[path]['bids'][1] != '' else '0'
                print('$', round(time.time() * 1000), symbol, 'B', amount + '@' + price)
    except:
        pass


async def subscribe(ws, symbols_):
    for x in symbols_:
        await ws.send(message=json.dumps({"event":"subscribe",
                                        "streams":[f'{x}{WS_PUBLIC_SPOT_TRADE}', f'{x}{WS_PUBLIC_SPOT_DEPTH_DELTA}']}))
        await asyncio.sleep(TIMEOUT)


async def main():
    meta_task = asyncio.create_task(meta(response))
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbols))
            ping_task = asyncio.create_task(heartbeat(ws))
            while True:
                try:
                    data = await ws.recv()
                    data_json = json.loads(data)
                    if WS_PUBLIC_SPOT_TRADE in list(data_json.keys())[0]:
                        print_trades(data_json)
                    if WS_PUBLIC_SPOT_DEPTH_DELTA in list(data_json.keys())[0]:
                        print_orderbooks(data_json, 0)
                    if WS_PUBLIC_SPOT_DEPTH_WHOLE in list(data_json.keys())[0]:
                        print_orderbooks(data_json, 1)
                except:
                    continue
        except:
            continue


asyncio.run(main())
