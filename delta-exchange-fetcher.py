import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://cdn.deltaex.org/v2'
API_SPOT_SYMBOLS_URL = '/tickers?contract_types=spot'
WS_URL = 'wss://socket.deltaex.org/'
WS_PUBLIC_SPOT_TRADE = 'all_trades'
WS_PUBLIC_SPOT_ORDERBOOKS = 'l2_updates'

TIMEOUT = 0.1
PING_TIMEOUT = 25
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['symbol'] for x in response.json()['result']]


def meta():
    for symbol in symbols:
        assets = symbol.split('_')
        print("@MD", symbol, "spot", assets[0],
              assets[1], -1, 1, 1, 0, 0, end="\n")
    print("@MDEND")


def print_trades(data):
    try:
        if data['seller_role'] == 'maker':
            bs = 'B'
        else:
            bs ='S'
        print('!', round(time.time() * 1000),
                data['symbol'], bs, data['price'], data['size'])
    except:
        pass


def print_orderbooks(data, is_snapshot):
    try:
        if is_snapshot:
            if data["asks"] != []:
                print('$', round(time.time() * 1000), data['symbol'], 'S', '|'.join(
                    i[1] + '@' + i[0] for i in data["asks"]), 'R')
            if data["bids"] != []:
                print('$', round(time.time() * 1000), data['symbol'], 'B', '|'.join(
                    i[1] + '@' + i[0] for i in data["bids"]), 'R')
        else:
            if data["asks"] != []:
                print('$', round(time.time() * 1000), data['symbol'], 'S', '|'.join(
                    i[1] + '@' + i[0] for i in data["asks"]))
            if data["bids"] != []:
                print('$', round(time.time() * 1000), data['symbol'], 'B', '|'.join(
                    i[1] + '@' + i[0] for i in data["bids"]))
    except:
        pass


async def subscribe(ws, symbols_):
    await ws.send(json.dumps({"type": "enable_heartbeat"}))
    await ws.send(json.dumps({
            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name": WS_PUBLIC_SPOT_TRADE,
                        "symbols": symbols_
                    }
                ]
            }
        }))
    await asyncio.sleep(TIMEOUT)
    await ws.send(json.dumps({
            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name": WS_PUBLIC_SPOT_ORDERBOOKS,
                        "symbols": symbols_
                    }
                ]
            }
        }))


async def main():
    meta()
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbols))
            while True:
                try:
                    data = await ws.recv()
                    data_json = json.loads(data)
                    if data_json['type'] == 'all_trades':
                        print_trades(data_json)
                    if data_json['action'] == 'update':
                        print_orderbooks(data_json, 0)
                    if data_json['action'] == 'snapshot':
                        print_orderbooks(data_json, 1)
                except:
                    continue
        except:
            continue

asyncio.run(main())
