import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://openapi.tapbit.com/spot'
API_SPOT_SYMBOLS_URL = '/instruments/trade_pair_list'
WS_URL = 'wss://ws-pc.tapbit.com/stream/ws?compress=false'
WS_PUBLIC_SPOT_TRADE = 'spot/tradeList.'
WS_PUBLIC_SPOT_DEPTH_ORDERBOOK = 'spot/orderBook.'
TIMEOUT = 0.1
PING_TIMEOUT = 5
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['trade_pair_name'].replace('/', '') for x in response.json()['data']]
trades = [WS_PUBLIC_SPOT_TRADE + i for i in symbols]
orderbooks = [WS_PUBLIC_SPOT_DEPTH_ORDERBOOK + i for i in symbols]
old_trades = {}
for i in trades:
    old_trades[i] = 0

async def meta(response):
    for i in response.json()['data']:
        print("@MD", i['trade_pair_name'].replace('/', ''), "spot", i['base_asset'], i['quote_asset'], i['price_precision'],
              1, 1, 0, 0, end="\n")
    print("@MDEND")

async def heartbeat(ws):
    while True:
        await ws.send(message='pong')
        await asyncio.sleep(PING_TIMEOUT)

def print_trades(data):
    try:
        for i in data["data"]:
            print('!', round(time.time() * 1000), data["topic"][15:].upper(), i[1].upper(), i[0], i[2])
    except:
        pass

def print_orderbooks(data, is_snapshot):
    try:
        if is_snapshot:
            if data["data"][0]["asks"] != []:
                print('$', round(time.time() * 1000), data["topic"][15:].upper(), 'S', '|'.join(i[1] + '@' + i[0] for i in data["data"][0]["asks"]), 'R')
            if data["data"][0]["bids"] != []:
                print('$', round(time.time() * 1000), data["topic"][15:].upper(), 'B', '|'.join(i[1] + '@' + i[0] for i in data["data"][0]["bids"]), 'R')
        else:
            if data["data"][0]["asks"] != []:
                print('$', round(time.time() * 1000), data["topic"][15:].upper(), 'S', '|'.join(i[1] + '@' + i[0] for i in data["data"][0]["asks"]))
            if data["data"][0]["bids"] != []:
                print('$', round(time.time() * 1000), data["topic"][15:].upper(), 'B', '|'.join(i[1] + '@' + i[0] for i in data["data"][0]["bids"]))
    except:
        pass

async def subscribe(ws, symbols_):
    for i in range(len(symbols_)):
        await ws.send(message=json.dumps({"op": "subscribe",
                                          "args": [
                                              f"{WS_PUBLIC_SPOT_TRADE}{symbols_[i]}"
                                          ]
                                          }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({"op": "subscribe",
                                          "args": [
                                              f"{WS_PUBLIC_SPOT_DEPTH_ORDERBOOK}{symbols_[i]}.200"
                                          ]
                                          }))
        await asyncio.sleep(TIMEOUT)

print(len(symbols))
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
                    if data_json["topic"] in trades:
                        if old_trades[data_json["topic"]] < 2:
                            old_trades[data_json["topic"]] += 1
                        else:
                            print_trades(data_json)
                    if data_json["topic"] in orderbooks and data_json["action"] == "update":
                        print_orderbooks(data_json, 0)
                    if data_json["topic"] in orderbooks and data_json["action"] == "insert":
                        print_orderbooks(data_json, 1)
                except:
                    continue
        except:
            continue

asyncio.run(main())
