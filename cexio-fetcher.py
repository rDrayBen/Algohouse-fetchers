import json
import requests
import asyncio
import time
import websockets
import random

API_URL = 'https://api.plus.cex.io/rest-public'
API_SPOT_SYMBOLS_URL = '/get_pairs_info'
WS_URL = 'wss://api.plus.cex.io/ws-public'
WS_PUBLIC_SPOT_TRADE = 'trade_subscribe'
WS_PUBLIC_SPOT_DEPTH_DELTA = 'order_book_increment'
WS_PUBLIC_SPOT_DEPTH_WHOLE = 'order_book_subscribe'
TIMEOUT = 5
PING_TIMEOUT = 5
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['base'].upper() + '-' + x['quote'].upper()
           for x in response.json()['data']]
trades = 'tradeUpdate'
deltas = WS_PUBLIC_SPOT_DEPTH_DELTA
snapshots = WS_PUBLIC_SPOT_DEPTH_WHOLE


async def meta(response):
    for i in response.json()['data']:
        print("@MD", (i['base'].upper() + '-' + i['quote'].upper()), "spot", i['base'].upper(), i['quote'].upper(), i['quotePrecision'],
              1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message=json.dumps({"e": "ping"}))
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    try:
        print('!', round(time.time() * 1000),
              data["data"]["pair"].upper(), data["data"]["side"].upper(), data["data"]["price"], data["data"]["amount"])
    except:
        pass


def print_orderbooks(data, is_snapshot):
    try:
        if is_snapshot:
            if data["data"]["asks"] != []:
                print('$', round(time.time() * 1000), data["data"]["pair"].upper(), 'S', '|'.join(
                    i[1] + '@' + i[0] for i in data["data"]["asks"]), 'R')
            if data["data"]["bids"] != []:
                print('$', round(time.time() * 1000), data["data"]["pair"].upper(), 'B', '|'.join(
                    i[1] + '@' + i[0] for i in data["data"]["bids"]), 'R')
        else:
            if data["data"]["asks"] != []:
                print('$', round(time.time() * 1000), data["data"]["pair"].upper(), 'S', '|'.join(
                    i[1] + '@' + i[0] for i in data["data"]["asks"]))
            if data["data"]["bids"] != []:
                print('$', round(time.time() * 1000), data["data"]["pair"].upper(), 'B', '|'.join(
                    i[1] + '@' + i[0] for i in data["data"]["bids"]))
    except:
        pass


async def subscribe(ws, symbols_):
    for i in range(len(symbols_)):
        await ws.send(message=json.dumps({
            "e": f"{WS_PUBLIC_SPOT_TRADE}",
            "oid": f"{random.randint(1, 9999999999)}_{WS_PUBLIC_SPOT_TRADE}",
            "data": {
                "pair": f"{symbols_[i]}",
            },
        })) 
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({
            "e": f"{WS_PUBLIC_SPOT_DEPTH_WHOLE}",
            "oid": f"{random.randint(1, 9999999999)}_{WS_PUBLIC_SPOT_DEPTH_WHOLE}",
            "data": {
                "pair": f"{symbols_[i]}",
            },
        }))
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
                    if data_json['e'] == trades:
                        print_trades(data_json)
                    if data_json['e'] == deltas:
                        print_orderbooks(data_json, 0)
                    if data_json['e'] == snapshots:
                        print_orderbooks(data_json, 1)        
                except:
                    continue
        except:
            continue

asyncio.run(main())
