import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.mexc.com"
API_SYMBOLS = "/api/v3/defaultSymbols"
API_SYMBOLS_PRECISSION = "/api/v3/exchangeInfo"
WS_URL = "wss://wbs.mexc.com/ws"
TIMEOUT = 0.001
PING_TIMEOUT = 5

async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps({
        "method": "SUBSCRIPTION",
        "params": [
            f"spot@public.deals.v3.api@{i}"
        ]
        }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({
            "method": "SUBSCRIPTION",
            "params": [
                f"spot@public.increase.depth.v3.api@{i}"
            ]
        }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({
            "method": "SUBSCRIPTION",
            "params": [
                f"spot@public.limit.depth.v3.api@{i}@20"
            ]
        }))
        await asyncio.sleep(TIMEOUT)

async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({"method":"PING"}))
        await asyncio.sleep(PING_TIMEOUT)

async def meta():
    response = requests.get(API_URL + API_SYMBOLS_PRECISSION)
    for i in response.json()['symbols']:
        print("@MD", i['symbol'], "spot", i['baseAsset'], i['quoteAsset'], i['quotePrecision'], 1, 1, 0, 0, end="\n")
    print("@MDEND")

def print_trades(data):
    for i in data['d']['deals']:
        if i['S'] == 1:
            print("!", round(time.time() *1000), data['s'], "B", i['p'], i['v'], end='\n')
        if i['S'] == 2:
            print("!", round(time.time() * 1000), data['s'], "S", i['p'], i['v'], end='\n')

def print_orderbooks(data, is_snapshot):
    if is_snapshot:
        if 'bids' in data['d']:
            print("$", round(time.time()*1000), data['s'], 'B', "|".join(i['v']+"@"+i['p'] for i in data['d']['bids']),
                  "R",end='\n')
        if 'asks' in data['d']:
            print("$", round(time.time()*1000), data['s'], 'S', "|".join(i['v']+"@"+i['p'] for i in data['d']['asks']),
                  "R",end='\n')
    else:
        if 'bids' in data['d']:
            print("$", round(time.time()*1000), data['s'], 'B', "|".join(i['v']+"@"+i['p'] for i in data['d']['bids']),
                  end='\n')
        if 'asks' in data['d']:
            print("$", round(time.time()*1000), data['s'], 'S', "|".join(i['v']+"@"+i['p'] for i in data['d']['asks']),
                  end='\n')

async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = response.json()['data']
        trades_channels = ["spot@public.deals.v3.api@" + i for i in symbols]
        snapshot_channels = ["spot@public.limit.depth.v3.api@" + i + "@20" for i in symbols]
        delta_channels = ["spot@public.increase.depth.v3.api@" + i for i in symbols]
        meta_task = asyncio.create_task(meta())
        async for ws in websockets.connect(WS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    data = await ws.recv()
                    dataJSON = json.loads(data)
                    try:
                        if dataJSON['c'] in trades_channels:
                            print_trades(dataJSON)
                        elif dataJSON['c'] in snapshot_channels:
                            print_orderbooks(dataJSON, 1)
                        elif dataJSON['c'] in delta_channels:
                            print_orderbooks(dataJSON, 0)
                    except Exception:
                        continue
            except Exception:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())