import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.bololex.com/api"
API_SYMBOLS = "/symbols"
WSS_URL = "wss://api.bololex.com/ws"
TIMEOUT = 0.001
PING_TIMEOUT = 6
async def subscribe(ws, data):
    id_ = 1
    for i in data:
        await ws.send(json.dumps({
            "method": "subscribeBook",
            "params": {
                "symbol": i
            },
            "id": id_
        }))
        id_ += 1
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({
            "method": "subscribeTrades",
            "params": {
                "symbol": i,
                "limit": 100
            },
            "id": id_
        }))
        id_ += 1
        await asyncio.sleep(TIMEOUT)

async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({}))
        await asyncio.sleep(PING_TIMEOUT)

async def meta(data):
    for i in data:
        print("@MD", i['symbol'], "spot", i['base'], i['quote'], i['totalPrecision'], 1, 1, 0, 0, end="\n")
    print("@MDEND")

def print_trade(data):
    print("!", round(time.time() * 1000), data['result']['pair'], data['result']['side'][0],
          data['result']['price'], data['result']['quantity'], end="\n")

def print_orderbook(data, isSnapshot):
    if isSnapshot:
        if data['result']['buy'] != []:
            print("$", round(time.time() * 1000), data['result']['symbol'], "B",
                  "|".join(i['quantity'] + "@" + i['price'] for i in data['result']['buy']), "R", end="\n")
        if data['result']['sell'] != []:
            print("$", round(time.time() * 1000), data['result']['symbol'], "S",
                  "|".join(i['quantity'] + "@" + i['price'] for i in data['result']['sell']), "R", end="\n")
    else:
        if data['result']['buy'] != []:
            print("$", round(time.time() * 1000), data['result']['symbol'], "B",
                  "|".join(i['quantity'] + "@" + i['price'] for i in data['result']['buy']), end="\n")
        if data['result']['sell'] != []:
            print("$", round(time.time() * 1000), data['result']['symbol'], "S",
                  "|".join(i['quantity'] + "@" + i['price'] for i in data['result']['sell']), end="\n")

async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['symbol'] for i in response.json()['result'] if i['isActive']]
        meta_task = asyncio.create_task(meta(response.json()['result']))
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                while True:
                    data = await ws.recv()
                    dataJSON = json.loads(data)
                    try:
                        if dataJSON['method'] == 'snapshotBook':
                            print_orderbook(dataJSON, 1)
                        if dataJSON['method'] == "newTrade":
                            print_trade(dataJSON)
                        if dataJSON['method'] == "bookUpdate":
                            print_orderbook(dataJSON, 0)
                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())