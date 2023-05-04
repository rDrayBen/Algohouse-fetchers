import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.bequant.io/api/3"
API_SYMBOL = "/public/symbol"
WSS_URL = "wss://api.bequant.io/api/3/ws/public"
TIMEOUT = 0.05
PING_TIMEOUT = 29
async def subscribe(ws, data):
    id = 0
    for i in data:
        await ws.send(json.dumps({
        "method": "subscribe",
        "ch": "trades",
        "params": {
            "symbols": [i],
            "limit": 1
        },
        "id": id}))
        id += 1
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({
        "method": "subscribe",
        "ch": "orderbook/full",
        "params": {
            "symbols": [i]
        },
        "id": id
        }))
        id += 1
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({
        "method": "subscribe",
        "ch": "orderbook/top/100ms",
        "params": {
            "symbols": [i]
        },
        "id": id
        }))
        id += 1
        await asyncio.sleep(TIMEOUT)

async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({"ping":0}))
        await asyncio.sleep(PING_TIMEOUT)

async def meta(data):
    response = requests.get(API_URL + API_SYMBOL)
    for i in data:
        precission = str(response.json()[i]["quantity_increment"])[::-1].find(".")
        if precission == -1:
            print("@MD", i, "spot", response.json()[i]["base_currency"], response.json()[i]["quote_currency"], 0, 1, 1, 0, 0)
        else:
            print("@MD", i, "spot", response.json()[i]["base_currency"], response.json()[i]["quote_currency"], precission, 1, 1,
                  0, 0)
    print("@MDEND")

def print_trade(data):
    for i in data['update']:
        print("!", round(time.time() * 1000), i, data['update'][i][0]['s'][0].upper(), data['update'][i][0]['p'],
              data['update'][i][0]['q'], end='\n')

def print_orderbook(data, isSnapshot):
    if isSnapshot:
        for i in data['snapshot']:
            print("$", round(time.time() * 1000), i, "S",
                  "|".join(str(i[0])+ "@"+str(i[1]) for i in data['snapshot'][i]['a']),
                  "R", end="\n")
            print("$", round(time.time() * 1000), i, "B",
                  "|".join(str(i[0]) + "@" + str(i[1]) for i in data['snapshot'][i]['b']),
                  "R", end="\n")
    else:
        for i in data['data']:
            print("$", round(time.time() * 1000), i, "S", data['data'][i]['a'], end="\n")
            print("$", round(time.time() * 1000), i, "B", data['data'][i]['b'], end="\n")

async def main():
    try:
        response = requests.get(API_URL + API_SYMBOL)
        data = [i for i in response.json() if response.json()[i]['type'] == "spot" and
                response.json()[i]['status'] == "working"]
        meta_task = asyncio.create_task(meta(data))
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, data))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    data = await ws.recv()
                    dataJSON = json.loads(data)
                    try:
                        if dataJSON['ch'] == "trades":
                            if dataJSON["update"]:
                                print_trade(dataJSON)
                        if dataJSON['ch'] == "orderbook/full":
                            if dataJSON['snapshot']:
                                print_orderbook(dataJSON, 1)
                        if dataJSON['ch'] == "orderbook/top/100ms":
                            if dataJSON['data']:
                                print_orderbook(dataJSON, 0)
                        if dataJSON['error']:
                            continue
                    except:
                        continue
            except Exception:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())