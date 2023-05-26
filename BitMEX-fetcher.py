import json
import websockets
import time
import asyncio
import requests

API_URL = "https://www.bitmex.com/api/v1/"
API_SYMBOLS = "/instrument"
WS_URL = "wss://ws.bitmex.com/realtime"
TIMEOUT = 0.001
PING_TIMEOUT = 5
async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps
            ({
            "op": "subscribe",
            "args": [i]
        }))
        await asyncio.sleep(TIMEOUT)
async def heartbeat(ws):
    pass

async def meta(pairs_api_response):
    for i in pairs_api_response:
        if i['typ'] == "FFWCSX":
            if i['quoteCurrency'] == "XXX":
                continue
            precission = str(i["tickSize"])[::-1].find(".")
            if precission == -1:
                print("@MD", i['symbol'], "spot", i["rootSymbol"], i["quoteCurrency"], 0,
                      1, 1, 0, 0, end="\n")
            else:
                print("@MD", i['symbol'], "spot", i["rootSymbol"], i["quoteCurrency"], precission,
                      1, 1, 0, 0, end="\n")
    print("@MDEND")

def print_trades(data):
    for i in range(len(data['data'])):
        print("!", round(time.time() * 1000), data['data'][i]['symbol'], data['data'][i]['side'][0], data['data'][i]['price'],
              data['data'][i]['size'], end="\n")

def print_orderbook(data, isSnapshot):
    asks = []
    bids = []
    for i in data['data']:
        if i['side'] == "Sell":
            asks.append(i)
        if i['side'] == "Buy":
            bids.append(i)
    if isSnapshot == 1:
        if bids != []:
            print("$", round(time.time() *1000), bids[0]['symbol'], "B",
                  "|".join(str(i["size"]) + "@" + str(i["price"]) for i in bids), "R", end="\n")
        if asks != []:
            print("$", round(time.time() * 1000), asks[0]['symbol'], "S",
                  "|".join(str(i["size"]) + "@" + str(i["price"]) for i in asks), "R", end="\n")

    elif isSnapshot == 0:
        if bids != []:
            print("$", round(time.time() * 1000), bids[0]['symbol'], "B",
                  "|".join(str(i["size"]) + "@" + str(i["price"]) for i in bids), end="\n")
        if asks != []:
            print("$", round(time.time() * 1000), asks[0]['symbol'], "S",
                  "|".join(str(i["size"]) + "@" + str(i["price"]) for i in asks), end="\n")

async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['symbol'] for i in response.json() if i['typ'] == "FFWCSX"]
        trade_channel = ["trade:" + i for i in symbols]
        snapshot_channel = ["orderBookL2_25:"+i for i in symbols]
        meta_task = asyncio.create_task(meta(response.json()))
        async for ws in websockets.connect(WS_URL):
            try:
                trade_task = asyncio.create_task(subscribe(ws, trade_channel))
                snapshot_task = asyncio.create_task(subscribe(ws,  snapshot_channel))
                heartbeat_task = asyncio.create_task(heartbeat(ws))
                while True:
                    data = await ws.recv()
                    dataJSON = json.loads(data)
                    try:
                        if dataJSON["table"] == "trade":
                            if dataJSON["action"] == "partial":
                                continue
                            else:
                                print_trades(dataJSON)

                        if dataJSON["table"] == "orderBookL2_25":
                            if dataJSON['action'] == "partial":
                                print_orderbook(dataJSON, 1)

                            if dataJSON["action"] == "insert":
                                print_orderbook(dataJSON, 0)
                    except:
                        continue
            except Exception:
                continue
    except requests.exceptions.ConnectionError:
        print("Error connecting")
        return
asyncio.run(main())