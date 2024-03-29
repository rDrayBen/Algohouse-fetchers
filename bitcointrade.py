import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.ripiotrade.co/v4/"
API_SYMBOLS = "public/pairs"
API_PRECISSION = "public/currencies"
WS_URL = "wss://ws.ripiotrade.co"
TIMEOUT = 0.01
PING_TIMEOUT = 5
DeltaDepth = 2

async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps(
                {
                    "method": "subscribe",
                    "topics": [i]
                }
            ))
        await asyncio.sleep(TIMEOUT)
async def hearbeat(ws):
    while True:
        await ws.send(json.dumps({"method":"ping"}))
        await asyncio.sleep(PING_TIMEOUT)
async def meta(pairs, precission):
    print(len(pairs), len(precission))
    for i in range(len(pairs)):
        base = pairs[i].find("_")
        try:
            print("@MD", pairs[i], "spot", pairs[i][0:base], pairs[i][base+1:len(pairs[i])], precission[i],
                  1, 1, 0, 0, end="\n")
        except:
            print("@MD", pairs[i], "spot", pairs[i][0:base], pairs[i][base + 1:len(pairs[i])], 0,
                  1, 1, 0, 0, end="\n")
    print("@MDEND")

def print_trades(data):
    print("!", round(time.time() * 1000), data['body']['pair'], data['body']['taker_side'][0].upper(), str(data['body']['price']),
          str("{0:4f}".format(data['body']['amount'])))

def print_orderbook(data):
    pair = data['topic'].find("@")
    print("$", round(time.time() * 1000), data['topic'][pair+1:len(data['topic'])], "S",
          "|".join(i['amount']+"@"+i['price'] for i in data['body']['asks']), "R", end='\n')
    print("$", round(time.time() * 1000), data['topic'][pair+1:len(data['topic'])], "B",
          "|".join(i['amount'] + "@" + i['price'] for i in data['body']['bids']), "R", end='\n')

def print_delta(data, depth):
    pair = data['topic'].find("@")
    print("$", round(time.time() * 1000), data['topic'][pair+1:len(data['topic'])], "S",
          "|".join(data['body']['asks'][i]['amount'] + "@" + data['body']['asks'][i]['price'] for i in range(depth)), end='\n')
    print("$", round(time.time() * 1000), data['topic'][pair+1:len(data['topic'])], "B",
          "|".join(data['body']['asks'][i]['amount'] + "@" + data['body']['bids'][i]['price'] for i in range(depth)),
          end='\n')

async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        currencies = [i['precision'] for i in requests.get(API_URL + API_PRECISSION).json()['data']]
        symbols = [i["symbol"] for i in response.json()['data']]
        counter = {}
        for i in symbols:
            counter[i] = 0
        trade_channel = ["trade@" + i for i in symbols]
        orderbook_channel = ["orderbook/level_3@" + i for i in symbols]
        meta_task = asyncio.create_task(meta(symbols, currencies))
        async for ws in websockets.connect(WS_URL):
            try:
                subscribe_task = asyncio.create_task(subscribe(ws, trade_channel + orderbook_channel))
                hearbeat_task = asyncio.create_task(hearbeat(ws))
                while True:
                    data = await ws.recv()
                    dataJSON = json.loads(data)

                    try:
                        pair = dataJSON['body']['pair']
                        if dataJSON['topic'] in trade_channel and 'amount' in dataJSON['body']:
                             print_trades(dataJSON)
                        if counter[pair] > 1:
                            if dataJSON['topic'] in orderbook_channel:
                                print_delta(dataJSON, DeltaDepth)

                        else:
                            if dataJSON['topic'] in orderbook_channel:
                                print_orderbook(dataJSON)
                                counter[dataJSON['body']['pair']] += 1
                    except:
                        continue
            except Exception:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())