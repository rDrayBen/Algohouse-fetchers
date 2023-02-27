import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.bitget.com"
API_SYMBOLS = "/api/spot/v1/public/products"
WS_URL = "wss://ws.bitget.com/spot/v1/stream"
PING_RANGE = 30
TIMEOUT = 0.1
SLEEP_TIME = 5

def print_trade(data):
    for i in data['data']:
        print("!", round(time.time() * 1000), data['arg']['instId'], i[3][0].upper(), i[1], i[2], end='\n')

def print_orderbook(data):
    if data['action'] == 'update':
        if data['data'][0]['asks'] != []:
            print("$", round(time.time() * 1000), data['arg']['instId'],
                  "S", "|".join(x[1] + "@" + x[0] for x in data['data'][0]['asks']), end='\n')
        if data['data'][0]['bids'] != []:
            print("$", round(time.time() * 1000), data['arg']['instId'],
                  "B", "|".join(x[1] + "@" + x[0] for x in data['data'][0]['bids']), end='\n')

    if data['action'] == 'snapshot':
        if data['data'][0]['asks'] != []:
            print("$", round(time.time() * 1000), data['arg']['instId'],
                  "S", "|".join(x[1] + "@" + x[0] for x in data['data'][0]['asks']),"R", end='\n')
        if data['data'][0]['bids'] != []:
            print("$", round(time.time() * 1000), data['arg']['instId'],
                  "B", "|".join(x[1] + "@" + x[0] for x in data['data'][0]['bids']),"R", end='\n')

def print_meta(data):
    print("@MD", data['symbolName'], "spot", data['baseCoin'], data['quoteCoin'], data['quantityScale'],
          1, 1, 0, 0, end="\n")

async def get_metadata(response):
    for i in response.json()['data']:
        print_meta(i)
    print("@MDEND")

async def subscribe(ws, symbols):
    start_send = time.time()
    for i in symbols:
        now = time.time()
        # Fix ping, send
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": [{
                "instType": "sp",
                "channel": "trade",
                "instId": i
            }]}))
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": [{
                "instType": "sp",
                "channel": "books",
                "instId": i
            }]
        }))
        time.sleep(TIMEOUT)

async def heartbit(ws):
    while True:
        await ws.send("ping")
        await asyncio.sleep(SLEEP_TIME)

async def main():
    try:
        response = requests.get(API_URL+API_SYMBOLS)
        symbols = [i["baseCoin"] + i["quoteCoin"] for i in response.json()['data']]
        async for ws in websockets.connect(WS_URL, ping_interval=None):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                heart_bit_task = asyncio.create_task(heartbit(ws))
                meta_task = asyncio.create_task(get_metadata(response))
                while True:
                    try:
                        data = await ws.recv()
                        dataJSON = json.loads(data)
                        if dataJSON['action'] == 'update':
                            if dataJSON['arg']['channel'] == 'books':
                                print_orderbook(dataJSON)
                            if dataJSON['arg']['channel'] == 'trade':
                                print_trade(dataJSON)

                        if dataJSON['action'] == 'snapshot':
                            if dataJSON['arg']['channel'] == 'books':
                                print_orderbook(dataJSON)

                    except Exception as e:
                        continue
            except Exception as conn_c:
                print(f"WARNING: connection exception {conn_c} occurred")
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())