import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://satangcorp.com/api/v3'
API_SPOT_SYMBOLS_URL = '/exchangeInfo'
WS_URL = 'wss://ws.satangcorp.com/stream'
WS_PUBLIC_SPOT_TRADE = '@aggTrade'
WS_PUBLIC_SPOT_ORDERBOOKS = '@depth20'
TIMEOUT = 0.1
PING_TIMEOUT = 5
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['symbol'] for x in response.json()['symbols']]
trades = [(i + WS_PUBLIC_SPOT_TRADE) for i in symbols]
orderbooks = [(i + WS_PUBLIC_SPOT_ORDERBOOKS) for i in symbols]

async def meta(response):
    for i in response.json()['symbols']:
        print("@MD", i['symbol'].upper(), "spot", i['baseAsset'].upper(), i['quoteAsset'].upper(), i['quotePrecision'],
              1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message=json.dumps({"e": "ping"}))
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    if data["data"]["m"]:
        bs = "B"
    else:
        bs = "S"
    try:
        print('!', round(time.time() * 1000), data["data"]["s"].upper(), bs, data["data"]["q"], data["data"]["p"])
    except:
        pass


def print_orderbooks(data):
    try:
        if data["data"]["asks"] != []:
            print('$', round(time.time() * 1000), data["stream"][:-12].upper(), 'S', '|'.join(
                i[1] + '@' + i[0] for i in data["data"]["asks"]), 'R')
        if data["data"]["bids"] != []:
            print('$', round(time.time() * 1000), data["stream"][:-12].upper(), 'B', '|'.join(
                i[1] + '@' + i[0] for i in data["data"]["bids"]), 'R')
    except:
        pass


async def subscribe(ws, symbols_):
    for i in range(len(symbols_)):
        await ws.send(message=json.dumps({
            "method": "SUBSCRIBE",
            "params": [
                f"{symbols_[i]}{WS_PUBLIC_SPOT_TRADE}",
                f"{symbols_[i]}{WS_PUBLIC_SPOT_ORDERBOOKS}"
            ]
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
                    if data_json['stream'] in trades:
                        print_trades(data_json)
                    if data_json['stream'] in orderbooks:
                        print_orderbooks(data_json)
                except:
                    continue
        except:
            continue

asyncio.run(main())
