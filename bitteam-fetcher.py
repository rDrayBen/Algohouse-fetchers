import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://bit.team/trade/api'
API_SPOT_SYMBOLS_URL = '/pairs'
WS_URL = 'wss://bit.team/trade/ws/'
WS_PUBLIC_SPOT_TRADE = '/trades/'
WS_PUBLIC_SPOT_DEPTH_ORDERBOOK = '/orderbooks/'
TIMEOUT = 0.1
PING_TIMEOUT = 10
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [[x['id'], x['name']] for x in response.json()['result']['pairs']]
trades = 'trade.new'
orderbooks = 'orderbooks.upgrade'


async def meta(response):
    for i in response.json()['result']['pairs']:
        print("@MD", i['name'].upper(), "spot", i['name'].split('_')[0].upper(), i['name'].split('_')[1].upper(), i['quoteStep'],
              1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message=json.dumps({"type": "ping", "id": 1}))
        await asyncio.sleep(PING_TIMEOUT)


def find_symbol_by_id(data, symbol_id):
    symbol_id = data["message"]["data"]["pairId"]
    return next((symbol[1] for symbol in symbols if symbol[0] == symbol_id), None)


def print_trades(data):
    try:
        symbol = find_symbol_by_id(data, data["message"]["data"]["pairId"])
        quantity = float(data["message"]["data"]["trades"][0]["quantity"]) / (10 ** data["message"]["data"]["trades"][0]["baseDecimals"])
        price = float(data["message"]["data"]["trades"][0]["price"]) / (10 ** data["message"]["data"]["trades"][0]["quoteDecimals"])
        print('!', round(time.time() * 1000), symbol.upper(), data["message"]["data"]["trades"][0]["side"].upper(), price, quantity)
    except:
        pass


def print_orderbooks(data):
    try:
        symbol = find_symbol_by_id(data, data["message"]["data"]["pairId"])
        if data["message"]["data"]["orderbooks"]["asks"] != []:
            print('$', round(time.time() * 1000), symbol.upper(), 'S', '|'.join(
                i["quantity"] + '@' + i["price"] for i in data["message"]["data"]["orderbooks"]["asks"]), 'R')
        if data["message"]["data"]["orderbooks"]["bids"] != []:
            print('$', round(time.time() * 1000), symbol.upper(), 'B', '|'.join(
                i["quantity"] + '@' + i["price"] for i in data["message"]["data"]["orderbooks"]["bids"]), 'R')
    except:
        pass


async def subscribe(ws, symbols_):
    await ws.send(message=json.dumps({
        "type": "hello",
        "version": "2",
        "auth": {
            "headers": {
                "Authorization": "jwt"
            }
        },
        "id": 1
    }))
    await asyncio.sleep(TIMEOUT)
    for i in range(len(symbols_)):
        await ws.send(message=json.dumps({
            "type": "sub",
            "path": f"{WS_PUBLIC_SPOT_TRADE}{symbols_[i][0]}",
            "id": 1
        }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({
            "type": "sub",
            "path": f"{WS_PUBLIC_SPOT_DEPTH_ORDERBOOK}{symbols_[i][0]}",
            "id": 1
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
                    if data_json['message']['type'] == trades:
                        print_trades(data_json)
                    if data_json['message']['type'] == orderbooks:
                        print_orderbooks(data_json)
                except:
                    continue
        except:
            continue

asyncio.run(main())
