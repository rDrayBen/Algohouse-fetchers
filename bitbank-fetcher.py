import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://api.bitbank.cc/v1'
API_SPOT_SYMBOLS_URL = '/spot/pairs'
WS_URL= 'wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket'
WS_PUBLIC_SPOT_TRADE = 'transactions_'
WS_PUBLIC_SPOT_DEPTH_DELTA = 'depth_diff_'
WS_PUBLIC_SPOT_DEPTH_WHOLE = 'depth_whole_'
TIMEOUT = 0.1
PING_TIMEOUT = 25
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['name'] for x in response.json()['data']['pairs']]
trades = [WS_PUBLIC_SPOT_TRADE + i for i in symbols]
deltas = [WS_PUBLIC_SPOT_DEPTH_DELTA + i for i in symbols]
snapshots = [WS_PUBLIC_SPOT_DEPTH_WHOLE + i for i in symbols]

async def meta(response):
    for i in response.json()['data']['pairs']:
        print("@MD", i['name'], "spot", i['base_asset'], i['quote_asset'], i['amount_digits'],
              1, 1, 0, 0, end="\n")
    print("@MDEND")

async def heartbeat(ws):
    while True:
        await ws.send(message='3')
        await asyncio.sleep(PING_TIMEOUT)

def print_trades(data):
    try:
        for i in data["message"]["data"]["transactions"]:
            print('!', round(time.time() * 1000), data["room_name"][13:].upper(), i["side"][0].upper(), i["price"], i["amount"])
    except:
        pass

def print_orderbooks(data, is_snapshot):
    try:
        if is_snapshot:
            if data["message"]["data"]["asks"] != []:
                print('$', round(time.time() * 1000), data["room_name"][12:].upper(), 'S', '|'.join(i[1] + '@' + i[0] for i in data["message"]["data"]["asks"]), 'R')
            if data["message"]["data"]["bids"] != []:
                print('$', round(time.time() * 1000), data["room_name"][12:].upper(), 'B', '|'.join(i[1] + '@' + i[0] for i in data["message"]["data"]["bids"]), 'R')
        else:
            if data["message"]["data"]["a"] != []:
                print('$', round(time.time() * 1000), data["room_name"][11:].upper(), 'S', '|'.join(i[1] + '@' + i[0] for i in data["message"]["data"]["a"]))
            if data["message"]["data"]["b"] != []:
                print('$', round(time.time() * 1000), data["room_name"][11:].upper(), 'B', '|'.join(i[1] + '@' + i[0] for i in data["message"]["data"]["b"]))
    except:
        pass

async def subscribe(ws, symbols_):
    await ws.send(message='40')
    for i in range(len(symbols_)):
        await ws.send(message=f'42["join-room","{WS_PUBLIC_SPOT_TRADE}{symbols_[i]}"]')
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=f'42["join-room","{WS_PUBLIC_SPOT_DEPTH_DELTA}{symbols_[i]}"]')
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=f'42["join-room","{WS_PUBLIC_SPOT_DEPTH_WHOLE}{symbols_[i]}"]')
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
                    if data[0] == '4' and data[1] == '2':
                        data_sliced = data[2:]
                        data_json = json.loads(data_sliced)
                        if data_json[1]['room_name'] in trades:
                            print_trades(data_json[1])
                        if data_json[1]['room_name'] in deltas:
                            print_orderbooks(data_json[1], 0)
                        if data_json[1]['room_name'] in snapshots:
                            print_orderbooks(data_json[1], 1)
                except:
                    continue
        except:
            continue

asyncio.run(main())
