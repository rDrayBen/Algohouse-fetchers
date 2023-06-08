import json
import websockets
import time
import asyncio
import requests
API_URL = "https://api.kuna.io"
API_SYMBOLS = "/v4/markets/public/getAll"
WS_URL = "wss://ws-pro.kuna.io/socketcluster/"
TIMEOUT = 0.05
PING_TIMEOUT = 7

async def subscribe(ws, channels):
    await ws.send(json.dumps({"event":"#handshake","data":{"authToken":None},"cid":1}))
    cid = 2
    for i in channels:
       await ws.send(json.dumps(
           {"event": "#subscribe", "data": {"channel": f"{i}"}, "cid": cid}
       ))
       cid += 1
       await asyncio.sleep(TIMEOUT)

async def heartbeat(ws):
    while True:
        await ws.send('')
        await asyncio.sleep(PING_TIMEOUT)

async def meta(ws, pairs_api_response):
    for i in pairs_api_response:
        print("@MD", i['pair'], "spot", i["baseAsset"]["code"], i["quoteAsset"]["code"], i["quoteAsset"]["precision"],
              1, 1, 0, 0, end="\n")
    print("@MDEND")

def print_trade(data):
    print("!", round(time.time() *1000), data['pair'], data['type'][0], data['matchPrice'], data['quoteQuantity'], end="\n")

def print_orderbook(data, isSnapshot):
    if isSnapshot:
        if data['asks'] != []:
             print("$", round(time.time() * 1000), data['pair'], "S", "|".join(i[1]+"@" +i[0] for i in data['asks']), "R", end="\n")
        if data['bids'] != []:
             print("$", round(time.time() * 1000), data['pair'], "B", "|".join(i[1] + "@" + i[0] for i in data['bids']), "R", end="\n")
    else:
        if data['asks'] != []:
            print("$", round(time.time() * 1000), data['pair'], "S", "|".join(i[1]+"@" +i[0] for i in data['asks']), end="\n")
        if data['bids'] != []:
            print("$", round(time.time() * 1000), data['pair'], "B", "|".join(i[1] + "@" + i[0] for i in data['bids']), end="\n")
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['pair'] for i in response.json()['data']]
        trade_channels = [i.lower()+"@trade" for i in symbols]
        depth_channels = [i.lower()+"@depth" for i in symbols]
        start_snapshots = {}
        symbol_list_ = {}
        for i in symbols:
            symbol_list_[i] = 0
            start_snapshots[i] = {"bids": [], "asks": [], "pair": i}
        metadata_task = asyncio.create_task(meta(websockets.connect(WS_URL), response.json()['data']))
        async for ws in websockets.connect(WS_URL):
            try:
                subscribe_task = asyncio.create_task(subscribe(ws, trade_channels + depth_channels))
                heartbeat_task = asyncio.create_task(heartbeat(ws))
                while True:
                    data = await ws.recv()
                    if data == '':
                        continue
                    dataJSON = json.loads(data)
                    try:
                        if dataJSON['data']:
                            if dataJSON['data']['channel'] in trade_channels:
                                print_trade(dataJSON['data']['data']['data'])
                            elif dataJSON['data']['channel'] in depth_channels:
                                if symbol_list_[dataJSON['data']['data']['data']['pair']] <= 6:
                                    if symbol_list_[dataJSON['data']['data']['data']['pair']] == 6:
                                        print_orderbook(start_snapshots[dataJSON['data']['data']['data']['pair']], 1)
                                        symbol_list_[dataJSON['data']['data']['data']['pair']] += 1
                                    else:
                                        start_snapshots[dataJSON['data']['data']['data']['pair']]["bids"] += \
                                            dataJSON['data']['data']['data']['bids']
                                        start_snapshots[dataJSON['data']['data']['data']['pair']]["asks"] += \
                                            dataJSON['data']['data']['data']['asks']
                                        symbol_list_[dataJSON['data']['data']['data']['pair']] += 1
                                else:
                                    print_orderbook(dataJSON['data']['data']['data'], 0)
                        else:
                            continue
                    except:
                        continue
            except Exception:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())