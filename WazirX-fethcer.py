import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.wazirx.com"
API_SYMBOLS = "/sapi/v1/exchangeInfo"
WS_URL = "wss://stream.wazirx.com/stream"
TIMEOUT = 0.01
PING_TIMEOUT = 15

async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps({
            "event": "subscribe",
            "streams": [i]
        }))
        await asyncio.sleep(TIMEOUT)

async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({"event":"ping"}))
        await asyncio.sleep(PING_TIMEOUT)

async def meta(data):
    for i in data:
        print("@MD", i['baseAsset'].upper() + "/" +i ['quoteAsset'].upper(), "spot", i['baseAsset'].upper(), i['quoteAsset'].upper(),
              i['quoteAssetPrecision'], 1, 1, 0, 0, end='\n')
    print("@MDEND")

def print_trades(data):
    print("!", round(time.time() * 1000), data['data']['trades'][0]["s"].upper(), data['data']['trades'][0]['S'][0].upper(),
          data['data']['trades'][0]["p"], data['data']['trades'][0]['q'], end='\n')

def print_orderbook(data, isSnapshot):
    if isSnapshot:
        if data['data']['a'] != []:
            print("$", round(time.time() * 1000), data['data']['s'].upper(), "S",
                  "|".join(i[1]+"@"+i[0] for i in data['data']['a']),
                  "R", end="\n")
        if data['data']['b'] != []:
            print("$", round(time.time() * 1000), data['data']['s'].upper(), "B",
                  "|".join(i[1] + "@" + i[0] for i in data['data']['b']),
                  "R", end="\n")
    else:
        if data['data']['a'] != []:
            print("$", round(time.time() * 1000), data['data']['s'].upper(), "S",
                  "|".join(i[1] + "@" + i[0] for i in data['data']['a']), end="\n")
        if data['data']['b'] != []:
            print("$", round(time.time() * 1000), data['data']['s'].upper(), "B",
                  "|".join(i[1] + "@" + i[0] for i in data['data']['b']), end="\n")

async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['symbol'] for i in response.json()['symbols']]
        trade_channel = [i + "@trades" for i in symbols]
        delta_channel = [i + "@depth10@100ms" for i in symbols]
        snapshot_channel = [i + "@depth" for i in symbols]
        meta_task = asyncio.create_task(meta(response.json()['symbols']))
        async for ws in websockets.connect(WS_URL):
            try:
                subscribe_task = asyncio.create_task(subscribe(ws, snapshot_channel))
                subscribe_task = asyncio.create_task(subscribe(ws, delta_channel))
                trade_sub_task = asyncio.create_task(subscribe(ws, trade_channel))
                heartbeat_task = asyncio.create_task(heartbeat(ws))

                while True:
                    data = await ws.recv()
                    dataJSON = json.loads(data)
                    try:
                        if dataJSON["stream"] in trade_channel:
                            print_trades(dataJSON)
                        elif dataJSON["stream"] in delta_channel:
                            print_orderbook(dataJSON, 0)
                        elif dataJSON["stream"] in snapshot_channel:
                            print_orderbook(dataJSON, 1)
                    except:
                        continue
            except Exception:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())