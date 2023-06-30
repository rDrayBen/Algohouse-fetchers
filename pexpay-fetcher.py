import json
import websockets
import time
import asyncio
import requests
API_URL = 'https://www.pexpay.com/bapi/mbx/v1/public/spot/config/symbols'
WSS_URL = 'wss://stream.pexpay.com/stream'
TIMEOUT = 0.4
PING_TIMEOUT = 4
async def meta(response):
    for i in response.json()['data']:
        precission = str(i["ts"])[::-1].find(".")
        if precission != -1:
            print("@MD", i['s'], "spot", i['b'], i['q'], precission, 1, 1, 0, 0, end="\n")
        else:
            print("@MD", i['s'], "spot", i['b'], i['q'], 0, 1, 1, 0, 0, end="\n")
    print("@MDEND")
async def subscribe(ws, data):
    id_ = 0
    for i in data:
        await ws.send(json.dumps({"method": "SUBSCRIBE",
                                  "params": [f"{i}@depth20"],
                                  "id": id_}))
        id_ += 1
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({"method":"SUBSCRIBE",
                                  "params":[f"{i}@aggTrade", f"{i}@depth5"],
                                  "id":id_}))
        id_ += 1
        await asyncio.sleep(TIMEOUT)


async def heartbeat(ws):
    id_ = 1000
    while True:
        await ws.send(json.dumps({"method":"GET_PROPERTY","params":["combined"],"id":id_}))
        id_ += 1
        await asyncio.sleep(PING_TIMEOUT)
def print_trades(data):
    print("!", round(time.time() * 1000), data['data']['s'], ''.join("S" if data['data']['m'] else "B"),
          data['data']['p'], data['data']['q'], end="\n")
def print_orderbooks(data, isSnapshot):
    symbol_id = data['stream'].find('@')
    symbol = data['stream'][:symbol_id].upper()
    if isSnapshot:
        if data['data']['bids'] != []:
            print("$", round(time.time() * 1000), symbol, "B",
                  "|".join(i[1] + "@" + i[0] for i in data['data']['bids']), "R" , end="\n")
        if data['data']['asks'] != []:
            print("$", round(time.time() * 1000), symbol, "S",
                  "|".join(i[1] + "@" + i[0] for i in data['data']['asks']), "R", end="\n")
    else:
        if data['data']['bids'] != []:
            print("$", round(time.time() * 1000), symbol, "B",
                  "|".join(i[1] + "@" + i[0] for i in data['data']['bids']), end="\n")
        if data['data']['asks'] != []:
            print("$", round(time.time() * 1000), symbol, "S",
                  "|".join(i[1] + "@" + i[0] for i in data['data']['asks']), end="\n")
async def main():
    try:
        response = requests.get(API_URL)
        symbols = [i['s'].lower() for i in response.json()['data']]
        trade_streams = [i + "@aggTrade" for i in symbols]
        snapshot_streams = [i + "@depth20" for i in symbols]
        delta_streams = [i + "@depth5" for i in symbols]
        meta_task = asyncio.create_task(meta(response))
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    try:
                        data = await ws.recv()
                        dataJSON = json.loads(data)
                        if dataJSON['stream'] in trade_streams:
                            print_trades(dataJSON)
                        if dataJSON['stream'] in delta_streams:
                            print_orderbooks(dataJSON, 0)
                        if dataJSON['stream'] in snapshot_streams:
                            print_orderbooks(dataJSON, 1)
                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())