import json
import websockets
import time
import asyncio
import requests

API_URL = "https://www.zbg.com"
API_SYMBOLS = "/exchange/api/v1/common/symbols"
WS_URL = "wss://kline.zbg.com/websocket"
TIMEOUT=0.001
PING_TIMEOUT = 5

async def subscribe(ws, data):
    for i in data['datas']:
        await ws.send(json.dumps({
            "action":"ADD",
            "dataType":f"{i['id']}_TRADE_{i['symbol'].upper()}",
            "dataSize":1
        }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({
            "action": "ADD",
            "dataType": f"{i['id']}_ENTRUST_ADD_{i['symbol'].upper()}",
        }))
        await asyncio.sleep(TIMEOUT)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({"action":"PING"}))
        await asyncio.sleep(PING_TIMEOUT)

async def meta(data):
    for i in data['datas']:
        print("@MD", i['symbol'], "spot", i['base-currency'], i['quote-currency'], i['price-precision'], 1, 1, 0, 0,
              end="\n")
    print("@MDEND")

def print_trade(data):
    if data[4] == "asks":
        print("!", round(time.time() * 1000), data[3], "S", data[5], data[6], end="\n")
    else:
        print("!", round(time.time() * 1000), data[3], "B", data[5], data[6], end="\n")


def print_orderbook(data, isSnapshot):
    if isSnapshot:
        if data[4]['asks'] != []:
            print("$", round(time.time() * 1000), data[2], 'S', "|".join(i[1] + "@" + i[0] for i in data[4]['asks']),
                  "R", end='\n')
        if data[4]['bids'] != []:
            print("$", round(time.time() * 1000), data[2], 'B', "|".join(i[1] + "@" + i[0] for i in data[4]['bids']),
                  "R", end='\n')
    else:
        if data[4] == "ASK":
            print("$", round(time.time() * 1000), data[3], "S", f"{data[6]}@{data[5]}", end='\n')
        if data[4] == "BID":
            print("$", round(time.time() * 1000), data[3], "B", f"{data[6]}@{data[5]}", end='\n')

async def main():
    try:
        response = requests.get(API_URL+API_SYMBOLS)
        meta_task = asyncio.create_task(meta(response.json()))
        async for ws in websockets.connect(WS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, response.json()))
                heartbeat_task = asyncio.create_task(heartbeat(ws))
                while True:
                    data = await ws.recv()
                    dataJSON = json.loads(data)
                    #print(dataJSON)
                    try:
                        if dataJSON[0][0] == 'T' and len(dataJSON) == 1:
                            print_trade(dataJSON[0])
                        if dataJSON[0][0] == 'AE':
                            print_orderbook(dataJSON[0], 1)
                        if dataJSON[0] == 'E':
                            print_orderbook(dataJSON, 0)
                    except:
                        pass
            except Exception:
                pass
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())