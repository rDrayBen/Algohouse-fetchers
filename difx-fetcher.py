import json
import websockets
import time
import asyncio
import requests
API_URL = "https://api-v2.difx.com/open/api/v1"
API_SYMBOLS = "/pairs"
API_PRECISSIONS = '/currency'
WSS_URL = "wss://api-v2.difx.com/socket.io/?EIO=4&transport=websocket"
TIMEOUT = 0.5
PING_TIMEOUT = 5
async def meta(response):
    precissions = requests.get(API_URL + API_PRECISSIONS)
    precc = {}
    for i in precissions.json()['data']:
        if i['coin'] == i['currency']:
            precc[i['currency']] = i['precision']
    for i in response.json()['data']:
        print("@MD", i['symbol'], "spot", i['currency1'], i['currency2'],
              precc[i['currency2']], 1, 1, 0, 0,
              end="\n")
    print("@MDEND")
async def heartbeat(ws):
    while True:
        await ws.send(message='3')
        await asyncio.sleep(PING_TIMEOUT)
async def subscribe(ws, data):
    await ws.send(message='40{"token":"guest"}')
    await ws.send(message='40/price_change,{"token":"guest"}')
    for i in data:
        await ws.send(message=f'42["join","{i}"]')
        await asyncio.sleep(TIMEOUT)
def print_trades(data):
    print("!", round(time.time() * 1000), data[0], "".join("S" if data[1]==1 else "B"), "{:.6f}".format(data[2]),
          "{:.6f}".format(data[3]), end="\n")
def print_orderbook(data, isSnapshot):
    if isSnapshot:
        if data['bids'] != []:
            print("$", round(time.time() *1000), data['pair'], "B",
                  "|".join("{:.6f}".format(i[1])+"@"+"{:.6f}".format(i[0]) for i in data['bids']), "R", end="\n")
        if data['asks'] != []:
            print("$", round(time.time() *1000), data['pair'], "S",
                  "|".join("{:.6f}".format(i[1])+"@"+"{:.6f}".format(i[0]) for i in data['asks']), "R", end="\n")
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['symbol'] for i in response.json()['data']]
        asyncio.create_task(meta(response))
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    try:
                        data = await ws.recv()
                        if data[0] == "4" and data[1] == "2":
                            clear_data = data[2:]
                            if clear_data[0] == '/':
                                clear_data = clear_data[14:]
                            dataJSON = json.loads(clear_data)
                            if dataJSON[0] == "orderbook_limited":
                                print_orderbook(dataJSON[1], 1)
                            if dataJSON[0] == "trades":
                                print_trades(dataJSON[1])
                    except:
                        pass
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())