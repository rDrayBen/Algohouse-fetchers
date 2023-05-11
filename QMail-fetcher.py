import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.qmall.io"
API_SYMBOLS = "/api/v1/public/markets"
WSS_URL = "wss://qmall.io/ws"
TIMEOUT = 0.001
PING_TIMEOUT = 6
WSS_DICT = {}
response = requests.get(API_URL + API_SYMBOLS)
symbols = [i["name"] for i in response.json()['result']]

async def subscribe(ws, data):
    await ws.send(json.dumps({
          "method":"deals.subscribe",
          "params": [data],
          "id":1
            }))
    await asyncio.sleep(TIMEOUT)
    await ws.send(json.dumps(
            {
                "method": "depth.subscribe",
                "params":
                    [
                        data,
                        1,
                        "0"
                    ],
                "id": 2
            }))

async def heatbeat(ws):
    while True:
        await ws.send(json.dumps({
            "method": "server.ping",
            "params": [],
            "id": 0
        }))
        await asyncio.sleep(PING_TIMEOUT)

async def meta(data):
    for i in data:
        print("@MD", i['name'], "spot", i['stock'], i['money'], i['feePrec'], 1, 1, 0, 0)
    print("@MDEND")

def print_trades(data):
    for i in data[1]:
        print("!", round(time.time() * 1000), data[0], i['type'][0].upper(), i["price"], i['amount'], end="\n")

def print_orderbooks(data, isSnapshot):
    if isSnapshot:
        if "asks" in data['params'][1]:
            print("$", round(time.time() * 1000), data['params'][2], "S",
                  "|".join(str(i[1]) + "@" + str(i[0]) for i in data['params'][1]['asks']), "R", end="\n")
        if "bids" in data['params'][1]:
            print("$", round(time.time() * 1000), data['params'][2], "S",
                  "|".join(str(i[1]) + "@" + str(i[0]) for i in data['params'][1]['asks']), "R", end="\n")
    else:
        if "asks" in data['params'][1]:
            print("$", round(time.time() * 1000), data['params'][2], "S",
                  "|".join(str(i[1]) + "@" + str(i[0]) for i in data['params'][1]['asks']), end="\n")
        if "bids" in data['params'][1]:
            print("$", round(time.time() * 1000), data['params'][2], "S",
                  "|".join(str(i[1]) + "@" + str(i[0]) for i in data['params'][1]['asks']), end="\n")

async def handle_socket(symbol, ):
    async for ws in websockets.connect(WSS_URL):
        try:

            await subscribe(ws, symbol)
            ping_task = asyncio.create_task(heatbeat(ws))
            async for message in ws:
                try:

                    dataJSON = json.loads(message)
                    if dataJSON['method'] == "deals.update" and len(dataJSON['params'][1]) < 4:
                        print_trades(dataJSON['params'])
                    if dataJSON['method'] == "depth.update" and dataJSON['params'][0] == False:
                        if len(dataJSON['params'][1]['asks']) > 20:
                            print_orderbooks(dataJSON, 1)
                        else:
                            print_orderbooks(dataJSON, 0)
                except KeyboardInterrupt:
                    exit(0)
                except:
                    pass
        except KeyboardInterrupt:
            exit(0)

        except:
            continue

async def handler():
    meta_task = asyncio.create_task(meta(response.json()['result']))
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in symbols])

def main():
    asyncio.get_event_loop().run_until_complete(handler())
main()