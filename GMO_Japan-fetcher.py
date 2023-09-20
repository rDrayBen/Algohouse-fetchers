import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.coin.z.com/public"
API_SYMBOLS = "/v1/symbols"
WSS_URL = "wss://api.coin.z.com/ws/public/v1"
TIMEOUT = 1
PING_TIMEOUT = 5
DELTA_DEPTH = 5
async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps( {
        "command": "subscribe",
        "channel": "orderbooks",
        "symbol": i
        }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({
        "command": "subscribe",
        "channel": "trades",
        "symbol": i
        }))
        await asyncio.sleep(TIMEOUT)

async def meta(data):
    for i in data['data'] :
        if i['symbol'].find('_') != -1:
            symbol_ = i['symbol'].find('_')
            precission = str(i["sizeStep"])[::-1].find(".")
            if precission != -1:
                print("@MD", i['symbol'], "spot", i['symbol'][0:symbol_], i['symbol'][symbol_+1:len(i['symbol'])],
                      precission, 1, 1, 0, 0)
            else:
                print("@MD", i['symbol'], "spot", i['symbol'][0:symbol_], i['symbol'][symbol_ + 1:len(i['symbol'])],
                      0, 1, 1, 0, 0)
        else:
            precission = str(i["sizeStep"])[::-1].find(".")
            if precission != -1:
                print("@MD", i['symbol'] + "_YEN", "spot", i['symbol'], "YEN",
                          precission, 1, 1, 0, 0)
            else:
                print("@MD", i['symbol'] + "_YEN", "spot", i['symbol'], "YEN",
                          0, 1, 1, 0, 0)
    print("@MDEND")

def print_trade(data):
    symbol_ = data['symbol'].find('_')
    if symbol_ == -1:
        print("!", round(time.time() * 1000), data['symbol'] + "_YEN", data['side'][0], data['price'], data['size'],
              end="\n")
    else:
        print("!", round(time.time() * 1000), data['symbol'], data['side'][0], data['price'], data['size'],
              end="\n")

def print_orderbook(data, isSnapshot):
    if isSnapshot:
        if data['asks'] != []:
            symbol_ = data['symbol'].find('_')
            if symbol_ == -1:
                print("$", round(time.time() * 1000), data['symbol'] + "_YEN", "S",
                      "|".join(i['size'] + "@" + i["price"] for i in data['asks']), "R", end="\n")
            else:
                print("$", round(time.time() * 1000), data['symbol'], "S",
                      "|".join(i['size'] + "@" + i["price"] for i in data['asks']), "R", end="\n")
        if data['bids'] != []:
            symbol_ = data['symbol'].find('_')
            if symbol_ == -1:
                print("$", round(time.time() * 1000), data['symbol'] + "_YEN", "B",
                      "|".join(i['size'] + "@" + i["price"] for i in data['bids']), "R", end="\n")
            else:
                print("$", round(time.time() * 1000), data['symbol'], "B",
                      "|".join(i['size'] + "@" + i["price"] for i in data['bids']), "R", end="\n")
    else:
        if data['asks'] != []:
            symbol_ = data['symbol'].find('_')
            if symbol_ == -1:
                print("$", round(time.time() * 1000), data['symbol'] + "_YEN", "S",
                      "|".join(data['asks'][i]['size'] + "@" + data['asks'][i]["price"]
                               for i in range(DELTA_DEPTH)), end="\n")
            else:
                print("$", round(time.time() * 1000), data['symbol'], "S",
                      "|".join(data['asks'][i]['size'] + "@" + data['asks'][i]["price"]
                               for i in range(DELTA_DEPTH)), end="\n")
        if data['asks'] != []:
            symbol_ = data['symbol'].find('_')
            if symbol_ == -1:
                print("$", round(time.time() * 1000), data['symbol'] + "_YEN", "B",
                      "|".join(data['bids'][i]['size'] + "@" + data['bids'][i]["price"]
                               for i in range(DELTA_DEPTH)), end="\n")
            else:
                print("$", round(time.time() * 1000), data['symbol'], "B",
                      "|".join(data['bids'][i]['size'] + "@" + data['bids'][i]["price"]
                               for i in range(DELTA_DEPTH)), end="\n")

async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['symbol'] for i in response.json()['data']]
        symbols_dict = {}
        for i in symbols:
            symbols_dict[i] = 0
        meta_task = asyncio.create_task(meta(response.json()))
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                while True:
                    data = await ws.recv()
                    dataJSON = json.loads(data)
                    try:
                        if "channel" in dataJSON:
                            if dataJSON['channel'] == "trades":
                                print_trade(dataJSON)
                            if dataJSON['channel'] == 'orderbooks':
                                if symbols_dict[dataJSON['symbol']] == 0:
                                    print_orderbook(dataJSON, 1)
                                    symbols_dict[dataJSON['symbol']] += 1
                                else:
                                    print_orderbook(dataJSON, 0)
                    except:
                        pass
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())