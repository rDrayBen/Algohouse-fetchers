import json
import websockets
import time
import asyncio
import requests
API_URL = "https://ascendex.com"
API_SYMBOLS = "/api/pro/v1/cash/products"
WSS_URL = "wss://ascendex.com/1/api/pro/v1/stream"
TIMEOUT = 0.001
PING_TIMEOUT = 10
response = requests.get(API_URL+API_SYMBOLS)
symbols = [i['symbol'] for i in response.json()['data']]
symbol_chunks = []

def divide_on_chunks(symbols, chunk_size=100):
    chunks_amount = round(len(symbols)/chunk_size)
    last = 0
    for i in range(chunks_amount):
        symbol_chunks.append(symbols[i*chunk_size:chunk_size*(i+1)])
        last = chunk_size*(i+1)
    if len(symbols) - last > 0:
        symbol_chunks.append(symbols[last: len(symbols)])

async def subscribe(ws, data):
    k = 0
    for i in data:
        await ws.send(json.dumps({"op": "sub", "id": f"abc123{k}", "ch":f"trades:{i}"}))
        await asyncio.sleep(TIMEOUT)
        k+=1
        await ws.send(json.dumps({ "op": "req", "action":"depth-snapshot","args": {"symbol": f"{i}"}}))
        await asyncio.sleep(TIMEOUT)
        k+=1
        await ws.send(json.dumps({ "op": "sub", "id": f"abc123{k}", "ch":f"depth:{i}"}))
        await asyncio.sleep(TIMEOUT)
        k += 1

async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({"op": "ping"}))
        await asyncio.sleep(PING_TIMEOUT)

async def meta(data):
    for i in data:
        separator = i['symbol'].find("/")
        print("@MD", i['symbol'], "spot", i['symbol'][0:separator], i['symbol'][separator+1:len(i['symbol'])],
              i['priceScale'], 1, 1, 0, 0, end="\n")
    print("@MDEND")

def print_trades(data):
    for i in data['data']:
        if i["bm"]:
            print("!", round(time.time() * 1000), data['symbol'], "B", i['p'], i['q'], end="\n")
        else:
            print("!", round(time.time() * 1000), data['symbol'], "S", i['q'], i['p'], end="\n")

def print_orderbooks(data, isSnapshot):
    if isSnapshot:
        if data['data']['bids'] != []:
            print("$", round(time.time() * 1000), data['symbol'], "B",
                  "|".join(str(i[1]) + "@" + str(i[0]) for i in data['data']['bids']), "R", end="\n")
        if data['data']['asks'] != []:
            print("$", round(time.time() * 1000), data['symbol'], "S",
                  "|".join(str(i[1]) + "@" + str(i[0]) for i in data['data']['asks']), "R", end="\n")
    else:
        if data['data']['bids'] != []:
            print("$", round(time.time() * 1000), data['symbol'], "B",
                  "|".join(str(i[1]) + "@" + str(i[0]) for i in data['data']['bids']), end="\n")
        if data['data']['asks'] != []:
            print("$", round(time.time() * 1000), data['symbol'], "S",
                  "|".join(str(i[1]) + "@" + str(i[0]) for i in data['data']['asks']), end="\n")

async def handle_socket(symbol, ):
    async for ws in websockets.connect(WSS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            ping_task = asyncio.create_task(heartbeat(ws))
            async for message in ws:
                try:
                    dataJSON = json.loads(message)
                    try:
                        if 'data' in dataJSON:
                            if dataJSON["m"] == "depth-snapshot":
                                print_orderbooks(dataJSON, 1)
                            if dataJSON["m"] == "depth":
                                print_orderbooks(dataJSON, 0)
                            if dataJSON["m"] == "trades":
                                print_trades(dataJSON)
                    except:
                        continue
                except KeyboardInterrupt:
                    exit(0)
                except:
                    pass
        except KeyboardInterrupt:
            exit(0)
        except:
            continue

async def handler():
    divide_on_chunks(symbols, 100)
    meta_task = asyncio.create_task(meta(response.json()['data']))
    await asyncio.wait([asyncio.create_task(handle_socket(chunk)) for chunk in symbol_chunks])

def main():
    asyncio.get_event_loop().run_until_complete(handler())
main()