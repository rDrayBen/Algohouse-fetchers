import json
import websockets
import time
import asyncio
import requests
API_URL = "https://api.bitget.com"
API_SYMBOLS = "/api/v2/spot/public/symbols"
WSS_URL = "wss://ws.bitget.com/spot/v1/stream"
CONNECTIONS_RANGE = 2
PING_TIMEOUT = 15
SUB_TIMEOUT = 0.2
response = requests.get(API_URL + API_SYMBOLS)
symbols_ = [i["symbol"] for i in response.json()['data'] if i['status'] == 'online']
symbol_chunks = []
def divide_on_chunks(symbols, chunk_size=100):
    chunks_amount = round(len(symbols)/chunk_size)
    last = 0
    for i in range(chunks_amount):
        symbol_chunks.append(symbols[i*chunk_size:chunk_size*(i+1)])
        last = chunk_size*(i+1)
    if len(symbols) - last > 0:
        symbol_chunks.append(symbols[last: len(symbols)])
def print_trade(data):
    for i in data['data']:
        print("!", round(time.time() * 1000), data['arg']['instId'], i[3][0].upper(), i[1], i[2], end='\n')
def print_orderbook(data):
    if data['action'] == 'update':
        if data['data'][0]['asks'] != []:
            print("$", round(time.time() * 1000), data['arg']['instId'],
                  "S", "|".join(x[1] + "@" + x[0] for x in data['data'][0]['asks']), end='\n')
        if data['data'][0]['bids'] != []:
            print("$", round(time.time() * 1000), data['arg']['instId'],
                  "B", "|".join(x[1] + "@" + x[0] for x in data['data'][0]['bids']), end='\n')

    if data['action'] == 'snapshot':
        if data['data'][0]['asks'] != []:
            print("$", round(time.time() * 1000), data['arg']['instId'],
                  "S", "|".join(x[1] + "@" + x[0] for x in data['data'][0]['asks']),"R", end='\n')
        if data['data'][0]['bids'] != []:
            print("$", round(time.time() * 1000), data['arg']['instId'],
                  "B", "|".join(x[1] + "@" + x[0] for x in data['data'][0]['bids']),"R", end='\n')
async def meta(respose):
    for i in response.json()['data']:
        if i['status'] == 'online':
            print("@MD", i['symbol'], "spot", i['baseCoin'], i['quoteCoin'], i['quotePrecision'],
                  1, 1, 0, 0, end="\n")
    print("@MDEND")
async def subscribe(ws, symbols):
    for i in symbols:
        await ws.send(json.dumps(
            {
                "op": "subscribe",
                "args": [{
                    "instType": "sp",
                    "channel": "trade",
                    "instId": f"{i}"
                }]
            }
        ))
        await asyncio.sleep(SUB_TIMEOUT)
        await ws.send(json.dumps(
            {
                "op": "subscribe",
                "args": [{
                    "instType": "sp",
                    "channel": "books",
                    "instId": f"{i}"
                }]
            }
        ))
        await asyncio.sleep(SUB_TIMEOUT)
async def heartbeat(ws):
    while True:
        await ws.send(message='ping')
        await asyncio.sleep(PING_TIMEOUT)
async def handle_socket(symbols):
    async for ws in websockets.connect(WSS_URL, ping_interval=None):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbols))
            ping_task = asyncio.create_task(heartbeat(ws))
            async for message in ws:
                try:
                    dataJSON = json.loads(message)
                    if dataJSON['action'] == 'update':
                        if dataJSON['arg']['channel'] == 'books':
                            print_orderbook(dataJSON)
                        if dataJSON['arg']['channel'] == 'trade':
                            print_trade(dataJSON)
                    if dataJSON['action'] == 'snapshot':
                        if dataJSON['arg']['channel'] == 'books':
                            print_orderbook(dataJSON)
                except KeyboardInterrupt:
                    exit(0)
                except:
                    pass
        except KeyboardInterrupt:
            print("Keyboard Interupt")
            exit(1)
        except Exception as conn_c:
            print(f"WARNING: connection exception {conn_c} occurred")
            continue
async def handler():
    meta_task = asyncio.create_task(meta(response))
    divide_on_chunks(symbols_, 100)
    await asyncio.wait([asyncio.create_task(handle_socket(chunk)) for chunk in symbol_chunks])
def main():
    asyncio.get_event_loop().run_until_complete(handler())
main()