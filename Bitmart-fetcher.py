import json
import requests
import re
import asyncio
import time
import websockets
import zlib
from math import floor
API_URL = 'https://api-cloud.bitmart.com'
API_SPOT_SYMBOLS_URL = '/spot/v1/symbols/details'
API_SPOT_SYMBOLS_BOOK_URL = '/spot/v1/symbols/book'
WS_URL= 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1'
WS_PUBLIC_SPOT_TRADE = 'spot/trade'
WS_PUBLIC_SPOT_DEPTH5 = 'spot/depth5'
WS_PUBLIC_SPOT_DEPTH50 = 'spot/depth50'
CONNECTIONS_MAX_SIZE = 10
TIMEOUT = 0.1
PING_TIMEOUT = 15
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['symbol'] for x in response.json()['data']['symbols']]
symbol_chunks = []

def divide_on_chunks(symbols, chunk_size=100):
    chunks_amount = round(len(symbols)/chunk_size)
    last = 0
    for i in range(chunks_amount):
        symbol_chunks.append(symbols[i*chunk_size:chunk_size*(i+1)])
        last = chunk_size*(i+1)
    if len(symbols) - last > 0:
        symbol_chunks.append(symbols[last: len(symbols)])
def convert(message):
    if type(message) == bytes:
        return inflate(message)
    else:
        return message
def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated.decode('UTF-8')
async def meta(response):
    for i in response.json()['data']['symbols']:
        print("@MD", i['symbol'], "spot", i['base_currency'], i['quote_currency'], i['price_max_precision'],
              1, 1, 0, 0, end="\n")
    print("@MDEND")
async def heartbeat(ws):
    while True:
        await ws.send(message=json.dumps({"subscribe":"ping"}))
        await asyncio.sleep(PING_TIMEOUT)
async def subscribe(ws, symbols_):
    for i in range(len(symbols_)):
        await ws.send(message=json.dumps({"op": "subscribe",
                                          "args": [f"{WS_PUBLIC_SPOT_TRADE}:{symbols_[i]}"]}))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({"op": "subscribe",
                                          "args": [f"{WS_PUBLIC_SPOT_DEPTH5}:{symbols_[i]}"]}))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({"op": "subscribe",
                                          "args": [f"{WS_PUBLIC_SPOT_DEPTH50}:{symbols_[i]}"]}))
        await asyncio.sleep(TIMEOUT)
def print_trades(data):
    try:
        if len(data['data']) >= 30:
            return
        for i in data['data']:
            if i['side'] == "buy":
                print("!", round(time.time() * 1000), i['symbol'].replace("_", "-"), 'B', i['price'],
                      i['size'], end='\n')
            else:
                print("!", round(time.time() * 1000), i['symbol'].replace("_", "-"), 'S', i['price'],
                      i['size'], end='\n')
    except:
        pass
def print_orderbook(data):
    try:
        asks = data['data'][0]['asks']
        bids = data['data'][0]['bids']
        if data['table'] == WS_PUBLIC_SPOT_DEPTH50:
            print("$", round(time.time() * 1000), data['data'][0]['symbol'].replace('_', '-'), 'B',
                      re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0]) + "|" for x in bids))
                      ,'R', end='\n')
            print("$", round(time.time() * 1000), data['data'][0]['symbol'].replace('_', '-'), 'S',
                      re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0]) + "|" for x in asks))
                      ,'R', end='\n')

        elif data['table'] == WS_PUBLIC_SPOT_DEPTH5:
            print("$", round(time.time() * 1000), data['data'][0]['symbol'].replace('_', '-'), 'B',
                  re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0]) + "|" for x in bids))
                  , end='\n')
            print("$", round(time.time() * 1000), data['data'][0]['symbol'].replace('_', '-'), 'S',
                  re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0]) + "|" for x in asks))
                  , end='\n')
    except:
        pass

async def handle_socket(symbol):
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            ping_task = asyncio.create_task(heartbeat(ws))
            async for message in ws:
                try:
                    decoded_data = convert(message)
                    dataJSON = json.loads(decoded_data)
                    if dataJSON['table'] == WS_PUBLIC_SPOT_TRADE:
                        print_trades(dataJSON)
                    if dataJSON['table'] == WS_PUBLIC_SPOT_DEPTH5:
                        print_orderbook(dataJSON)
                    if dataJSON['table'] == WS_PUBLIC_SPOT_DEPTH50:
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
    divide_on_chunks(symbols, 100)
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in symbol_chunks[0:10]])

def main():
    asyncio.get_event_loop().run_until_complete(handler())
main()