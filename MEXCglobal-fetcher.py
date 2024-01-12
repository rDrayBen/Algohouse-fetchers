import json
import websockets
import asyncio
import requests
import os
from CommonFunctions.CommonFunctions import get_unix_time, print_stats
# Fetcher use V3 of API and WS API, limits on 30 subs per connection
# Maximum number of messages 100m/1s
API_URL = "https://api.mexc.com"
API_SYMBOLS = "/api/v3/exchangeInfo"
WSS_URL = "wss://wbs.mexc.com/ws"
SUBSCRIBE_TIMEOUT = 0.01
PING_TIMEOUT = 4.5
HTTP_OK = 200
CHUNK_SIZE = 10
DEPTH_SIZE = 20
SKIP_ORDERBOOKS = os.getenv("SKIP_ORDERBOOKS") == None
TRADE = 2
SNAPSHOT = 1
DELTA = 0
TRADE_STATS = {}
ORDERBOOKS_STATS = {}
def divide_on_chunks(symbols, chunk_size):
    symbol_chunks = []
    chunks_amount = round(len(symbols)/chunk_size)
    last = 0
    for i in range(chunks_amount):
        symbol_chunks.append(symbols[i*chunk_size:chunk_size*(i+1)])
        last = chunk_size*(i+1)
    if len(symbols) - last > 0:
        symbol_chunks.append(symbols[last: len(symbols)])
    return symbol_chunks
async def meta(response):
    for pair in response['symbols']:
        if pair['status'] == "ENABLED" and pair['isSpotTradingAllowed'] == True and pair['permissions'] == ["SPOT"]:
            print("@MD", pair['symbol'], 'spot', pair['baseAsset'], pair['quoteAsset'],
                  pair['quotePrecision'], 1, 1, 0, 0, end="\n")
    print("@MDEND")
async def heartbeat(ws):
    while True:
        await ws.send(message=json.dumps({"method":"PING"}))
        await asyncio.sleep(PING_TIMEOUT)
async def subscribe(ws, symbols):
    for symbol in symbols:
        if SKIP_ORDERBOOKS:
            await ws.send(message=json.dumps(
                {
                    "method": "SUBSCRIPTION",
                    "params": [
                        f"spot@public.deals.v3.api@{symbol}",
                        f"spot@public.limit.depth.v3.api@{symbol}@{DEPTH_SIZE}"
                    ]
                }
            ))
            await asyncio.sleep(SUBSCRIBE_TIMEOUT)
        else:
            await ws.send(message=json.dumps(
                {
                    "method": "SUBSCRIPTION",
                    "params": [
                        f"spot@public.deals.v3.api@{symbol}"
                    ]
                }
            ))
            await asyncio.sleep(SUBSCRIBE_TIMEOUT)
def print_trades(data):
    for deal in data['d']['deals']:
        if deal['S'] == 1:
            print("!", get_unix_time(), data['s'], 'B', deal['p'], deal['v'], end="\n")
            TRADE_STATS[data['s']]+=1
        if deal['S'] == 2:
            print("!", get_unix_time(), data['s'], 'S', deal['p'], deal['v'], end="\n")
            TRADE_STATS[data['s']] += 1
def print_orderbooks(data, isSnapshot):
    if isSnapshot == SNAPSHOT:
        if data['d']['asks'] != []:
            print("$", get_unix_time(), data['s'], 'S',
                  "|".join(pq['v'] + "@" + pq['p'] for pq in data['d']['asks']), "R", end="\n")
            ORDERBOOKS_STATS[data['s']] += 1
        if data['d']['bids'] != []:
            print("$", get_unix_time(), data['s'], 'B',
                  "|".join(pq['v'] + "@" + pq['p'] for pq in data['d']['bids']), "R", end="\n")
            ORDERBOOKS_STATS[data['s']] += 1
    if isSnapshot == DELTA:
        pass
async def handle_socket(symbols):
    async for ws in websockets.connect(WSS_URL, ping_interval=None, ping_timeout=None):
        try:
            ping_task = asyncio.create_task(heartbeat(ws))
            sub_task = asyncio.create_task(subscribe(ws, symbols))
            async for message in ws:
                try:
                    message = json.loads(message)
                    if message['d']['e'] == "spot@public.deals.v3.api":
                        print_trades(message)
                    if message['d']['e'] == "spot@public.limit.depth.v3.api":
                        print_orderbooks(message, SNAPSHOT)
                    del message
                except KeyboardInterrupt:
                    exit(0)
                except:
                    continue
        except KeyboardInterrupt:
            exit(0)
        except:
            continue
async def handler():
    response = requests.get(API_URL + API_SYMBOLS)
    if response.status_code == HTTP_OK:
        meta_task = asyncio.create_task(meta(response.json()))

        symbols = [symbol['symbol'] for symbol in response.json()['symbols']
                                          if symbol['status'] == "ENABLED" and \
                                          symbol['isSpotTradingAllowed'] == True
                                          and symbol['permissions'] == ["SPOT"]]
        for symbol in symbols:
            TRADE_STATS[symbol] = 0
            ORDERBOOKS_STATS[symbol] = 0
        symbol_chunks = divide_on_chunks(symbols,
                                         CHUNK_SIZE)
        stats_task = asyncio.create_task(print_stats(TRADE_STATS, ORDERBOOKS_STATS))
        del response
        await asyncio.wait([asyncio.create_task(handle_socket(chunk)) for chunk in symbol_chunks])
    else:
        del response
def main():
    asyncio.get_event_loop().run_until_complete(handler())
main()
