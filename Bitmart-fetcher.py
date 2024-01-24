import json
import requests
import asyncio
import websockets
import os
import zlib
from CommonFunctions.CommonFunctions import get_unix_time, print_stats

API_URL = 'https://api-cloud.bitmart.com/spot/v1'
API_SPOT_SYMBOLS_URL = '/symbols'
WS_URL = 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1'
WS_PUBLIC_SPOT_TRADE = "spot/trade"
WS_PUBLIC_SPOT_ORDERBOOK = "spot/depth50"
TIMEOUT = 1
PING_TIMEOUT = 0.1
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x for x in response.json()['data']['symbols']]
symbols_parts = []
old_trades = {}
trades_stats = {}
orderbooks_stats = {}
for symbol in symbols:
    old_trades[symbol] = 0
    trades_stats[symbol.upper()] = 0
    orderbooks_stats[symbol.upper()] = 0


def divide_on_parts(symbols, part_size=100):
    parts_amount = round(len(symbols)/part_size)
    last = 0
    for i in range(parts_amount):
        symbols_parts.append(symbols[i*part_size:part_size*(i+1)])
        last = part_size*(i+1)
    if len(symbols) - last > 0:
        symbols_parts.append(symbols[last: len(symbols)])
    

async def meta(response):
    for i in response.json()['data']['symbols']:
        assets = i.split('_')
        print("@MD", i.upper(), "spot",
              assets[0].upper(), assets[1].upper(), -1, 1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message="ping")
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    try:
        for i in data['data']:
            print('!', get_unix_time(),
                  i['symbol'], i['side'][0].upper(), i['price'], i['size'])
            trades_stats[i['symbol']] += 1
    except:
        pass


def print_orderbooks(data):
    try:
        for data_part in data['data']:
            if data_part['asks'] != []:
                print('$', get_unix_time(), data_part['symbol'], 'S',
                        '|'.join(i[1] + '@' + i[0] for i in data_part['asks']), 'R')
                orderbooks_stats[data_part['symbol']] += 1
            if data_part['bids'] != []:
                print('$', get_unix_time(), data_part['symbol'], 'B',
                        '|'.join(i[1] + '@' + i[0] for i in data_part['bids']), 'R')
                orderbooks_stats[data_part['symbol']] += 1
    except:
        pass


async def subscribe(ws, symbols_):
    for symbol in symbols_:
        await ws.send(message=json.dumps({
            "op": "subscribe",
            "args": [f"{WS_PUBLIC_SPOT_TRADE}:{symbol}"]
        }))
        await asyncio.sleep(TIMEOUT)
        if (os.getenv("SKIP_ORDERBOOKS") == None):  # don't subscribe or report orderbook changes
            await ws.send(message=json.dumps({
                "op": "subscribe",
                "args": [f"{WS_PUBLIC_SPOT_ORDERBOOK}:{symbol}"]
            }))
            await asyncio.sleep(TIMEOUT)


async def handle_socket(symbol):
    async for ws in websockets.connect(WS_URL, ping_interval=None):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            #ping_task = asyncio.create_task(heartbeat(ws))
            while True:
                try:
                    data = await ws.recv()
                    data_json = json.loads(data)
                    if data_json['table'] == WS_PUBLIC_SPOT_TRADE:
                        if old_trades[data_json['data'][0]['symbol']] < 2:
                            old_trades[data_json['data'][0]['symbol']] += 1
                        else:
                            print_trades(data_json)
                    if data_json['table'] == WS_PUBLIC_SPOT_ORDERBOOK:
                        print_orderbooks(data_json)
                except:
                    continue
        except KeyboardInterrupt:
            print("Keyboard Interupt")
            exit(1)
        except Exception as conn_c:
            print(f"WARNING: connection exception {conn_c} occurred")
            await asyncio.sleep(1)
            continue


async def handler():
    meta_task = asyncio.create_task(meta(response))
    stats_task = asyncio.create_task(print_stats(trades_stats, orderbooks_stats))
    divide_on_parts(symbols, int(len(symbols) // 10))
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in symbols_parts])


async def main():
    await handler()

asyncio.run(main())
