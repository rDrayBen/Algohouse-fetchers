import json
import requests
import asyncio
import time
import websockets
from decimal import Decimal 

API_URL = 'https://www.catex.io/api'
API_SPOT_BASE_SYMBOLS_URL = '/token/baseCurrency'
API_SPOT_SYMBOLS_URL = '/token/currency?baseCurrency='
WS_URL = 'wss://www.catex.io/stream'
WS_PUBLIC_SPOT_TRADE = '/topic/trading/'
WS_PUBLIC_SPOT_ORDERBOOK_BUY = '/topic/order/buy/'
WS_PUBLIC_SPOT_ORDERBOOK_SELL = '/topic/order/sell/'
TIMEOUT = 0.1
PING_TIMEOUT = 4
response_base = requests.get(API_URL + API_SPOT_BASE_SYMBOLS_URL)
base_symbols = [x for x in response_base.json()['data']]
symbols = []
for symbol in base_symbols:
    response = requests.get(API_URL + API_SPOT_SYMBOLS_URL + symbol)
    for pair in response.json()['data']:
        symbols.append(pair)
    time.sleep(TIMEOUT)


def format_decimal(value):
    try:
        if 'e' in str(value) or 'E' in str(value):
            parts = str(value).lower().split('e')
            precision = len(parts[0]) + abs(int(parts[1])) - 2
            return format(value, f'.{precision}f')
        else:
            return value
    except:
        return value


async def meta(symbols):
    for i in symbols:
        symbol = i['baseCurrency'] + '-' + i['currency']
        print("@MD", symbol.upper(), "spot", i['baseCurrency'].upper(), i['currency'].upper(), -1,
              1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message="\n")
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    try:
        data = data.split('\n')
        pair = data[1].split('/')[-1] + '-' + data[1].split('/')[-2]
        data = json.loads(data[-1].replace('\x00', ''))
        if data['type'] == 'BUY':
            bs = 'B'
        else:
            bs = 'S'
        print('!', round(time.time() * 1000), pair.upper(), bs, str(format_decimal(data['price'])), str(format_decimal(data['amount'])))
    except:
        pass


def print_orderbooks(data, is_sell):
    try:
        data = data.split('\n')
        pair = data[1].split('/')[-1] + '-' + data[1].split('/')[-2]
        data = json.loads(data[-1].replace('\x00', ''))
        if is_sell:
            print('$', round(time.time() * 1000), pair, 'S', '|'.join(
                str(format_decimal(i['amount'])) + '@' + str(format_decimal(i['price'])) for i in data), 'R')
        else:
            print('$', round(time.time() * 1000), pair, 'B', '|'.join(
                str(format_decimal(i['amount'])) + '@' + str(format_decimal(i['price'])) for i in data), 'R')
    except:
        pass


async def subscribe(ws, symbols_):
    await ws.send(message="CONNECT\naccept-version:1.0,1.1,1.2\nheart-beat:4000,4000\n\n\u0000")
    await asyncio.sleep(TIMEOUT)
    k = 0
    for i in range(len(symbols_)):
        await ws.send(message=f"SUBSCRIBE\nid:sub-{k}\ndestination:{WS_PUBLIC_SPOT_TRADE}{symbols_[i]['currency']}/{symbols_[i]['baseCurrency']}\n\n\u0000")
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=f"SUBSCRIBE\nid:sub-{k+1}\ndestination:{WS_PUBLIC_SPOT_ORDERBOOK_BUY}{symbols_[i]['currency']}/{symbols_[i]['baseCurrency']}\n\n\u0000")
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=f"SUBSCRIBE\nid:sub-{k+2}\ndestination:{WS_PUBLIC_SPOT_ORDERBOOK_SELL}{symbols_[i]['currency']}/{symbols_[i]['baseCurrency']}\n\n\u0000")
        await asyncio.sleep(TIMEOUT)
        k += 3


async def main():
    meta_task = asyncio.create_task(meta(symbols))
    
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbols))
            ping_task = asyncio.create_task(heartbeat(ws))
            while True:
                try:
                    data = await ws.recv()
                    if WS_PUBLIC_SPOT_TRADE in data:
                        print_trades(data)
                    if WS_PUBLIC_SPOT_ORDERBOOK_BUY in data:
                        print_orderbooks(data, 0)
                    if WS_PUBLIC_SPOT_ORDERBOOK_SELL in data:
                        print_orderbooks(data, 1)
                except:
                    continue
        except:
            continue
    

asyncio.run(main())