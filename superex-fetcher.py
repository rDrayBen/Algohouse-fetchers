import json
import asyncio
import time
import websockets
import base64
import gzip

WS_URL = 'wss://api.superexchang.com/socket/ws'
WS_PUBLIC_SPOT_SYMBOLS = 'spot/symbol:spot'
WS_PUBLIC_SPOT_TRADE = 'spot/trade:'
WS_PUBLIC_SPOT_DEPTH_ORDERBOOKS = 'spot/depth:'
TIMEOUT = 0.1
PING_TIMEOUT = 5
symbols = []
symbols_parts = []
old_trades = {}


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


def divide_on_parts(symbols, part_size=100):
    parts_amount = round(len(symbols)/part_size)
    last = 0
    for i in range(parts_amount):
        symbols_parts.append(symbols[i*part_size:part_size*(i+1)])
        last = part_size*(i+1)
    if len(symbols) - last > 0:
        symbols_parts.append(symbols[last: len(symbols)])


def meta(data):
    for i in data['data']:
        symbols.append(i['symbol'])
        assets = i['symbol'].split("_")
        print("@MD", i['symbol'].upper(), "spot", assets[0].upper(), assets[1].upper(),
              -1, 1, 1, 0, 0, end="\n")
    print("@MDEND")
    for i in symbols:
        old_trades[i] = 0


async def heartbeat(ws):
    while True:
        await ws.send(message="ping")
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    try:
        if data['data'][0]['type'] == 1:
            bs = 'B'
        else:
            bs = 'S'
        print('!', round(time.time() * 1000),
              data['symbol'].upper(), bs, str(format_decimal(data['data'][0]['price'])), str(format_decimal(data['data'][0]['volume'])))
    except:
        pass


def print_orderbooks(data):
    try:
        if data['data']['asks'] != []:
            print('$', round(time.time() * 1000), data['symbol'].upper(), 'S', '|'.join(str(format_decimal(
                i['amount'])) + '@' + str(format_decimal(i['price'])) for i in data['data']['asks']), 'R')
        if data['data']['bids'] != []:
            print('$', round(time.time() * 1000), data['symbol'].upper(), 'B', '|'.join(str(format_decimal(
                i['amount'])) + '@' + str(format_decimal(i['price'])) for i in data['data']['bids']), 'R')
    except:
        pass


async def symbols_subscribe(ws):
    await ws.send(message=json.dumps({
        "op": "sub",
        "args": f"{WS_PUBLIC_SPOT_SYMBOLS}"
    }))
    await asyncio.sleep(TIMEOUT)
    await ws.send(message=json.dumps({
        "op": "unsub",
        "args": f"{WS_PUBLIC_SPOT_SYMBOLS}"
    }))
    await asyncio.sleep(TIMEOUT)
    collected = False
    while not collected:
        try:
            data_encoded = await ws.recv()
            data_gzip = base64.b64decode(data_encoded)
            data = gzip.decompress(data_gzip).decode()
            data_json = json.loads(data)
            if data_json['table'] == 'spot/symbol':
                meta(data_json)
                collected = True
        except:
            continue


async def subscribe(ws, symbols_):
    for i in range(len(symbols_)):
        await ws.send(message=json.dumps({
            "op": "sub",
            "args": f"{WS_PUBLIC_SPOT_TRADE}{symbols_[i]}"
        }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({
            "op": "sub",
            "args": f"{WS_PUBLIC_SPOT_DEPTH_ORDERBOOKS}{symbols_[i]}"
        }))
        await asyncio.sleep(TIMEOUT)

async def handle_symbols():
    async for ws in websockets.connect(WS_URL):
        try:
            symbol_task = asyncio.create_task(symbols_subscribe(ws))
            await symbol_task
            break
        except:
            continue


async def handle_socket(symbol):
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            ping_task = asyncio.create_task(heartbeat(ws))
            async for message in ws:
                try:
                    data_encoded = await ws.recv()
                    data_gzip = base64.b64decode(data_encoded)
                    data = gzip.decompress(data_gzip).decode()
                    data_json = json.loads(data)
                    if data_json['table'] == 'spot/trade':
                        if old_trades[data_json['symbol']] < 2:
                            old_trades[data_json['symbol']] += 1
                        else:
                            print_trades(data_json)
                    if data_json['table'] == 'spot/depth':
                        print_orderbooks(data_json)
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
    divide_on_parts(symbols, 100)
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in symbols_parts[0:10]])


async def main():
    await handle_symbols()
    await handler()

asyncio.run(main())
