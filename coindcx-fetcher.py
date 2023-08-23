import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://api.coindcx.com'
API_SPOT_SYMBOLS_URL = '/exchange/v1/markets_details'
WS_URL = 'wss://stream.coindcx.com/socket.io/?EIO=3&transport=websocket'
WS_PUBLIC_SPOT_TRADE = '@trades'
WS_PUBLIC_SPOT_ORDERBOOKS = '@orderbook@50'
TIMEOUT = 0.1
PING_TIMEOUT = 0
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['pair'] for x in response.json()]
symbols_parts = []
trades = 'new-trade'
deltas = 'depth-update'
snapshots = 'depth-snapshot'


def format_decimal(value):
    try:
        if 'e' in str(value) or 'E' in str(value):
            parts = str(value).lower().split('e')
            precision = len(parts[0]) + abs(int(parts[1])) - 2
            return format(float(value), f'.{precision}f')
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


async def meta(response):
    for i in response.json():
        print("@MD", i['pair'].split('-')[1].upper(), "spot", i['base_currency_short_name'].upper(), i['target_currency_short_name'].upper(), i['base_currency_precision'],
              1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message='2')
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data, symbol):
    try:
        data_json = json.loads(data['data'])
        if data_json['m']:
            bs = "S"
        else:
            bs = "B"
        print('!', round(time.time() * 1000), symbol.split('-')[1].upper(), bs, str(format_decimal(data_json['p'])), str(format_decimal(data_json['q'])))
    except:
        pass


def print_orderbooks(data, symbol, is_snapshot):
    try:
        data_json = json.loads(data['data'])
        if is_snapshot:
            if data_json['asks']:
                print('$', round(time.time() * 1000), symbol.split('-')[1].upper(), 'S', '|'.join(
                    str(format_decimal(value)) + '@' + str(format_decimal(key)) for key, value in data_json['bids'].items()), 'R')
            if data_json['bids']:
                print('$', round(time.time() * 1000), symbol.split('-')[1].upper(), 'B', '|'.join(
                    str(format_decimal(value)) + '@' + str(format_decimal(key)) for key, value in data_json['bids'].items()), 'R')
        else:
            if data_json['asks']:
                print('$', round(time.time() * 1000), symbol.split('-')[1].upper(), 'S', '|'.join(
                    str(format_decimal(value)) + '@' + str(format_decimal(key)) for key, value in data_json['asks'].items()))
            if data_json['bids']:
                print('$', round(time.time() * 1000), symbol.split('-')[1].upper(), 'B', '|'.join(
                    str(format_decimal(value)) + '@' + str(format_decimal(key)) for key, value in data_json['bids'].items()))
    except:
        pass


async def subscribe(ws, symbols_):
    for i in range(len(symbols_)): 
        await ws.send(message=f'42{json.dumps(["join", {"channelName": f"{symbols_[i]}{WS_PUBLIC_SPOT_TRADE}"}, {"x-source": "web"}])}')
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=f'42{json.dumps(["join", {"channelName": f"{symbols_[i]}{WS_PUBLIC_SPOT_ORDERBOOKS}"}, {"x-source": "web"}])}')
        await asyncio.sleep(TIMEOUT)


async def handle_socket(symbol):
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            ping_task = asyncio.create_task(heartbeat(ws))
            while True:
                try:
                    data = await ws.recv()
                    if data[0] == '4' and data[1] == '2':
                        data_sliced = data[2:]
                        data_json = json.loads(data_sliced)
                        if data_json[1]['event'] == trades:
                            print_trades(data_json[1], symbol[0])
                        if data_json[1]['event'] == deltas:
                            print_orderbooks(data_json[1], symbol[0], 0)
                        if data_json[1]['event'] == snapshots:
                            print_orderbooks(data_json[1], symbol[0], 1)  
                except:
                    continue
        except KeyboardInterrupt:
            print("Keyboard Interupt")
            exit(1)
        except Exception as conn_c:
            print(f"WARNING: connection exception {conn_c} occurred")
            continue


async def handler():
    meta_task = asyncio.create_task(meta(response))
    divide_on_parts(symbols, 1)
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in symbols_parts])

async def main():
    await handler()

asyncio.run(main())
