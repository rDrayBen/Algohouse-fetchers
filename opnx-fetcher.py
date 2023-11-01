import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://api.opnx.com'
API_SPOT_SYMBOLS_URL = '/v3/markets'
WS_URL = 'wss://api.opnx.com/v2/websocket'
WS_PUBLIC_SPOT_TRADE = 'trade:'
WS_PUBLIC_SPOT_DEPTH_DELTA = 'depthUpdate:'
WS_PUBLIC_SPOT_DEPTH_WHOLE = 'depth:'
TIMEOUT = 10
PING_TIMEOUT = 10
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['marketCode'] for x in response.json()['data']]
symbols_parts = []
trades = 'trade'
deltas = 'depthUpdate-diff'
snapshots = 'depth'


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


async def meta(response):
    for i in response.json()['data']:
        print("@MD", i['marketCode'].upper(), "spot", i['base'].upper(), i['counter'].upper(), len(str(i['minSize']).split('.')[-1]),
              1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message='ping')
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    try:
        for i in data['data']:
            print('!', round(time.time() * 1000),
                  i['marketCode'].upper(), i['side'].upper(), str(format_decimal(i['price'])), str(format_decimal(i['quantity'])))
    except:
        pass


def print_orderbooks(data, is_snapshot):
    try:
        if is_snapshot:
            if data["data"]["asks"] != []:
                print('$', round(time.time() * 1000), data['data']['marketCode'].upper(), 'S', '|'.join(
                    str(format_decimal(i[1])) + '@' + str(format_decimal(i[0])) for i in data["data"]["asks"]), 'R')
            if data["data"]["bids"] != []:
                print('$', round(time.time() * 1000), data['data']['marketCode'].upper(), 'B', '|'.join(
                    str(format_decimal(i[1])) + '@' + str(format_decimal(i[0])) for i in data["data"]["bids"]), 'R')
        else:
            if data["data"]["asks"] != []:
                print('$', round(time.time() * 1000), data['data']['marketCode'].upper(), 'S', '|'.join(
                    str(format_decimal(i[1])) + '@' + str(format_decimal(i[0])) for i in data["data"]["asks"]))
            if data["data"]["bids"] != []:
                print('$', round(time.time() * 1000), data['data']['marketCode'].upper(), 'B', '|'.join(
                    str(format_decimal(i[1])) + '@' + str(format_decimal(i[0])) for i in data["data"]["bids"]))
    except:
        pass


async def subscribe(ws, symbols_):
    for i in range(len(symbols_)):
        await ws.send(message=json.dumps({"op": "subscribe",
                                          "args": [
                                              f"{WS_PUBLIC_SPOT_TRADE}{symbols_[i]}"
                                          ]
                                          }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({"op": "subscribe",
                                          "args": [
                                              f"{WS_PUBLIC_SPOT_DEPTH_DELTA}{symbols_[i]}"
                                          ]
                                          }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({"op": "subscribe",
                                          "args": [
                                              f"{WS_PUBLIC_SPOT_DEPTH_WHOLE}{symbols_[i]}"
                                          ]
                                          }))
        await asyncio.sleep(TIMEOUT)


async def handle_socket(symbol):
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            ping_task = asyncio.create_task(heartbeat(ws))
            while True:
                try:
                    data = await ws.recv()
                    data_json = json.loads(data)
                    if data_json["table"] == trades:
                        print_trades(data_json)
                    if data_json["table"] == deltas:
                        print_orderbooks(data_json, 0)
                    if data_json["table"] == snapshots:
                        print_orderbooks(data_json, 1)
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
    divide_on_parts(symbols, int(len(symbols) / 4))
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in symbols_parts])


async def main():
    await handler()

asyncio.run(main())
