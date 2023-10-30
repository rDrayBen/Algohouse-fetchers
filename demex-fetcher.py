import json
import asyncio
import time
import websockets
import base64
import gzip

WS_URL = 'wss://ws-api.carbon.network/ws'
WS_PUBLIC_SPOT_SYMBOLS = 'market_stats'
WS_PUBLIC_SPOT_TRADE = 'recent_trades:'
WS_PUBLIC_SPOT_DEPTH_ORDERBOOKS = 'books:'
TIMEOUT = 0.1
PING_TIMEOUT = 10
symbols = {'cmkt/109': 'SWTH/USD',
           'cmkt/110': 'ETH/USD',
           'swth_eth1': 'SWTH/ETH',
           'cmkt/111': 'WBTC/USD',
           'cmkt/115': 'USC/USD',
           'cmkt/136': 'RSWTH/SWTH'}
symbols_pows = {'cmkt/109': [0.00000000001, 0.0000000001],
                'cmkt/110': [0.000000000000000001, 0.001],
                'swth_eth1': [0.00000000001, 0.0000000001],
                'cmkt/111': [0.00000001, 0.0000000000001],
                'cmkt/115': [0.000000001, 0.000000000001],
                'cmkt/136': [0.00000000001, 1]}


def format_decimal(value):
    try:
        if 'e' in str(value) or 'E' in str(value):
            parts = str(value).lower().split('e')
            precision = len(parts[0]) + abs(int(parts[1])) - 2
            return format(value, f'.{precision}f')
        else:
            return float(value)
    except:
        return value


def meta(symbols_):
    for i in symbols_.values():
        assets = i.split("/")
        print("@MD", i, "spot", assets[0], assets[1],
              -1, 1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message="ping")
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    try:
        for i in data['result']:
            symbol = i['market']
            if i['maker_side'] == 'buy':
                bs = 'B'
            else:
                bs = 'S'
            print('!', round(time.time() * 1000),
                  symbols[symbol], bs,
                  str(format_decimal(
                      float(i['price']) * symbols_pows[symbol][1])),
                  str(format_decimal(float(i['quantity']) * symbols_pows[symbol][0])))
    except:
        pass


def print_orderbooks(data, is_snapshot):
    try:
        symbol = data['channel'].split(':')[1]
        if is_snapshot:
            if data["result"]["asks"] != []:
                print('$', round(time.time() * 1000), symbols[symbol], 'S', '|'.join(
                    str(format_decimal(
                        float(i['quantity']) * symbols_pows[symbol][0])) + '@'
                    + str(format_decimal(float(i['price']) * symbols_pows[symbol][1]))
                    for i in data["result"]["asks"]), 'R')
            if data["result"]["bids"] != []:
                print('$', round(time.time() * 1000), symbols[symbol], 'B', '|'.join(
                    str(format_decimal(
                        float(i['quantity']) * symbols_pows[symbol][0])) + '@'
                    + str(format_decimal(float(i['price']) * symbols_pows[symbol][1]))
                    for i in data["result"]["bids"]), 'R')
        else:
            bought = []
            sold = []
            for i in data['result']:
                if i['side'] == 'buy':
                    bought.append(i)
                else:
                    sold.append(i)
            if sold != []:
                print('$', round(time.time() * 1000), symbols[symbol], 'S', '|'.join(
                    str(format_decimal(
                        float(i['quantity']) * symbols_pows[symbol][0])) + '@'
                    + str(format_decimal(float(i['price'])
                                         * symbols_pows[symbol][1]))
                    for i in sold))
            if bought != []:
                print('$', round(time.time() * 1000), symbols[symbol], 'S', '|'.join(
                    str(format_decimal(
                        float(i['quantity']) * symbols_pows[symbol][0])) + '@'
                    + str(format_decimal(float(i['price'])
                                         * symbols_pows[symbol][1]))
                    for i in bought))
    except:
        pass


async def subscribe(ws, symbols_):
    k = 1
    keys = symbols_.keys()
    print(symbols_)
    for i in keys:
        await ws.send(message=json.dumps({"id": f"g{k}",
                                          "method": "subscribe",
                                          "params": {"channels": [f"{WS_PUBLIC_SPOT_DEPTH_ORDERBOOKS}{i}"]}
                                          }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({"id": f"g{k+1}",
                                          "method": "subscribe",
                                          "params": {"channels": [f"{WS_PUBLIC_SPOT_TRADE}{i}"]}
                                          }))
        await asyncio.sleep(TIMEOUT)
        k += 2


async def handle_socket(symbols):
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbols))
            ping_task = asyncio.create_task(heartbeat(ws))
            async for message in ws:
                try:
                    data = await ws.recv()
                    data_json = json.loads(data)
                    if WS_PUBLIC_SPOT_TRADE in data_json['channel']:
                        print_trades(data_json)
                    if WS_PUBLIC_SPOT_DEPTH_ORDERBOOKS in data_json['channel'] and data_json['update_type'] == 'delta':
                        print_orderbooks(data_json, 0)
                    if WS_PUBLIC_SPOT_DEPTH_ORDERBOOKS in data_json['channel'] and data_json['update_type'] == 'full_state':
                        print_orderbooks(data_json, 1)
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
    meta(symbols)
    await asyncio.wait([asyncio.create_task(handle_socket({symbol: symbols[symbol]})) for symbol in symbols])


async def main():
    await handler()

asyncio.run(main())
