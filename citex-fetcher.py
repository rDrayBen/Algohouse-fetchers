import json
import requests
import asyncio
import time
import websockets
import gzip

API_URL = 'https://www.citex.io/fe-ex-api/'
API_SPOT_SYMBOLS_URL = '/common/public_info_market'
WS_URL = 'wss://ws.citex.club/kline-api/ws'
WS_PUBLIC_SPOT_TRADE = '_trade_ticker'
WS_PUBLIC_SPOT_DEPTH_ORDERBOOK = '_depth_step0'
TIMEOUT = 0.1
PING_TIMEOUT = 10
response = requests.post(API_URL + API_SPOT_SYMBOLS_URL)
markets = [x for x in response.json()['data']['market']['market']['USDT1742']]
symbols = [response.json()['data']['market']['market']['USDT1742']
           [x]['symbol'] for x in markets]
symbol_names = {}


def format_decimal(value):
    try:
        if 'e' in str(value) or 'E' in str(value):
            parts = str(value).lower().split('e')
            print(int(parts[1]))
            precision = len(parts[0]) + abs(int(parts[1])) - 2
            return format(value, f'.{precision}f')
        else:
            return float(value)
    except:
        return value


async def meta(response):
    for i in markets:
        res = response.json()['data']['market']['market']['USDT1742'][i]
        assets = res['showName'].upper().split('/')
        print("@MD", res['showName'].upper(), "spot", assets[0], assets[1], res['volume'],
              1, 1, 0, 0, end="\n")
        symbol_names[res['symbol']] = res['showName'].upper()
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message=json.dumps({'pong': int(time.time())}))
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    try:
        symbol = data["channel"].split('_')[1]
        for i in data["tick"]['data']:
            print('!', round(time.time() * 1000),
                  symbol_names[symbol], i['side'][0].upper(), str(format_decimal(i['price'])), str(format_decimal(i['amount'])))
    except:
        pass


def print_orderbooks(data):
    try:
        symbol = data["channel"].split('_')[1]
        if data["tick"]["asks"] != []:
            print('$', round(time.time() * 1000), symbol_names[symbol], 'S', '|'.join(
                str(format_decimal(i[1])) + '@' + str(format_decimal(i[0])) for i in data["tick"]["asks"]), 'R')
        if data["tick"]["buys"] != []:
            print('$', round(time.time() * 1000), symbol_names[symbol], 'B', '|'.join(
                str(format_decimal(i[1])) + '@' + str(format_decimal(i[0])) for i in data["tick"]["buys"]), 'R')
    except:
        pass


async def subscribe(ws, symbols_):
    for i in range(len(symbols_)):
        await ws.send(message=json.dumps({"event": "sub",
                                          "params": {
                                              "channel": f"market_{symbols_[i]}{WS_PUBLIC_SPOT_TRADE}",
                                              "cb_id": f"{symbols_[i]}",
                                              "top": 100
                                          }
                                          }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps({"event": "sub",
                                          "params": {
                                              "channel": f"market_{symbols_[i]}{WS_PUBLIC_SPOT_DEPTH_ORDERBOOK}",
                                              "cb_id": f"{symbols_[i]}"
                                          }
                                          }))
        await asyncio.sleep(TIMEOUT)


async def main():
    meta_task = asyncio.create_task(meta(response))
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbols))
            ping_task = asyncio.create_task(heartbeat(ws))
            while True:
                try:
                    data_gzip = await ws.recv()
                    data = gzip.decompress(data_gzip).decode()
                    data_json = json.loads(data)                   
                    if WS_PUBLIC_SPOT_TRADE in data_json['channel']:
                        print_trades(data_json)
                    if WS_PUBLIC_SPOT_DEPTH_ORDERBOOK in data_json['channel']:
                        print_orderbooks(data_json)
                except:
                    continue
        except:
            continue


asyncio.run(main())
