import json
import websockets
import time
import asyncio
import requests
API_URL = "https://bitexlive.com/api/public"
API2_URL = 'https://bitexlive.com:8443'
API_SYMBOLS = "/tickers"
API_SYMBOLS_INFO = '/charts/symbols?symbol='
API_SNAPSHOT = 'https://bitexlive.com/api/public/orderBook?filter='
WSS_URL = "wss://bitexlive.com:8443/socket.io/?EIO=3&transport=websocket"
SUB_TIMEOUT = 0.001
REST_TIMEOUT = 0.5
PING_TIMEOUT = 24
SNAPSHOT_TIMEOUT = 1
coins_ids = {'BTC': 1, 'USDT': 9, 'BNB': 165, 'ETH': 6, 'LTC': 2, 'DOGE': 3,
             'LINK': 163, 'CAKE': 166, 'AAVE': 167, 'DGB': 14, 'XRP': 7,
             'BCH': 15, 'ZRH': 114, 'NEXO': 79, 'HOT': 139, 'XVG': 17,
             'TRX': 128, 'G999': 161, 'STRM': 170, 'LYS': 168, 'ONION': 38,
             'NYC': 78, 'REV': 164, 'VOLT': 172, 'NAV': 74, 'GRT': 162
             }
async def meta(response):
    for i in response.json():
        response2 = requests.get(API2_URL + API_SYMBOLS_INFO + i['tradingPairs'])
        break_str = i['tradingPairs'].split("_", 2)
        if response2.status_code != 200:
            print("@MD", i['tradingPairs'], 'spot', break_str[0], break_str[1], 0,
                  1, 1, 0, 0, end='\n')
            continue
        precision = str(response2.json()['pricescale']).count('0')
        print("@MD", i['tradingPairs'], 'spot', break_str[0], break_str[1], precision,
              1, 1, 0, 0, end='\n')
    print('@MDEND')
async def subscribe(ws, data):
    for i in data:
        break_str = i['tradingPairs'].split("_", 2)
        await ws.send(message='42["tradeData",' + f'["{coins_ids[break_str[1]]}", "{coins_ids[break_str[0]]}"]]')
        await asyncio.sleep(SUB_TIMEOUT)
        await ws.send(message='42["exchangeBuy",' + f'["{coins_ids[break_str[1]]}", "{coins_ids[break_str[0]]}"]]')
        await asyncio.sleep(SUB_TIMEOUT)
async def heartbeat(ws):
    while True:
        await ws.send(message="2")
        await asyncio.sleep(PING_TIMEOUT)
async def rest_print_snapshot(data):
    while True:
        for i in data:
            response_ = requests.get(API_SNAPSHOT+i+"&limit=30")
            print("$", round(time.time()*1000), i, 'B',
                  '|'.join(str(j[1]) + "@" + str(j[0]) for j in response_.json()['bids']), "R")
            print("$", round(time.time() * 1000), i, 'S',
                  '|'.join(str(j[1]) + "@" + str(j[0]) for j in response_.json()['asks']), "R")
            await asyncio.sleep(REST_TIMEOUT)
def print_trade(data):
    base = [k for k, v in coins_ids.items() if v == data[1]['data'][0]['trade_to_wallet_id']]
    quote = [k for k, v in coins_ids.items() if v == data[1]['data'][0]['trade_from_wallet_id']]
    print('!', round(time.time() * 1000), str(base[0]) + "_" + str(quote[0]),
          data[1]['data'][0]['trade_type'][0].upper(), str(data[1]['data'][0]['trade_bid']),
          str(data[1]['data'][0]['trade_unit']), end='\n')
def print_orderbook(data):
    base = [k for k, v in coins_ids.items() if v == data[1]['data'][0]['from']]
    quote = [k for k, v in coins_ids.items() if v == data[1]['data'][0]['to']]
    if data[0] == 'exchangeBuy':
        print("$", round(time.time()*1000), str(base[0]) + "_" + str(quote[0]), 'B',
              "|".join(str(i['unit']) + "@" + str(i['_id']) for i in data[1]['data']), 'R', end='\n')
    if data[0] == 'exchangeSell':
        print("$", round(time.time() * 1000), str(base[0]) + "_" + str(quote[0]), 'S',
              "|".join(str(i['unit']) + "@" +str(i['_id']) for i in data[1]['data']), 'R', end='\n')
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['tradingPairs'] for i in response.json()]
        meta_task = asyncio.create_task(meta(response))
        await meta_task
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, response.json()))
                snapshot_task = asyncio.create_task(rest_print_snapshot(symbols))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    try:
                        data = await ws.recv()
                        if data[0] == '4' and data[1] == '2':
                            data_sliced = data[2:]
                            dataJSON = json.loads(data_sliced)
                            if dataJSON[0] == 'exchangeBuy' or dataJSON[0] == 'exchangeSell':
                                print_orderbook(dataJSON)
                            if dataJSON[0] == 'tradeData':
                                print_trade(dataJSON)

                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())