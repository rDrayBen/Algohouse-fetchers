import json
import websockets
import time
import asyncio
import requests
import gzip
API_URL = 'https://api.bitmake.com'
API_SYMBOLS = '/u/v1/base/symbols'
WSS_URL = 'wss://ws.bitmake.com/t/v1/ws'
DEPTH5_LEVEL = 1
DEPTH100_LEVEL = 5
SUB_TIMEOUT = 0.01
PING_TIMEOUT = 5
async def meta(response):
    if response.status_code == 200:
        for i in response.json():
            if i['symbol'] != i['baseToken'] + i['quoteToken']:
                print("@MD", i['symbol'], 'spot', i['baseToken'], i['quoteToken'], i['quantityPrecision'], 1, 1, 0, 0,
                      end='\n')
        print('@MDEND')
async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps(
            {
                "tp": "diffMergedDepth",
                "e": "sub",
                "ps": {
                    "symbol": i,
                    "depthLevel": DEPTH100_LEVEL
                }
            }
        ))
        await asyncio.sleep(SUB_TIMEOUT)
        await ws.send(json.dumps(
            {
                "tp": "diffMergedDepth",
                "e": "sub",
                "ps": {
                    "symbol": i,
                    "depthLevel": DEPTH5_LEVEL
                }
            }
        ))
        await asyncio.sleep(SUB_TIMEOUT)
        await ws.send(json.dumps(
            {
                "tp": "trade",
                "e": "sub",
                "ps": {
                    "symbol": i
                }
            }
        ))
        await asyncio.sleep(SUB_TIMEOUT)
async def heartbeat(ws):
    while True:
        await ws.send(json.dumps(
            {"ping": round(time.time() * 1000)}
        ))
        await asyncio.sleep(PING_TIMEOUT)
def print_trade(data):
     for i in data['d']:
         if i['m']:
            print("!", round(time.time() * 1000), data['ps']['symbol'], 'B', i['p'], i['q'], end='\n')
         else:
            print("!", round(time.time() * 1000), data['ps']['symbol'], 'S', i['p'], i['q'], end='\n')
def print_orderbook(data, isSnapshot):
   if isSnapshot:
       if data['d'][0]['b'] != []:
           print("$", round(time.time() * 1000), data['ps']['symbol'], 'B',
                 '|'.join(i[1] + "@" + i[0] for i in data['d'][0]['b']), 'R', end='\n')
       if data['d'][0]['a'] != []:
           print("$", round(time.time() * 1000), data['ps']['symbol'], 'S',
                 '|'.join(i[1] + "@" + i[0] for i in data['d'][0]['a']), 'R', end='\n')
   else:
       if data['d'][0]['b'] != []:
           print("$", round(time.time() * 1000), data['ps']['symbol'], 'B',
                 '|'.join(i[1] + "@" + i[0] for i in data['d'][0]['b']), end='\n')
       if data['d'][0]['a'] != []:
           print("$", round(time.time() * 1000), data['ps']['symbol'], 'S',
                 '|'.join(i[1] + "@" + i[0] for i in data['d'][0]['a']), end='\n')
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['baseToken'] + "_" + i['quoteToken'] for i in response.json()]
        symbols = sorted(set(symbols))
        trade_snapshot = {}
        for i in symbols:
            trade_snapshot[i] = 0
        meta_task = asyncio.create_task(meta(response))
        async for ws in websockets.connect(WSS_URL, ping_interval=None):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    data_encoded = await ws.recv()
                    data = gzip.decompress(data_encoded)
                    dataJSON = json.loads(data)
                    if 'm' in dataJSON:
                        continue
                    if 'tp' in dataJSON:
                        if dataJSON['tp'] == 'diffMergedDepth':
                            if dataJSON['ps']['depthLevel'] == DEPTH5_LEVEL:
                                print_orderbook(dataJSON, 0)
                            if dataJSON['ps']['depthLevel'] == DEPTH100_LEVEL:
                                print_orderbook(dataJSON, 1)
                        if dataJSON['tp'] == 'trade':
                            if trade_snapshot[dataJSON['ps']['symbol']] == 0:
                                trade_snapshot[dataJSON['ps']['symbol']] += 1
                                continue
                            else:
                                print_trade(dataJSON)
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())