import json
import websockets
import time
import asyncio
import requests
API_URL = 'https://api.chiliz.net'
API_SYMBOLS = '/openapi/v1/brokerInfo'
WSS_URL = 'wss://ws.chiliz.net/ws/quote/v1?lang=en-us'
TIMEOUT = 0.1
PING_TIMEOUT = 60
async def meta(response):
    for i in response.json()['symbols']:
        if i['status'] == 'TRADING':
            precission = str(i["quotePrecision"])[::-1].find(".")
            if precission == -1:
                print("@MD", i['symbol'], 'spot', i['baseAsset'], i['quoteAsset'],
                      0, 1, 1, 0, 0, end='\n')
            else:
                print("@MD", i['symbol'], 'spot', i['baseAsset'], i['quoteAsset'],
                      precission, 1, 1, 0, 0, end='\n')
    print("@MDEND")
async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({'ping': round(time.time() * 1000)}))
        await asyncio.sleep(PING_TIMEOUT)
async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps(
            {
                "symbol": f"{i}",
                "topic": "depth",
                "event": "sub",
                "params": {
                    "binary": False
                }
            }
        ))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps(
            {
                "symbol": f"{i}",
                "topic": "diffDepth",
                "event": "sub",
                "params": {
                    "binary": False
                }
            }
        ))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps(
            {
                "symbol": f"{i}",
                "topic": "trade",
                "event": "sub",
                "params": {
                    "binary": False
                }
            }
        ))
        await asyncio.sleep(TIMEOUT)
def print_trade(data):
    if data['f'] == False:
        for i in data['data']:
            print("!", round(time.time() * 1000), data['symbol'], "".join('B' if i['m'] else 'S'),
                  i['p'], i['q'], end='\n')
def print_orderbooks(data, isSnapshot):
    if isSnapshot:
        if data['data'][0]['b'] != []:
            print("$", round(time.time() * 1000), data['data'][0]['s'], 'B',
                  '|'.join(i[1] + "@" + i[0] for i in data['data'][0]['b']),
                  "R", end='\n')
        if data['data'][0]['a'] != []:
            print("$", round(time.time() * 1000), data['data'][0]['s'], 'S',
                  '|'.join(i[1] + "@" + i[0] for i in data['data'][0]['a']),
                  "R", end='\n')
    else:
        if data['data'][0]['b'] != []:
            print("$", round(time.time() * 1000), data['symbol'], 'B',
                  '|'.join(i[1] + "@" + i[0] for i in data['data'][0]['b']),
                  end='\n')
        if data['data'][0]['a'] != []:
            print("$", round(time.time() * 1000), data['symbol'], 'S',
                  '|'.join(i[1] + "@" + i[0] for i in data['data'][0]['a']),
                  end='\n')
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['symbol'] for i in response.json()['symbols'] if i['status'] == 'TRADING']
        meta_task = asyncio.create_task(meta(response))
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    try:
                        data = await ws.recv()
                        dataJSON = json.loads(data)
                        if dataJSON['topic'] == 'depth':
                            print_orderbooks(dataJSON, 1)
                        if dataJSON['topic'] == 'diffDepth':
                            print_orderbooks(dataJSON, 0)
                        if dataJSON['topic'] == 'trade':
                            print_trade(dataJSON)
                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())