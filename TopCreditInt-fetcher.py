import json
import websockets
import time
import asyncio
import requests
import zlib
API_URL = 'https://app.tope.com'
API_SYMBOLS = '/MarketV2/getSpotList?app_id=AwyOTFRlsfQ5mRkqwCNaEd5T'
API_PRECISION = '/Lever/symbols?app_id=AwyOTFRlsfQ5mRkqwCNaEd5T&symbol='
WSS_URL = 'wss://wsv3.wietees.com/?m=itI47Lj85Qf3XCsPmKca1rPuOJPto9kKWgYc/' \
          'HgozGU5ccW9CV91CPiy96Hn29NMjeVwjcfpoynyD4xzHtmOT/tfTQ==&k=hWyfqpfTamXoZm3tqgzd'
API_REQUESTS_TIMEOUT = 0.0001
SUBSCRIBE_TIMEOUT = 0.03
PING_TIMEOUT = 4
last_trades = {}
async def meta(response):
    for i in response.json()['data']:
        precision_response = requests.post(API_URL + API_PRECISION + str(i['symbol_id']))
        if precision_response.status_code == 200:
            print("@MD", precision_response.json()['data']['name'], 'spot',
                  precision_response.json()['data']['coin_from'].upper(),
                  precision_response.json()['data']['coin_to'].upper(),
                  precision_response.json()['data']['precision'][1],
                  1, 1, 0, 0, end='\n')
            await asyncio.sleep(API_REQUESTS_TIMEOUT)
        else:
            continue
    print("@MDEND")
async def subscribe(ws, response):
    for i in response['data']:
        await ws.send(json.dumps(
            {
                "action": "Topic.sub",
                "data": {
                            "type": "market",
                            "app_id": "AwyOTFRlsfQ5mRkqwCNaEd5T",
                            "symbol": i['symbol_id']
                        }
            }
        ))
        await asyncio.sleep(SUBSCRIBE_TIMEOUT)
        await ws.send(json.dumps(
            {
                "action": "Topic.sub",
                "data": {
                            "type": "depth",
                            "symbol": i['symbol_id']
                        }
            }
        ))
        await asyncio.sleep(SUBSCRIBE_TIMEOUT)
        await ws.send(json.dumps(
            {
                "action": "Topic.sub",
                "data": {
                            "type": "order",
                            "symbol": i['symbol_id']
                        }
            }
        ))
        await asyncio.sleep(SUBSCRIBE_TIMEOUT)
async def heartbeat(ws):
    while True:
        await ws.send(json.dumps('ping'))
        await asyncio.sleep(PING_TIMEOUT)
def print_trades(data,symbol):
    if last_trades[data['params']['p']] == '':
        last_trades[data['params']['p']] = data['data'][0]
        return
    if last_trades[data['params']['p']] == data['data'][0]:
        return
    else:
        i = 0
        while data['data'][i] != last_trades[data['params']['p']]:
            if i >= 5:
                break
            if data['data'][i]['m'] == '0':
                print("!", round(time.time() * 1000), symbol.replace('_', '/'), 'S', data['data'][i]['p'], data['data'][i]['q'], end='\n')
            if data['data'][i]['m'] == '1':
                print("!", round(time.time() * 1000), symbol.replace('_', '/'), 'B', data['data'][i]['p'], data['data'][i]['q'], end='\n')
            i+=1
        last_trades[data['params']['p']] = data['data'][0]
def print_orderbooks(data, symbol, isSnapshot):
    if isSnapshot:
        if data['data']['b'] != []:
            print("$", round(time.time() * 1000), symbol.replace('_', '/'), 'B',
                  '|'.join(i[1] + "@" + i[0] for i in data['data']['b']), 'R', end='\n')
        if data['data']['a'] != []:
            print("$", round(time.time() * 1000), symbol.replace('_', '/'), 'S',
                  '|'.join(i[1] + "@" + i[0] for i in data['data']['a']), 'R', end='\n')
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = {}
        for i in response.json()['data']:
            symbols[str(i['symbol_id'])] = i['name'].upper()
            last_trades[str(i['symbol_id'])] = ''
        meta_task = asyncio.create_task(meta(response))
        await meta_task
        async for ws in websockets.connect(WSS_URL,ping_interval=None):
            try:
                sub_task = asyncio.create_task(subscribe(ws, response.json()))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    try:
                        data_encoded = await ws.recv()
                        data = zlib.decompress(data_encoded)
                        dataJSON = json.loads(data)
                        if dataJSON['action'] == 'Pushdata.depth':
                            print_orderbooks(dataJSON, symbols[dataJSON['params']['p']], 1)
                        if dataJSON['action'] == 'Pushdata.orderbook':
                            print_trades(dataJSON,symbols[dataJSON['params']['p']])
                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())