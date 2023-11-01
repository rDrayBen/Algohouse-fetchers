import json
import websockets
import time
import asyncio
import requests
API_URL = 'https://nami.exchange'
API_SYMBOLS = '/api/v3/spot/config'
WSS_URL = 'wss://stream-asia2.nami.exchange/ws/?EIO=3&transport=websocket'
SUB_TIMEOUT = 0.001
PING_TIMEOUT = 23
async def meta(response):
    for i in response.json()['data']:
        if i['isSpotTradingAllowed'] and i['status'] == 'TRADING':
            print("@MD", i['symbol'], 'spot', i['baseAsset'], i['quoteAsset'], i['quotePrecision'], 1, 1, 0, 0, end='\n')
    print("@MDEND")
async def subscribe(ws, data):
    await ws.send(message='420["timesync",{"jsonrpc":"2.0","id":0,"method":"timesync"}]')
    for i in data:
        await ws.send(message='42["subscribe:recent_trade",' + f'"{i}"]')
        await asyncio.sleep(SUB_TIMEOUT)
        await ws.send(message='42["subscribe:depth",' + f'"{i}"]')
        await asyncio.sleep(SUB_TIMEOUT)
async def heartbeat(ws):
    while True:
        await ws.send(message="2")
        await asyncio.sleep(PING_TIMEOUT)
def print_trades(data):
    print('!', round(time.time() * 1000), data['s'], data['S'][0], data['p'], data['q'], end='\n')
def print_orderbooks(data, isSnapshot, precision):
    if isSnapshot:
        if data['bids'] != []:
            print("$", round(time.time() * 1000), data['symbol'], 'B',
                  "|".join(str("{:.{}f}".format(i[1], precision)) + "@" + str("{:.{}f}".format(i[0], precision))
                           for i in data['bids']), 'R', end='\n'
                  )
        if data['asks'] != []:
            print("$", round(time.time() * 1000), data['symbol'], 'S',
                  "|".join(str("{:.{}f}".format(i[1], precision),) + "@" + str("{:.{}f}".format(i[0], precision))
                           for i in data['asks']), 'R', end='\n'
                  )
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['symbol'] for i in response.json()['data'] if i['status'] == 'TRADING']
        precission_dict = {}
        for i in response.json()['data']:
            precission_dict[i['symbol']] = i['quotePrecision']

        meta_task = asyncio.create_task(meta(response))
        async for ws in websockets.connect(WSS_URL, ping_interval=None):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    try:
                        data = await ws.recv()
                        if data[0] == '4' and data[1] == '2':
                            data_sliced = data[2:]
                            dataJSON = json.loads(data_sliced)
                            if dataJSON[0] == 'spot:recent_trade:add':
                                print_trades(dataJSON[1])
                            if dataJSON[0] == 'spot:depth:update':
                                print_orderbooks(dataJSON[1], 1, precission_dict[dataJSON[1]['symbol']])
                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())