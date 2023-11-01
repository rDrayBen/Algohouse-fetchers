import json
import websockets
import time
import asyncio
import requests
API_URL = 'https://one.inx.co'
API_SYMBOLS = '/exchange/market/getAllMarkets'
WSS_URL = 'wss://one.inx.co/socket.io/?EIO=4&transport=websocket'
PING_TIMEOUT = 24
SUB_TIMEOUT = 0.001
DEPTH25 = 25
async def meta(response):
    if response.status_code == 200:
        for i in response.json():
            if i['isActive']:
                print("@MD", i['marketName'], 'spot', i['asset1'], i['asset2'],
                        i['maxPriceDecimals'], 1, 1, 0, 0, end='\n')
        print('@MDEND')
async def subscribe(ws, data):
    await ws.send(message="40")
    for i in data.json():
        await ws.send(message='42["/tradeHistory/subscribeTradeHistory",{"marketName":' + f'"{i["marketName"]}"' + '}]')
        await asyncio.sleep(SUB_TIMEOUT)
        await ws.send(message='42["/orderBook/subscribeOrderBook", {"marketName":'
                              + f'"{i["marketName"]}"' + ', "precisionOffset": 0, "depth":' + f'{DEPTH25}' + '}]')
        await asyncio.sleep(SUB_TIMEOUT)
async def heartbeat(ws):
    while True:
        await ws.send(message="3")
        await asyncio.sleep(PING_TIMEOUT)
def print_trade(data):
    print('!', round(time.time()*1000), data[1]['marketName'], data[1]['data'][0]['side'][0], data[1]['data'][0]['price'],
          data[1]['data'][0]['size'], end='\n')
def print_orderbook(data, isSnapshot):
    if isSnapshot:
        if 'BUY' in data[1]:
            if data[1]['BUY'] != [] and len(data[1]['BUY']) > 3:
                for i in data[1]['BUY']:
                    if i['amount'] == 'None' or i['amount'] is None:
                        i['amount'] = 0
                print('$', round(time.time()*1000), data[1]['marketName'], 'B',
                        '|'.join(str(i['amount']) + '@' + str(i['price']) for i in data[1]['BUY']), 'R', end='\n')

        if 'SELL' in data[1]:
            if data[1]['SELL'] != [] and len(data[1]['SELL']) > 3:
                for i in data[1]['SELL']:
                    if i['amount'] == 'None' or i['amount'] is None:
                        i['amount'] = 0
                print('$', round(time.time() * 1000), data[1]['marketName'], 'S',
                        '|'.join(str(i['amount']) + '@' + str(i['price']) for i in data[1]['SELL']), 'R', end='\n')
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        meta_task = asyncio.create_task(meta(response))
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, response))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    try:
                        data = await ws.recv()
                        if data[0] == '2':
                            await ws.send(message="3")
                            continue
                        if data[0] == '4' and data[1] == '2':
                            data_sliced = data[2:]
                            dataJSON = json.loads(data_sliced)
                            if dataJSON[0] =='TRADE_HISTORY':
                                if dataJSON[1]['data'] != [] or len(dataJSON[1]['data']) > 1:
                                    continue
                                else:
                                    print_trade(dataJSON)
                            if dataJSON[0] == 'ORDER_BOOK':
                                print_orderbook(dataJSON, 1)
                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())