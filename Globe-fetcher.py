import json
import websockets
import time
import asyncio
import requests
API_URL = 'https://globe.exchange/api/v1/'
WSS_URL = 'wss://globe.exchange/api/v1/ws'
TIMEOUT = 0.1
async def meta(response):
    for i in response:
        print("@MD", i['subscription']['instrument'], 'spot', i['data']['base_symbol'],
              i['data']['quote_symbol'], i['data']['qty_precision'], 1, 1, 0, 0, end='\n')
    print("@MDEND")
async def subscribe(ws, data):
    for i in data:
        await ws.send(
            message=json.dumps(
                {
                    "command": "subscribe",
                    "channel": "depth",
                    "instrument": f"{i['subscription']['instrument']}"
                }
            )
        )
        await asyncio.sleep(TIMEOUT)
        await ws.send(
            message=json.dumps(
                {
                    "channel":"trades",
                    "instrument":f"{i['subscription']['instrument']}",
                    "command":"subscribe"
                }
            )
        )
        await asyncio.sleep(TIMEOUT)

def print_trade(data):
    print("!", round(time.time() * 1000), data['subscription']['instrument'], data['data'][0]['side'][0].upper(),
        str(data['data'][0]['price']), '{:.9f}'.format(data['data'][0]['quantity']), end='\n')

def print_orderbook(data):
    if data['data']['asks'] != []:
        print('$', round(time.time() * 1000), data['subscription']['instrument'],
              'S', '|'.join('{:.9f}'.format(i['volume']) + "@" + '{:.9f}'.format(i['price']) for i in data['data']['asks']),
              'R', end='\n')

    if data['data']['bids'] != []:
        print('$', round(time.time() * 1000), data['subscription']['instrument'],
              'B', '|'.join('{:.9f}'.format(i['volume']) + "@" + '{:.9f}'.format(i['price']) for i in data['data']['bids']),
              'R', end='\n')

async def main():
    try:
        response = []
        async with websockets.connect(WSS_URL) as ws:
            await ws.send(json.dumps({"command": "subscribe",
                                      "channel": "product-list"}))
            product_list = []
            while product_list == []:
                data = await ws.recv()
                dataJSON = json.loads(data)
                product_list = dataJSON['data']
            spot_instruments = 0
            for i in product_list:
                if i['category'] == 'Spot':
                    spot_instruments += 1
                    await ws.send(json.dumps({"command": "subscribe",
                                              "channel": "product-detail",
                                              "instrument": i['symbol']}))
                    await asyncio.sleep(TIMEOUT)
            while len(response) != spot_instruments:
                data = await ws.recv()
                dataJSON = json.loads(data)
                response.append(dataJSON)
            await ws.close()
        meta_task = asyncio.create_task(meta(response))
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, response))
                while True:
                    try:
                        data = await ws.recv()
                        dataJSON = json.loads(data)
                        if dataJSON['subscription']['channel'] == 'trades':
                            if len(dataJSON['data']) == 1:
                                 print_trade(dataJSON)
                        if dataJSON['subscription']['channel'] == 'depth':
                            print_orderbook(dataJSON)
                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())