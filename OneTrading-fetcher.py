import json
import websockets
import time
import asyncio
import requests
API_URL = 'https://api.onetrading.com/public/v1'
API_SYMBOLS = '/instruments'
WSS_URL = 'wss://streams.onetrading.com'
TIMEOUT = 0.1
PING_TIMEOUT = 9
async def meta(response):
    for i in response.json():
        if i['state'] == 'ACTIVE':
            print("@MD", i['base']['code'] + "_" + i['quote']['code'], 'spot',
                  i['base']['code'],
                  i['quote']['code'],
                  i['quote']['precision'], 1, 1, 0, 0, end='\n')
    print("@MDEND")
async def subscribe(ws, data):
    await ws.send(json.dumps(
        {
            "type": "SUBSCRIBE",
            "channels": [
                {
                    "name": "ORDER_BOOK",
                    "depth": 200,
                    "instrument_codes": data
                },
                {
                    "name": "PRICE_TICKS",
                    "instrument_codes": data
                }
            ]
        }
    ))
def print_trades(data):
    print("!", round(time.time() * 1000), data['instrument_code'], data['taker_side'][0],
          data['price'], data['amount'], end="\n")
def print_orderbooks(data, isSnapshot):
    if isSnapshot:
        if data['bids'] != []:
            print("$", round(time.time() * 1000), data['instrument_code'], "B",
                  "|".join(i[1] + "@" + i[0] for i in data['bids']), "R" , end='\n')
        if data['asks'] != []:
            print("$", round(time.time() * 1000), data['instrument_code'], "S",
                  "|".join(i[1] + "@" + i[0] for i in data['asks']), "R", end='\n')
    else:
        if data['changes'] != []:
            count_bids = 0
            count_asks = 0
            for i in data['changes']:
                if i[0][0] == "B":
                    count_bids += 1
                if i[0][0] == 'S':
                    count_asks += 1
            if count_asks != 0:
                print("$", round(time.time() * 1000), data['instrument_code'], "S",
                      "|".join(i[2] + "@" + i[1] for i in data['changes'] if i[0][0] == 'S'), end='\n')
            if count_bids != 0:
                print("$", round(time.time() * 1000), data['instrument_code'], "B",
                      "|".join(i[2] + "@" + i[1] for i in data['changes'] if i[0][0] == 'B'), end='\n')
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['base']['code']+"_"+i['quote']['code'] for i in response.json() if i['state'] == "ACTIVE"]
        meta_task = asyncio.create_task(meta(response))
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                while True:
                    try:
                        data = await ws.recv()
                        dataJSON = json.loads(data)
                        if dataJSON['type'] == 'PRICE_TICK':
                            print_trades(dataJSON)
                        if dataJSON['type'] == 'ORDER_BOOK_SNAPSHOT':
                            print_orderbooks(dataJSON, 1)
                        if dataJSON['type'] == 'ORDER_BOOK_UPDATE':
                            print_orderbooks(dataJSON, 0)
                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())