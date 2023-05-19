import json
import websockets
import time
import asyncio
import requests

API_URL = "https://www.deribit.com/api/v2"
API_CURRENCIES = "/public/get_currencies"
API_SYMBOLS = "/public/get_instruments?currency="
WS_URL = "wss://www.deribit.com/ws/api/v2"
TIMEOUT = 0.12
PING_TIMEOUT = 5

async def subscribe(ws, data):
    global j
    j = 0
    for i in data:
        j += 1
        await ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": j,
            "method": "public/subscribe",
            "params": {
                "channels": [i]
            }
        }))
        await asyncio.sleep(TIMEOUT)

async def heartbeat(ws):
    k = 9000
    await ws.send(json.dumps({
        "jsonrpc": "2.0",
        "id": k,
        "method": "public/set_heartbeat",
        "params": {"interval": 10}
    }))
    while True:
        k+=1
        await ws.send(json.dumps({"jsonrpc": "2.0",
                                  "id": k, "method": "public/test",
                                  "params": {}}))
        await asyncio.sleep(PING_TIMEOUT)

async def meta(data):
    for i in data:
        response = requests.get(API_URL + API_SYMBOLS + i)
        for j in response.json()['result']:
            if(j['kind'] == "spot"):
                precission = str(j["min_trade_amount"])[::-1].find(".")
                if precission == -1:
                    print("@MD", j['instrument_name'], "spot", j['base_currency'], j['counter_currency'], 0, 1, 1, 0, 0, end="\n")
                else:
                    print("@MD", j['instrument_name'], "spot", j['base_currency'], j['counter_currency'], precission, 1, 1, 0, 0,
                          end="\n")
    print("@MDEND")

def print_trades(data):
    for i in data['params']['data']:
        print("!", round(time.time() * 1000), i['instrument_name'], i['direction'][0].upper(),
              str(i['price']), str(i['amount']), end="\n")

def print_orderbook(data, isSnapshot):
    if isSnapshot:
        if data['params']['data']['bids'] != []:
            print("$", round(time.time() * 1000), data['params']['data']['instrument_name'], "B",
                  "|".join(str(i[2]) + "@" + str(i[1]) for i in data['params']['data']['bids']), "R", end='\n')

        if data['params']['data']['asks'] != []:
            print("$", round(time.time() * 1000), data['params']['data']['instrument_name'], "S",
                  "|".join(str(i[2]) + "@" + str(i[1]) for i in data['params']['data']['asks']), "R", end='\n')
    else:
        if data['params']['data']['bids'] != []:
            print("$", round(time.time() * 1000), data['params']['data']['instrument_name'], "B",
                  "|".join(str(i[2]) + "@" + str(i[1]) for i in data['params']['data']['bids']), end='\n')

        if data['params']['data']['asks'] != []:
            print("$", round(time.time() * 1000), data['params']['data']['instrument_name'], "S",
                  "|".join(str(i[2]) + "@" + str(i[1]) for i in data['params']['data']['asks']), end='\n')

async def main():
    try:
        response_currencies = requests.get(API_URL + API_CURRENCIES)
        currencies = [i["currency"] for i in response_currencies.json()["result"]]

        symbols = []
        instrument_name = []
        resp_instrument = []
        for i in currencies:
            response_instruments = requests.get(API_URL + API_SYMBOLS + i + "&kind=spot")
            symbol_ = [j['price_index'] for j in response_instruments.json()["result"] if j['is_active'] == True]
            instrument_name += [k['instrument_name'] for k in response_instruments.json()['result'] if k['is_active'] == True]
            symbols += symbol_

        trade_channel = ['trades.' + k + '.100ms' for k in instrument_name]
        orderbook_channel = ['book.' + k + '.100ms' for k in instrument_name]
        meta_task = asyncio.create_task(meta(currencies))
        async for ws in websockets.connect(WS_URL):
            try:
                trade_task = asyncio.create_task(subscribe(ws, trade_channel))
                orderbook_task = asyncio.create_task(subscribe(ws, orderbook_channel))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    data = await ws.recv()
                    dataJSON = json.loads(data)
                    try:
                        if dataJSON['method'] == "subscription":
                            if dataJSON['params']['channel'] in trade_channel:
                                print_trades(dataJSON)
                            if dataJSON['params']['channel'] in orderbook_channel:
                                if dataJSON['params']['data']['type'] == "snapshot":
                                    print_orderbook(dataJSON, 1)
                                if dataJSON['params']['data']['type'] == "change":
                                    print_orderbook(dataJSON, 0)
                    except:
                        pass
            except:
                pass
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())