import json
import websockets
import time
import asyncio
import requests
API_URL = "https://plasma-relay-backend.timex.io"
API_SYMBOLS = "/public/markets"
API_CURRENCIES = '/public/currencies'
WSS_URL = "wss://plasma-relay-backend.timex.io/socket/relay"
TIMEOUT = 0.1
async def meta(response):
    currencies_response = requests.get(API_URL + API_CURRENCIES)
    curr_precc = {}
    for i in currencies_response.json():
        if i['active']:
            curr_precc[i['symbol']] = i['decimals']
    for i in response.json():
        if i['locked'] and i['promotional']:
            continue
        else:
            print("@MD", i['symbol'], "spot", i['baseCurrency'], i['quoteCurrency'], curr_precc[i['quoteCurrency']],
                  1, 1, 0, 0, end="\n")
    print("@MDEND")
async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps({
            "type": "SUBSCRIBE",
            "requestId": f"{i['symbol']}_TRADE",
            "pattern": f"^/trade/{i['baseTokenAddress']}/{i['quoteTokenAddress']}"
        }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({
            "type": "SUBSCRIBE",
            "requestId": f"{i['symbol']}_ORDERBOOK",
            "pattern": f"/orderbook.group/{i['symbol']}"
        }))
        await asyncio.sleep(TIMEOUT)
def print_orderbook(data, isSnapshot):
    if isSnapshot:
        if data['bid'] != []:
            print("$", round(time.time() * 1000), data['symbol'], 'B',
                  "|".join(i['volume'] + "@" + i['price'] for i in data['bid']),
                  "R", end="\n")

        if data['ask'] != []:
            print("$", round(time.time() * 1000), data['symbol'], 'S',
                  "|".join(i['volume'] + "@" + i['price'] for i in data['ask']),
                  "R", end="\n")
    else:
        if data['data']['orderbook']['bid'] != []:
            print("$", round(time.time() * 1000), data['data']['market'], 'B',
                  "|".join(i['volume'] + "@" + i['price'] for i in data['data']['orderbook']['bid']),
                  end='\n')
        if data['data']['orderbook']['ask'] != []:
            print("$", round(time.time() * 1000), data['data']['market'], 'S',
                  "|".join(i['volume'] + "@" + i['price'] for i in data['data']['orderbook']['ask']),
                  end='\n')
def print_trade(data):
    print("!", round(time.time()*1000), data['event']['topics'][0] + data['event']['topics'][1],
          data['payload']['direction'][0], data['payload']['price'], data['payload']['quantity'], end='\n')
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        meta_task = asyncio.create_task(meta(response))
        snapshots_dict = {}
        symbol_snapshot_dict={}
        for i in response.json():
            snapshots_dict[i['symbol']] = 0
            symbol_snapshot_dict[i['symbol']] = {"symbol":i['symbol'], "bid":[], "ask":[]}
        async for ws in websockets.connect(WSS_URL, ping_interval=4):
            try:
                sub_task = asyncio.create_task(subscribe(ws, response.json()))
                while True:
                    try:
                        data = await ws.recv()
                        dataJSON = json.loads(data)
                        if dataJSON['message']['event']['type'] == 'GROUP_ORDER_BOOK_UPDATED':
                            if snapshots_dict[dataJSON['message']['event']['reference']] <= 2:
                                symbol_snapshot_dict[dataJSON['message']['event']['reference']]['bid']\
                                    += dataJSON['message']['event']['data']['orderbook']['bid']
                                symbol_snapshot_dict[dataJSON['message']['event']['reference']]['ask'] \
                                    += dataJSON['message']['event']['data']['orderbook']['ask']
                                snapshots_dict[dataJSON['message']['event']['reference']] += 1
                            else:
                                print_orderbook(symbol_snapshot_dict[dataJSON['message']['event']['reference']], 1)
                                symbol_snapshot_dict[dataJSON['message']['event']['reference']] = {
                                    "symbol": dataJSON['message']['event']['reference'],
                                    "bid": [],
                                    "ask": []
                                }
                                snapshots_dict[dataJSON['message']['event']['reference']] = 0
                                continue
                            print_orderbook(dataJSON['message']['event'], 0)
                        if dataJSON['message']['event']['type'] == 'TRADE':
                            print_trade(dataJSON['message'])
                    except:
                        continue
            except:
                print("Reconnect")
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())