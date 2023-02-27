import json
import requests
import re
import asyncio
import time
import websockets
API_URL = 'https://api-cloud.bitmart.com'
API_SPOT_SYMBOLS_URL = '/spot/v1/symbols/details'
API_SPOT_SYMBOLS_BOOK_URL = '/spot/v1/symbols/book'
WS_URL= 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1'
WS_PUBLIC_SPOT_TRADE = 'spot/trade'
WS_PUBLIC_SPOT_DEPTH5 = 'spot/depth5'
WS_PUBLIC_SPOT_DEPTH50 = 'spot/depth50'
CONNECTIONS_MAX_SIZE = 10
SLEEP_TIME = 0.1
PING_RANGE = 970

def create_channel(channel, symbol):
    return f"{channel}:{symbol}"

def create_spot_subscribe_params(channels):
    return json.dumps({
        'op': 'subscribe',
        'args': channels
    })

def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def print_meta(data):
    print("@MD", data['symbol'], "spot", data['base_currency'], data['quote_currency'], data['price_max_precision'],
          1, 1,0,0, end="\n")

async def get_metadata(response):
    for i in response.json()['data']['symbols']:
        print_meta(i)
    print("@MDEND")

async def subscribe(ws, trade_messages, delta_messages, orderbook_messages):
    for i in range(len(trade_messages)):
        await ws.send(message=trade_messages[i])
        await ws.send(message=orderbook_messages[i])
        await ws.send(message=delta_messages[i])
        await asyncio.sleep(SLEEP_TIME)

def print_trades(data):
    try:
        if len(data['data']) >= 30:
            return
        for i in data['data']:
            if i['side'] == "buy":
                print("!", round(time.time() * 1000), i['symbol'].replace("_", "-"), 'B', i['price'],
                      i['size'], end='\n')
            else:
                print("!", round(time.time() * 1000), i['symbol'].replace("_", "-"), 'S', i['price'],
                      i['size'], end='\n')
    except:
        pass

def print_orderbook(data):
    try:
        asks = data['data'][0]['asks']
        bids = data['data'][0]['bids']
        if data['table'] == WS_PUBLIC_SPOT_DEPTH50:
            print("$", round(time.time() * 1000), data['data'][0]['symbol'].replace('_', '-'), 'B',
                      re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0]) + "|" for x in bids))
                      ,'R', end='\n')
            print("$", round(time.time() * 1000), data['data'][0]['symbol'].replace('_', '-'), 'S',
                      re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0]) + "|" for x in asks))
                      ,'R', end='\n')

        elif data['table'] == WS_PUBLIC_SPOT_DEPTH5:
            print("$", round(time.time() * 1000), data['data'][0]['symbol'].replace('_', '-'), 'B',
                  re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0]) + "|" for x in bids))
                  , end='\n')
            print("$", round(time.time() * 1000), data['data'][0]['symbol'].replace('_', '-'), 'S',
                  re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0]) + "|" for x in asks))
                  , end='\n')
    except:
        pass

def send_chuncks(symbols, url):
    messages = []
    chunks = list(divide_chunks(symbols, CONNECTIONS_MAX_SIZE))
    channels = []
    for j in chunks:
        for i in j:
            channels.append(create_channel(url, i))
        messages.append(create_spot_subscribe_params(channels))
        channels.clear()
    return messages

async def main():
    response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
    symbols = [x['symbol'] for x in response.json()['data']['symbols']]

    trade_messages = send_chuncks(symbols, WS_PUBLIC_SPOT_TRADE)
    delta_messages= send_chuncks(symbols, WS_PUBLIC_SPOT_DEPTH5)
    orderbook_messages = send_chuncks(symbols, WS_PUBLIC_SPOT_DEPTH50)
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, trade_messages, delta_messages, orderbook_messages))
            meta_task = asyncio.create_task(get_metadata(response))
            amount = 0
            while True:
                try:
                    data = await ws.recv()
                    amount += 1
                    if amount >= PING_RANGE:
                        await ws.send(json.dumps("ping"))
                        amount = 0
                    dicted_data = eval(data)
                    if dicted_data['table'] == WS_PUBLIC_SPOT_TRADE:
                        print_trades(dicted_data)
                    if dicted_data['table'] == WS_PUBLIC_SPOT_DEPTH5:
                        print_orderbook(dicted_data)
                    if dicted_data['table'] == WS_PUBLIC_SPOT_DEPTH50:
                        print_orderbook(dicted_data)
                except:
                    pass
        except Exception as conn_c:
            print(f"WARNING: connection exception {conn_c} occurred")
            continue
asyncio.run(main())