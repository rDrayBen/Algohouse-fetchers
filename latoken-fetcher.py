import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://api.latoken.com/v2'
API_SPOT_SYMBOLS_URL = '/pair'
API_SPOT_SYMBOL_INFO_URL = '/currency'
API_SPOT_SNAPSHOTS_URL = '/book/'
WS_URL = 'wss://api.latoken.com/stomp/'
WS_PUBLIC_SPOT_TRADE = '/v1/trade/'
WS_PUBLIC_SPOT_ORDERBOOK = '/v1/book/'
TIMEOUT = 0.1
PING_TIMEOUT = 1
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
response_info = requests.get(API_URL + API_SPOT_SYMBOL_INFO_URL)
symbols = {x['id']: x['tag'] for x in response_info.json()}
symbols_parts = []
pairs_id = [[x['baseCurrency'], x['quoteCurrency']] for x in response.json()]
old_trades = {}
for i in pairs_id:
        old_trades[i[0] + '-' + i[1]] = 0

def format_decimal(value):
    try:
        if 'e' in str(value) or 'E' in str(value):
            parts = str(value).lower().split('e')
            precision = len(parts[0]) + abs(int(parts[1])) - 2
            return format(value, f'.{precision}f')
        else:
            return value
    except:
        return value


def divide_on_parts(symbols, part_size=100):
    parts_amount = round(len(symbols)/part_size)
    last = 0
    for i in range(parts_amount):
        symbols_parts.append(symbols[i*part_size:part_size*(i+1)])
        last = part_size*(i+1)
    if len(symbols) - last > 0:
        symbols_parts.append(symbols[last: len(symbols)])


async def meta(response):
    for i in response.json():
        print("@MD", symbols[i['baseCurrency']].upper() + '-' + symbols[i['quoteCurrency']].upper(),
              "spot", symbols[i['baseCurrency']].upper(), symbols[i['quoteCurrency']].upper(),
              i['priceDecimals'], 1, 1, 0, 0, end="\n")
    print("@MDEND")


async def get_snapshots(symbols_):
    for i in symbols_:
        response = requests.get(f"{API_URL}{API_SPOT_SNAPSHOTS_URL}{i[0]}/{i[1]}")
        if response.json()['ask'] != []:
            print('$', round(time.time() * 1000), symbols[i[0]].upper() + '-' + symbols[i[1]].upper(), 
                  'S', '|'.join(j['quantity'] + '@' + j['cost'] for j in response.json()['ask']), 'R')
        if response.json()['bid'] != []:
            print('$', round(time.time() * 1000), symbols[i[0]].upper() + '-' + symbols[i[1]].upper(), 
                  'B', '|'.join(j['quantity'] + '@' + j['cost'] for j in response.json()['bid']), 'R')
        await asyncio.sleep(TIMEOUT)
    

async def heartbeat(ws):
    while True:
        await ws.send(message=json.dumps(["\n"]))
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data):
    data_splitted = data.split('\\n')
    pair = data_splitted[1].split('/')[-2:]
    symbol = symbols[pair[0]].upper() + '-' + symbols[pair[1]].upper()
    data = json.loads(data_splitted[-1].split('\\u')[0].replace('\\', ''))
    try:
        for i in data["payload"]:
            if i["makerBuyer"]:
                bs = "B"
            else:
                bs = "S"
            print('!', round(time.time() * 1000), symbol, bs, i["cost"], i["quantity"])
    except:
        pass


def print_orderbooks(data, is_snapshot):
    data_splitted = data.split('\\n')
    pair = data_splitted[1].split('/')[-2:]
    symbol = symbols[pair[0]].upper() + '-' + symbols[pair[1]].upper()
    data = json.loads(data_splitted[-1].split('\\u')[0].replace('\\', ''))
    try:
        if is_snapshot:
            if data["payload"]["ask"] != []:
                print('$', round(time.time() * 1000), symbol, 'S', 
                      '|'.join(i['quantity'] + '@' + i['cost'] for i in data["payload"]["ask"]), 'R')
            if data["payload"]["bid"] != []:
                print('$', round(time.time() * 1000), symbol, 'B', 
                      '|'.join(i['quantity'] + '@' + i['cost'] for i in data["payload"]["bid"]), 'R')
        else:
            if data["payload"]["ask"] != []:
                print('$', round(time.time() * 1000), symbol, 'S', 
                      '|'.join(i['quantity'] + '@' + i['cost'] for i in data["payload"]["ask"]))
            if data["payload"]["bid"] != []:
                print('$', round(time.time() * 1000), symbol, 'B', 
                      '|'.join(i['quantity'] + '@' + i['cost'] for i in data["payload"]["bid"]))
    except:
        pass


async def subscribe(ws, symbols_):
    await ws.send(message=json.dumps(["CONNECT\naccept-version:1.0,1.1,1.2\nheart-beat:10000,10000\n\n\u0000"]))
    await asyncio.sleep(TIMEOUT)
    k = 0
    for i in range(len(symbols_)):
        await ws.send(message=json.dumps([f"SUBSCRIBE\nid:sub-{k}\ndestination:{WS_PUBLIC_SPOT_TRADE}{symbols_[i][0]}/{symbols_[i][1]}\n\n\u0000"]))
        await asyncio.sleep(TIMEOUT)
        await ws.send(message=json.dumps([f"SUBSCRIBE\nid:sub-{k+1}\ndestination:{WS_PUBLIC_SPOT_ORDERBOOK}{symbols_[i][0]}/{symbols_[i][1]}\n\n\u0000"]))
        await asyncio.sleep(TIMEOUT)
        k += 2


async def handle_socket(symbol):
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            ping_task = asyncio.create_task(heartbeat(ws))
            while True:
                try:
                    data = await ws.recv()
                    if data[0] == 'a':
                        data = data[1:]
                        if WS_PUBLIC_SPOT_TRADE in data:
                            data_splitted = data.split('\\n')
                            pair = data_splitted[1].split('/')[-2:]
                            if old_trades[pair[0] + '-' + pair[1]] < 2:
                                old_trades[pair[0] + '-' + pair[1]] += 1
                            else:
                                print_trades(data)
                        if WS_PUBLIC_SPOT_ORDERBOOK in data:
                            print_orderbooks(data, 0)
                except:
                    continue
        except KeyboardInterrupt:
            print("Keyboard Interupt")
            exit(1)
        except Exception as conn_c:
            print(f"WARNING: connection exception {conn_c} occurred")
            continue


async def handler():
    meta_task = asyncio.create_task(meta(response))
    snapshot_task = asyncio.create_task(get_snapshots(pairs_id))
    divide_on_parts(pairs_id, int(len(pairs_id) // 100))
    #divide_on_parts(pairs_id[0], 1)
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in symbols_parts])


async def main():
    await handler()

asyncio.run(main())