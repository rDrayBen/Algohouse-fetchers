import json
import websockets
import time
import asyncio
import requests
API_URL = "https://api.coinzix.com/api"
API2_URL = 'https://api.coinzix.com/v1/public'
API_SYMBOLS = "/default/ticker"
API_SNAPSHOT = '/book?pair='
WSS_URL = "wss://socket.coinzix.com/socket.io/?EIO=3&transport=websocket"
SUB_TIMEOUT = 0.001
REST_TIMEOUT = 0.05
PING_TIMEOUT = 24
SNAPSHOT_TIMEOUT = 1
async def meta(response):
    for i in response.json()['data']:
        print("@MD", i['name'], 'spot', i['main']['iso3'], i['second']['iso3'], i['second']['decimal'],
              1, 1, 0, 0, end='\n')
    print('@MDEND')
async def subscribe(ws, data):
    for i in data['data']:
        await ws.send(message='42["subscribe",' + f'"short_book_{i["_id"]}"]')
        await asyncio.sleep(SUB_TIMEOUT)
        await ws.send(message='42["subscribe",' + f'"hist_{i["_id"]}"]')
        await asyncio.sleep(SUB_TIMEOUT)
async def heartbeat(ws):
    while True:
        await ws.send(message="2")
        await asyncio.sleep(PING_TIMEOUT)
def print_trade(data, pair, precision):
    str_price = str(data['data']['rate'])
    volume = str(data['data']['volume'])
    if precision > len(str_price):
        add = precision - len(str_price)
        for i in range(add):
            str_price = '0' + str_price
        str_price = '0.' + str_price
    else:
        str_price = str_price[:(len(str_price)-precision)] + '.' + str_price[(len(str_price)-precision):]
        if str_price[0] == '.':
            str_price = "0" + str_price
    if precision >= len(volume):
        add = precision-len(volume)
        for i in range(add):
            volume = '0'+volume
        volume = '0.' + volume
    else:
        volume = volume[:(len(volume)-precision)] + '.' + volume[(len(volume)-precision):]
    if data['data']['type']:
        print("!", round(time.time() * 1000), pair, 'S', str_price, volume, end='\n')
    else:
        print("!", round(time.time() * 1000), pair, 'B', str_price, volume, end='\n')
def print_orderbook(data, pair, precision, isSnapshot):
    if isSnapshot:
        pass
    else:
        if 'sell' in data['data']:
            _, v = data['data']['sell'].popitem()
            if 'volume' in v:
                str_price = str(v['rate'])
                volume = str(v['volume'])
                if precision > len(str_price):
                    add = precision - len(str_price)
                    for i in range(add):
                        str_price = '0' + str_price
                    str_price = '0.' + str_price
                else:
                    str_price = str_price[:(len(str_price) - precision)] + '.' + str_price[
                                                                                 (len(str_price) - precision):]
                    if str_price[0] == '.':
                        str_price = "0" + str_price
                if precision >= len(volume):
                    add = precision - len(volume)
                    for i in range(add):
                        volume = '0' + volume
                    volume = '0.' + volume
                else:
                    volume = volume[:(len(volume) - precision)] + '.' + volume[(len(volume) - precision):]
                print('$', round(time.time() * 1000), pair, 'S',
                      volume + "@" + str_price, end='\n')
            else:
                str_price = str(v['rate'])
                if precision > len(str_price):
                    add = precision - len(str_price)
                    for i in range(add):
                        str_price = '0' + str_price
                    str_price = '0.' + str_price
                else:
                    str_price = str_price[:(len(str_price) - precision)] + '.' + str_price[
                                                                                 (len(str_price) - precision):]
                    if str_price[0] == '.':
                        str_price = "0" + str_price
                print('$', round(time.time() * 1000), pair, 'S',
                      '0' + "@" + str_price, end='\n')

        if 'buy' in data['data']:
            _, v = data['data']['buy'].popitem()
            if 'volume' in v:
                str_price = str(v['rate'])
                volume = str(v['volume'])
                if precision > len(str_price):
                    add = precision - len(str_price)
                    for i in range(add):
                        str_price = '0' + str_price
                    str_price = '0.' + str_price
                else:
                    str_price = str_price[:(len(str_price) - precision)] + '.' + str_price[
                                                                                 (len(str_price) - precision):]
                    if str_price[0] == '.':
                        str_price = "0" + str_price
                if precision >= len(volume):
                    add = precision - len(volume)
                    for i in range(add):
                        volume = '0' + volume
                    volume = '0.' + volume
                else:
                    volume = volume[:(len(volume) - precision)] + '.' + volume[(len(volume) - precision):]
                print('$', round(time.time() * 1000), pair, 'B',
                      volume + "@" + str_price, end='\n')
            else:
                str_price = str(v['rate'])
                if precision > len(str_price):
                    add = precision - len(str_price)
                    for i in range(add):
                        str_price = '0' + str_price
                    str_price = '0.' + str_price
                else:
                    str_price = str_price[:(len(str_price) - precision)] + '.' + str_price[
                                                                                 (len(str_price) - precision):]
                    if str_price[0] == '.':
                        str_price = "0" + str_price
                print('$', round(time.time() * 1000), pair, 'B',
                      '0' + "@" + str_price, end='\n')
async def print_snapshot_orderbook(symbols):
    while True:
        for i in symbols['data']:
            response = requests.get(API2_URL + API_SNAPSHOT + i['name'])
            if response.json()['data']['buy'] != []:
                print('$', round(time.time() * 1000), i['name'], 'B',
                      "|".join(str("{:.{}f}".format(j['volume'], i['second']['decimal']))
                               + "@" + str("{:.{}f}".format(j['rate'], i['main']['decimal']))
                               for j in response.json()['data']['buy']), 'R')
            if response.json()['data']['sell'] != []:
                print('$', round(time.time() * 1000), i['name'], 'S',
                      "|".join(str("{:.{}f}".format(j['volume'], i['second']['decimal']), )
                               + "@" + str("{:.{}f}".format(j['rate'], i['main']['decimal'])) for j in
                               response.json()['data']['sell']), 'R')

            await asyncio.sleep(REST_TIMEOUT)
        await asyncio.sleep(SNAPSHOT_TIMEOUT)
async def main():
    try:
        response = requests.get(API_URL + API_SYMBOLS)
        meta_task = asyncio.create_task(meta(response))
        dict_snapshot_book = {}
        pair_names = {}
        for i in response.json()['data']:
            dict_snapshot_book['short_book_' + i['_id']] = [10, 10, [], [], i['name'], i['main']['decimal']]
            pair_names[i['id']] = [i['name'], i['main']['decimal']]
        async for ws in websockets.connect(WSS_URL):
            try:
                sub_task = asyncio.create_task(subscribe(ws, response.json()))
                rest_snapshot_task = asyncio.create_task(print_snapshot_orderbook(response.json()))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    try:
                        data = await ws.recv()
                        if data[0] == '4' and data[1] == '2':
                            data_sliced = data[2:]
                            dataJSON = json.loads(data_sliced)
                            if dataJSON[1][0]['type'] == 'hist':
                                print_trade(dataJSON[1][0], pair_names[dataJSON[1][0]['data']['pair_id']][0],
                                            pair_names[dataJSON[1][0]['data']['pair_id']][1])
                            if dataJSON[1][0]['type'] == 'short_book':
                                print_orderbook(dataJSON[1][0], dict_snapshot_book[dataJSON[1][0]['room']][4],
                                                dict_snapshot_book[dataJSON[1][0]['room']][5], 0)
                    except:
                        continue
            except:
                continue
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())