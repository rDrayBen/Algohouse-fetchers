import json
import requests
import websockets
import time
import asyncio
import gzip
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

# get all cryptocoin symbols
currency_url = 'https://api.huobi.pro/v1/settings/common/market-symbols'
curr_response = requests.get(currency_url)
resp = curr_response.json()
size_symbols = dict()
symbol_dict = dict()
trades_count_5min = {}
orders_count_5min = {}
for i in range(len(resp['data'])):
    if resp['data'][i]['state'] == 'online':
        size_symbols[resp['data'][i]['symbol']] = (resp['data'][i]['bc']).upper() + \
                                                  '-' + (resp['data'][i]['qc']).upper()
        symbol_dict[resp['data'][i]['bc']] = resp['data'][i]['qc']
# base web socket url
WS_URL = "wss://api.huobi.pro/ws"


async def metadata():
    for pair in resp['data']:
        if pair['state'] == 'online':
            trades_count_5min[pair['bc'].upper() + '-' + pair['qc'].upper()] = 0
            orders_count_5min[pair['bc'].upper() + '-' + pair['qc'].upper()] = 0
            pair_data = '@MD ' + pair['bc'].upper() + '-' + pair['qc'].upper() + ' spot ' + \
                        pair['bc'].upper() + ' ' + pair['qc'].upper() + ' ' + str(pair['vp']) + ' 1 1 0 0'
            print(pair_data, flush=True)
    print('@MDEND')


# function to format the trades output
def get_trades(message):
    trade_data = message
    coin_name = trade_data['ch'].replace('market.', '').replace('.trade.detail', '')
    trades_count_5min[size_symbols[coin_name]] += len(trade_data['tick']['data'])
    for elem in trade_data['tick']['data']:
        print('!', get_unix_time(),
              size_symbols[coin_name],
              str(elem['direction'])[:1].upper(), str('{0:.9f}'.format(elem['price'])),
              str('{0:.4f}'.format(elem['amount'])), flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    order_data = message
    # check if bids array is not Null
    coin_name = order_data['ch'].replace('market.', '').replace('.depth.step0', '')
    orders_count_5min[size_symbols[coin_name]] += len(order_data['tick']['bids']) + len(order_data['tick']['asks'])
    answer = ''
    if len(order_data['tick']['bids']) != 0:
        answer += '$ ' + str(get_unix_time()) + ' '  + size_symbols[coin_name] + ' B '
        pq = '|'.join(f"{str('{0:.10f}'.format(elem[1]))}@{str('{0:.8f}'.format(elem[0]))}"
                      for elem in order_data['tick']['bids'])
        if update:
            print(answer + pq, flush=True)
        else:
            print(answer + pq + ' R', flush=True)
    if len(order_data['tick']['asks']) != 0:
        answer = ''
        answer += '$ ' + str(get_unix_time()) + ' ' + size_symbols[coin_name] + ' S '
        pq = '|'.join(f"{str('{0:.10f}'.format(elem[1]))}@{str('{0:.8f}'.format(elem[0]))}"
                      for elem in order_data['tick']['asks'])
        if update:
            print(answer + pq, flush=True)
        else:
            print(answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
            "pong": get_unix_time()
        }))
        await asyncio.sleep(5)


async def subscribe(ws):
    # create trades for each cryptocoin symbol
    for key, value in symbol_dict.items():
        await ws.send(json.dumps({
            "sub": f"market.{key + value}.trade.detail",
            'id': '428550639'
        }))
        if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
            await ws.send(json.dumps({
                "sub": f"market.{key + value}.depth.step0",
                'id': '428550639'
            }))
            await ws.send(json.dumps({
                "sub": f"market.{key + value}.mbp.150",
                'id': '428550639'
            }))


# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
    time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
    if time_to_wait != 300:
        await asyncio.sleep(time_to_wait)
    while True:
        stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
        time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
        await asyncio.sleep(time_to_wait)


async def main():
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL, ping_interval=None):
        try:
            sub_task = asyncio.create_task(subscribe(ws))
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))
            # print metadata about each pair symbols
            meta_data = asyncio.create_task(metadata())
            # print stats for trades and orders
            statistics = asyncio.create_task(print_stats(trades_count_5min, orders_count_5min))
            while True:
                # receiving data from server
                data = await ws.recv()
                # change format of received data to json format
                dataJSON = json.loads(gzip.decompress(data))
                try:
                    if 'ch' in dataJSON:
                        # check if received data is about trades
                        if 'trade' in dataJSON['ch']:
                            get_trades(dataJSON)
                        # check if received data is about updates on order book
                        elif 'mbp' in dataJSON['ch']:
                            get_order_books_and_deltas(dataJSON, update=True)
                        # check if received data is about order books
                        elif 'depth' in dataJSON['ch']:
                            get_order_books_and_deltas(dataJSON, update=False)
                        else:
                            print(dataJSON)
                    elif 'ping' in dataJSON:
                        await ws.send(json.dumps({
                            "pong": dataJSON['ping']
                        }))
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred", data)
                    time.sleep(1)
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")
            time.sleep(1)


# run main function
asyncio.run(main())
