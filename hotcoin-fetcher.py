import json
import websockets
import time
import asyncio
import requests
import gzip
import pandas as pd
import datetime

API_URL = "https://api.hotcoinfin.com"
API_SYMBOLS = "/v1/common/symbols"
WSS_URL = "wss://wss.hotcoinfin.com/trade/multiple"
TIMEOUT = 0.01
PING_TIMEOUT = 9

async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps({"sub":f"market.{i}.trade.depth"}))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({"sub":f"market.{i}.trade.bbo"}))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({"sub":f"market.{i}.trade.detail"}))
        await asyncio.sleep(TIMEOUT)

async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({"ping": "ping"}))
        await asyncio.sleep(PING_TIMEOUT)

async def meta(data):
    for i in data:
        base = i['symbol'].find("_")
        print("@MD", i['symbol'].upper(), "spot", i['symbol'][0:base].upper(),
              i['symbol'][base+1:len(i['symbol'])].upper(), i['pricePrecision'],
              1, 1, 0, 0,end="\n")
    print("@MDEND")

def print_trades(data):
    symbol = data['ch'].replace('market.', '')
    symbol2 = symbol.replace('.trade.detail', '')
    for i in data['data']:
        print("!", round(time.time() * 1000), symbol2.upper(), i['direction'][0].upper(), i['price'], i['amount'],
              end='\n')

def print_orderbooks(data, is_snapshot):
    if is_snapshot:
        symbol = data['ch'].replace('market.', '')
        symbol2 = symbol.replace('.trade.depth', '')
        if data['data']['asks'] != []:
            print("$", round(time.time() * 1000), symbol2.upper(), "S",
                  "|".join(i[1]+"@"+i[0] for i in data['data']['asks']), "R", end="\n")
        if data['data']['bids'] != []:
            print("$", round(time.time() * 1000), symbol2.upper(), "B",
                  "|".join(i[1] + "@" + i[0] for i in data['data']['bids']), "R", end="\n")
    else:
        symbol = data['ch'].replace('market.', '')
        symbol2 = symbol.replace('.trade.bbo', '')
        if data['data']['ask'] != '':
            print("$", round(time.time() * 1000), symbol2.upper(), "S",
                  f"{data['data']['askSize']}@{data['data']['ask']}", end="\n")
        if data['data']['bid'] != '':
            print("$", round(time.time() * 1000), symbol2.upper(), "B",
                  f"{data['data']['bidSize']}@{data['data']['bid']}", end="\n")

async def statistics(count_connection_error, count_trades, count_deltas, count_snapshots):
    df = pd.DataFrame([[ datetime.datetime.now(), count_connection_error, count_trades, count_deltas, count_snapshots]],
                          columns=['Time', 'Connection Errors', 'Trades', 'Deltas', 'Snapshots'])
    df.to_csv("stats_hotcoin.txt", mode='a', sep="\t")

async def main():
    try:
        count_connection_error = 0
        count_trades = 0
        count_deltas = 0
        count_snapshots = 0
        response = requests.get(API_URL + API_SYMBOLS)
        symbols = [i['symbol'] for i in response.json()['data'] if i['state'] == "enable"]
        depth_channels = [f"market.{i}.trade.depth" for i in symbols]
        trade_channels = [f"market.{i}.trade.detail" for i in symbols]
        delta_channels = [f"market.{i}.trade.bbo" for i in symbols]
        meta_task = asyncio.create_task(meta(response.json()['data']))
        async for ws in websockets.connect(WSS_URL, ping_interval=None):
            try:
                sub_task = asyncio.create_task(subscribe(ws, symbols))
                ping_task = asyncio.create_task(heartbeat(ws))
                while True:
                    stats_task = asyncio.create_task(statistics(count_connection_error, count_trades, count_deltas,
                                                                count_snapshots))
                    data = await ws.recv()
                    dataGZIP = gzip.decompress(data)
                    try:
                        dataJSON = json.loads(dataGZIP)
                        if dataJSON['ch'] in depth_channels:
                            print_orderbooks(dataJSON, 1)
                            count_snapshots += 1
                        if dataJSON['ch'] in trade_channels:
                            print_trades(dataJSON)
                            count_trades += 1
                        if dataJSON['ch'] in delta_channels:
                            print_orderbooks(dataJSON, 0)
                            count_deltas += 1
                    except:
                        continue
            except:
                count_connection_error += 1
    except requests.exceptions.ConnectionError as conn_c:
        print(f"WARNING: connection exception {conn_c} occurred")
asyncio.run(main())