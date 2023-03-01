import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.bitkub.com"
META_API = "https://tradingview.bitkub.com/tradingview"
DECIMALS_API = "/symbols?symbol="
API_SYMBOLS = "/api/market/symbols"
WS_URL = "wss://api.bitkub.com/websocket-api/"
WS_ORDERBOOKS = "wss://api.bitkub.com/websocket-api/orderbook/"

def create_trades_chanels(symbol:str):
    return WS_URL + "market.trade." + symbol.lower()

def create_orderbook_chanels(symbol_id:int):
    return WS_ORDERBOOKS + str(symbol_id)

def print_trade(data):
    side = "S"
    if data['txn'].find("SELL") == -1:
        side = "B"
    print("!", round(time.time()*1000), data['sym'], side, data['rat'], "{:f}".format(data['amt']).rstrip('0').rstrip('.'), end="\n")

def print_orderbook(data, symbol_):
    bids_update_data = []
    asks_update_data = []
    for i in data['data'][1]:
        if i[5] == True:
            bids_update_data.append(i)
    for j in data['data'][2]:
        if j[5] == True:
            asks_update_data.append(j)

    if bids_update_data != [] or asks_update_data != []:
        if bids_update_data != []:
            print("$", round(time.time()*1000), symbol_, "B",
                  "|".join("{:f}".format(i[2]) + "@" + "{:f}".format(i[1]).rstrip('0').rstrip('.') for i in bids_update_data),
                  end="\n")
        if asks_update_data != []:
            print("$", round(time.time() * 1000), symbol_, "S",
                  "|".join("{:f}".format(i[2]) + "@" + "{:f}".format(i[1]).rstrip('0').rstrip('.') for i in asks_update_data),
                  end="\n")
    else:
        if data != []:
            print("$", round(time.time() * 1000), symbol_, "B",
                  "|".join("{:f}".format(i[2]) + "@" + "{:f}".format(i[1]).rstrip('0').rstrip('.') for i in
                           data['data'][1]), "R",
                  end="\n")

            print("$", round(time.time() * 1000), symbol_, "S",
                  "|".join("{:f}".format(i[2]) + "@" + "{:f}".format(i[1]).rstrip('0').rstrip('.') for i in
                           data['data'][2]),"R",
                  end="\n")

def print_meta(data):
    quote_symbol = data['symbol'][4:len(data['symbol'])]
    precission = str(requests.get(META_API + DECIMALS_API + quote_symbol + "_THB").json()['pricescale'])
    decimals_ = precission.count('0')
    # add decimals
    print("@MD", data['symbol'].replace('_', '/'), "spot", "THB", quote_symbol, decimals_,
          1, 1, 0, 0, end="\n")

async def get_metadata(response):
    for i in response.json()['result']:
        print_meta(i)
    print("@MDEND")

response = requests.get(API_URL + API_SYMBOLS)
symbols = [x['symbol'] for x in response.json()['result']]
symbols_id = [x['id'] for x in response.json()['result']]
trade_messages = [create_trades_chanels(x) for x in symbols]
orderbook_messages = [create_orderbook_chanels(x) for x in symbols_id]
symbols_ = {x['id']: x['symbol'] for x in response.json()['result']}

async def handle_socket(uri, ):
    async with websockets.connect(uri) as websocket:
        async for message in websocket:
            try:
                data = json.loads(message)
                if 'txn' in data:
                    print_trade(data)
                if data['event'] == "tradeschanged":
                    print_orderbook(data,  symbols_[int(uri.replace(WS_ORDERBOOKS, ""))])
            except:
                pass
        await websocket.keepalive_ping()

async def handler():
    meta_task = asyncio.create_task(get_metadata(response))
    await asyncio.wait([handle_socket(uri) for uri in trade_messages+orderbook_messages])

def main():
    asyncio.get_event_loop().run_until_complete(handler())
main()