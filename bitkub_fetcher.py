import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.bitkub.com"
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
    print("!", round(time.time()*1000), data['sym'].replace("_", "-"), side, data['rat'], data['amt'], end="\n")

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
            print("$", round(time.time()*1000), symbol_, "B","|".join(str(i[2]) + "@" + str(i[1]) for i in bids_update_data),
                  end="\n")
        if asks_update_data != []:
            print("$", round(time.time() * 1000), symbol_, "S","|".join(str(i[2]) + "@" + str(i[1]) for i in asks_update_data),
                  end="\n")
    else:
        print("$", round(time.time() * 1000), symbol_, "B",
              "|".join(str(i[2]) + "@" + str(i[1]) for i in data['data'][1]), "R",
              end="\n")

        print("$", round(time.time() * 1000), symbol_, "S",
                  "|".join(str(i[2]) + "@" + str(i[1]) for i in data['data'][2]), "R",
                  end="\n")


response = requests.get(API_URL + API_SYMBOLS)
symbols = [x['symbol'] for x in response.json()['result']]
symbols_id = [x['id'] for x in response.json()['result']]
trade_messages = [create_trades_chanels(x) for x in symbols]
orderbook_messages = [create_orderbook_chanels(x) for x in symbols_id]
symbols_ = {x['id']: x['symbol'].replace("_", "-") for x in response.json()['result']}


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
    await asyncio.wait([handle_socket(uri) for uri in trade_messages+orderbook_messages])

def main():
    asyncio.get_event_loop().run_until_complete(handler())
main()