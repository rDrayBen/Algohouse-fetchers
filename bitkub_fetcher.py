import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.bitkub.com"
API_SYMBOLS = "/api/market/symbols"
WS_URL = "wss://api.bitkub.com/websocket-api/"
WS_URL_ORDERBOOK = "wss://api.bitkub.com/websocket-api/orderbook/"
ORDERBOOK_WS_CONNECTIONS = []
response = requests.get(API_URL + API_SYMBOLS)
symbols = {x['id']: x['symbol'].replace("_", "-") for x in response.json()['result']}

def create_trade_message(symbol):
    trade_request = ",".join("market.trade." + x.lower() for x in symbol)
    return WS_URL + trade_request

def create_orderbook_message(symbol):
    return WS_URL_ORDERBOOK + str(symbol)

def print_trades(data):
    side = "B"
    if data["txn"].find("SELL") != -1:
        side = "S"
    print("!", round(time.time() * 1000), data['sym'].replace("_", "-"), side,
          data['rat'], data['amt'], end='\n')

def print_orderbooks(bids, asks, symbol):
    k = 0
    d = 0
    for i in bids:
        if i[4] == True:
           k += 1
    for i in asks:
        if i[4] == True:
           d += 1

    if k > 0 and d > 0:
        print("$", round(time.time() * 1000), symbol, "B",
              "|".join(str(x[2]) + "@" + str(x[1]) for x in bids if x[4] is True), end="\n")
        print("$", round(time.time() * 1000), symbol, "S",
              "|".join(str(x[2]) + "@" + str(x[1]) for x in asks if x[4] is True), end="\n")

    print("$", round(time.time() * 1000), symbol, "B",
          "|".join(str(x[2])+"@"+str(x[1]) for x in bids if x[4] is False),
          "R", end="\n")
    print("$", round(time.time() * 1000), symbol, "S",
          "|".join(str(x[2]) + "@" + str(x[1]) for x in asks if x[4] is False),
          "R", end="\n")

async def handle_socket(uri, ):
    async with websockets.connect(uri) as websocket:
        await websocket.ping(data=str(0))
        async for message in websocket:
            dicted_data = json.loads(message)
            try:
                if "txn" in dicted_data:
                    print_trades(dicted_data)

                if "event" in dicted_data:
                    if dicted_data["event"] == "tradeschanged":
                        print_orderbooks(dicted_data["data"][1], dicted_data["data"][2],
                                         symbols[int(uri.replace(WS_URL_ORDERBOOK, ""))])
            except:
                pass

async def handler():
    await asyncio.wait([handle_socket(uri) for uri in ORDERBOOK_WS_CONNECTIONS])

def main():
    symbol_id = [x['id'] for x in response.json()['result']]
    symbl = [x['symbol'] for x in response.json()['result']]
    ORDERBOOK_WS_CONNECTIONS.append(create_trade_message(symbl))
    orderbook_messages = [create_orderbook_message(i) for i in symbol_id]
    for i in orderbook_messages:
        ORDERBOOK_WS_CONNECTIONS.append(i)
    asyncio.get_event_loop().run_until_complete(handler())
main()