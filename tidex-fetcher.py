import json
import websockets
import time
import asyncio
import requests

API_URL = "https://api.tidex.com"
API_SYMBOLS = "/api/v1/public/markets"
WS_URL = "wss://ws.tidex.com/"

def create_trades_message(symbols, id):
    return json.dumps({
      "method":"deals.subscribe",
      "params":
        symbols,
      "id":id
    })

def create_orderbooks_message(symbol, limit, id):
    return json.dumps({
      "method":"depth.subscribe",
      "params":
        [
          symbol,
          limit,
          "0"
        ],
      "id":id
    })

def print_trade(data):
    for i in data[1]:
        if i['type'] == "sell":
            print("!", round(time.time() *1000), data[0], "S", i['price'], i['amount'], end="\n")
        else:
            print("!", round(time.time() * 1000), data[0], "B", i['price'], i['amount'], end="\n")

def print_orderbook(data):
    asks = data[1]['asks']
    bids = data[1]['bids']
    if data[0] == False:
        # delta
        if asks != []:
            print("$", round(time.time() * 1000), data[2], "S", "|".join(i[1] + "@" + i[0] for i in asks), end="\n")
        if bids != []:
            print("$", round(time.time() * 1000), data[2], "B", "|".join(i[1] + "@" + i[0] for i in bids), end="\n")
    elif data[0] == True:
        #snapshot
        if asks != []:
            print("$", round(time.time() * 1000), data[2], "S", "|".join(i[1] + "@" + i[0] for i in asks),"R", end="\n")
        if bids != []:
            print("$", round(time.time() * 1000), data[2], "B", "|".join(i[1] + "@" + i[0] for i in bids),"R", end="\n")

async def main():
    response = requests.get(API_URL+ API_SYMBOLS)
    symbols = [i['name'] for i in response.json()['result']]
    trade_message = create_trades_message(symbols, 0)
    orderbook_messages = [create_orderbooks_message(symbols[i], 30, i+1) for i in range(len(symbols))]
    async with websockets.connect(WS_URL) as ws:
        await ws.send(trade_message)
        for i in orderbook_messages:
            await ws.send(i)
            time.sleep(0.047)
        amount = 0
        while True:
            data = await ws.recv()
            amount += 1
            if amount >= 950:
                await ws.send(json.dumps({"method":"server.ping","params":[], "id":1000}))
                amount = 0
            try:
                dicted_data = json.loads(data)
                if dicted_data["method"] == "deals.update":
                    print_trade(dicted_data['params'])
                if dicted_data["method"] == "depth.update":
                    print_orderbook(dicted_data['params'])
            except:
                pass
asyncio.run(main())
