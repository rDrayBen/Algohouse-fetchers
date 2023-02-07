import json
import websocket
import time
import asyncio
import requests

API_URL = "https://big.one/api/v3" # REST server main url
API_SYMBOLS = "/asset_pairs"  # Take all pairs from ticker price
WS_URL = "wss://big.one/ws/v2"  # Websocket server main url

def create_trade_request(id, symbol):
    """
    Takes two required params
    id - it`s websocket chanel id
    symbol - it`s websocket chanel instrument aka BTC-USDT

    Function creates new message to websocket server to take latest trades
    """
    treade_request = json.dumps({"requestId": str(id),
                                 "subscribeMarketTradesRequest": {"market": symbol}})
    return treade_request

def create_orderbook_requets(id, symbol):
    """
     Takes two required params
     id - it`s websocket chanel id
     symbol - it`s websocket chanel instrument aka BTC-USDT

     Function creates new message to websocket server to take orderbooks or deltas
     """
    depth_request = json.dumps({"requestId": str(id),
                        "subscribeMarketDepthRequest":{"market":symbol}})
    return depth_request

def print_trades(data):
    """
    data - received last trade dictionary
    print recent trades
    """
    try:
        if data["tradeUpdate"]["trade"]["takerSide"] == 'BID':
            print("!", round(time.time() * 1000), data["tradeUpdate"]["trade"]["market"],
                  "B", data["tradeUpdate"]["trade"]["price"], data["tradeUpdate"]["trade"]["amount"], end="\n")
        else:
            print("!", round(time.time() * 1000), data["tradeUpdate"]["trade"]["market"],
                  "S", data["tradeUpdate"]["trade"]["price"], data["tradeUpdate"]["trade"]["amount"], end="\n")
    except:
        pass

def print_orderbooks(data):
    try:
        if "depthSnapshot" in data:
            print("$", round(time.time() * 1000), data['depthSnapshot']['depth']['market'],
                  '|'.join(x['amount'] + "@" + x['price'] for x in data['depthSnapshot']['depth']['bids']), "R", end="\n")
            print("$", round(time.time() * 1000), data['depthSnapshot']['depth']['market'],
                  '|'.join(x['amount'] + "@" + x['price'] for x in data['depthSnapshot']['depth']['asks']), "R", end="\n")

        elif "depthUpdate" in data:
            if data['depthUpdate']['depth']['bids'] != []:
                print("$", round(time.time() * 1000), data['depthUpdate']['depth']['market'],
                      '|'.join(x['amount']+"@"+x['price'] for x in data['depthUpdate']['depth']['bids']), end="\n")
            if data['depthUpdate']['depth']['asks'] != []:
                print("$", round(time.time() * 1000), data['depthUpdate']['depth']['market'],
                      '|'.join(x['amount'] + "@" + x['price'] for x in data['depthUpdate']['depth']['asks']), end="\n")
    except:
        pass

async def main():
    pairs = requests.get(API_URL + API_SYMBOLS)  # REST request to API to get tickets and theirs symbols
    symbols = [x['name'] for x in pairs.json()['data']]  # Get all pairs aka BTC-USD etc.
    trade_messages = []
    orderbook_messages = []

    # Iterating and creating new json messages to websocket server
    for i in range(len(symbols)):
        trade_messages.append(create_trade_request(i, symbols[i]))
        orderbook_messages.append(create_orderbook_requets(len(symbols)+i, symbols[i]))

    try:
        # Connecting to websocket server
        ws = websocket.WebSocket()
        ws.connect(url=WS_URL, header={"Sec-WebSocket-Protocol":"json"})

        # Send messages to server (maximum 500 messages per 10 secs). AVG time of working 4-7 secs.
        for i in range(len(symbols)):
            ws.send(trade_messages[i])
            ws.send(orderbook_messages[i])
            time.sleep(0.01)

        # Reciving latest trades and snapshots with deltas in byte format and loads it with json
        while True:
            try:
                data = ws.recv()
                decoded_data = data.decode("utf-8")
                dicted_data = json.loads(decoded_data)
                if "tradeUpdate" in dicted_data:
                    print_trades(dicted_data)
                if "depthSnapshot" in dicted_data or "depthUpdate" in dicted_data:
                    print_orderbooks(dicted_data)
            except:
                pass
    except:
        print("Error")
asyncio.run(main())