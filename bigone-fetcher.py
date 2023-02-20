import json
import websockets
import time
import asyncio
import requests

API_URL = "https://big.one/api/v3"
API_SYMBOLS = "/asset_pairs"
WS_URL = "wss://big.one/ws/v2"
TIMEOUT_SEND = 0.01

def create_trade_request(id, symbol):
    treade_request = json.dumps({"requestId": str(id),
                                 "subscribeMarketTradesRequest": {"market": symbol}})
    return treade_request

def create_orderbook_requets(id, symbol):
    depth_request = json.dumps({"requestId": str(id),
                        "subscribeMarketDepthRequest":{"market":symbol}})
    return depth_request

pairs = requests.get(API_URL + API_SYMBOLS)
symbols = [x['name'] for x in pairs.json()['data']]
trade_messages = []
orderbook_messages = []

for i in range(len(symbols)):
    trade_messages.append(create_trade_request(i, symbols[i]))
    orderbook_messages.append(create_orderbook_requets(len(symbols)+i, symbols[i]))

def print_trades(data):
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
            print("$", round(time.time() * 1000), data['depthSnapshot']['depth']['market'], "B",
                  '|'.join(x['amount'] + "@" + x['price'] for x in data['depthSnapshot']['depth']['bids']), "R", end="\n")
            print("$", round(time.time() * 1000), data['depthSnapshot']['depth']['market'], "S",
                  '|'.join(x['amount'] + "@" + x['price'] for x in data['depthSnapshot']['depth']['asks']), "R", end="\n")

        elif "depthUpdate" in data:
            if data['depthUpdate']['depth']['bids'] != []:
                print("$", round(time.time() * 1000), data['depthUpdate']['depth']['market'], "B",
                      '|'.join(x['amount']+"@"+x['price'] for x in data['depthUpdate']['depth']['bids']), end="\n")
            if data['depthUpdate']['depth']['asks'] != []:
                print("$", round(time.time() * 1000), data['depthUpdate']['depth']['market'], "S",
                      '|'.join(x['amount'] + "@" + x['price'] for x in data['depthUpdate']['depth']['asks']), end="\n")
    except:
        pass

async def main():
    async for ws in websockets.connect(uri=WS_URL, subprotocols=['graphql-ws'],
                                      extra_headers={"Sec-WebSocket-Protocol":"json"}):
        try:
            for i in range(len(symbols)):
                await ws.send(trade_messages[i])
                await ws.send(orderbook_messages[i])
                time.sleep(TIMEOUT_SEND)

            while True:
                try:
                    data = await ws.recv()
                    decoded_data = data.decode("utf-8")
                    dicted_data = json.loads(decoded_data)
                    if "tradeUpdate" in dicted_data:
                        print_trades(dicted_data)
                    if "depthSnapshot" in dicted_data or "depthUpdate" in dicted_data:
                        print_orderbooks(dicted_data)
                except KeyboardInterrupt:
                    exit(0)
                except Exception as e:
                    print(f"Exception {e} occurred")
        except Exception as conn_c:
            print(f"WARNING: connection exception {conn_c} occurred")
            continue
asyncio.run(main())