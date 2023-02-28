import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.fmfw.io/api/2/public/symbol'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.fmfw.io/api/2/ws/public'

for element in currencies:
    list_currencies.append(element["id"])

def get_unix_time():
    return round(time.time() * 1000)


def get_trades(var):
    trade_data = var
    if 'data' in trade_data["params"]:
        for elem in trade_data["params"]["data"]:
            print('!', get_unix_time(), trade_data["params"]['symbol'],
                  "B" if elem["side"] == "buy" else "S", elem['price'],
                  elem["quantity"], flush=True)


def get_order_books(var, update):
    order_data = var
    if 'ask' in order_data['params'] and len(order_data["params"]["ask"]) != 0:
        order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params']['symbol'] + ' S '
        pq = "|".join(el["size"] + "@" + el["price"] for el in order_data["params"]["ask"])
        answer = order_answer + pq
        # checking if the input data is full orderbook or just update
        if (update == True):
            print(answer)
        else:
            print(answer + " R")

    if 'bid' in order_data['params'] and len(order_data["params"]["bid"]) != 0:
        order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params']['symbol'] + ' B '
        pq = "|".join(el["size"] + "@" + el["price"] for el in order_data["params"]["bid"])
        answer = order_answer + pq
        # checking if the input data is full orderbook or just update
        if (update == True):
            print(answer)
        else:
            print(answer + " R")


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
            "event": "ping"
        }))
        await asyncio.sleep(5)


async def main():
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL, ping_interval=None):
        try:
            pong = asyncio.create_task(heartbeat(ws))
            for i in range(len(list_currencies)):
                # create the subscription for trades
                await ws.send(json.dumps({
                    "method": "subscribeTrades",
                    "params": {
                        "symbol": f"{list_currencies[i]}",
                        "limit": 100
                    },
                    "id": 123
                }))

                # create the subscription for full orderbooks and updates
                await ws.send(json.dumps({
                    "method": "subscribeOrderbook",
                    "params": {
                        "symbol": f"{list_currencies[i]}"
                    },
                    "id": 123
                }))

            while True:
                data = await ws.recv()

                dataJSON = json.loads(data)

                if "method" in dataJSON:

                    try:

                        # if received data is about trades
                        if dataJSON['method'] == 'updateTrades':
                            get_trades(dataJSON)

                        # if received data is about updates
                        if dataJSON['method'] == 'updateOrderbook':
                            get_order_books(dataJSON, update=True)

                        # if received data is about orderbooks
                        if dataJSON['method'] == 'snapshotOrderbook':
                            get_order_books(dataJSON, update=False)

                        else:
                            pass

                    except Exception as ex:
                        print(f"Exception {ex} occurred")

        except Exception as conn_ex:
            print(f"Connection exception {conn_ex} occurred")



asyncio.run(main())



