import json
import websockets
import time
import asyncio
import requests
API_URL = "https://api.cryptology.com"
API_SYMBOLS = "/v1/public/get-trade-pairs"
WSS_URL = "wss://octopus-prod-ws.cryptology.com/v1/connect"
TIMEOUT = 0.2
PING_TIMEOUT = 10
async def meta(response):
    for i in response['payload']['data']['instruments']:
        if response['payload']['data']['instruments'][i]['is_enabled']:
            print("@MD", i, "spot", response['payload']['data']['instruments'][i]['base'],
                  response['payload']['data']['instruments'][i]['counter'],
                  response['payload']['data']['instruments'][i]['counter_currency_precision'], 1, 1, 0, 0, end="\n")
    print("@MDEND")
async def subscribe(ws, data):
    for i in data:
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": f"photonTrades.{i}",
            "params": {
                "rate_limit": 10
            },
            "request_id": f"photonTrades.{i}::1686918920142::cym8zumrk"
        }))
        await asyncio.sleep(TIMEOUT)
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": f"photonOrderbookVolumes.{i}",
            "params": {
                "rate_limit": 10
            },
            "request_id": f"photonOrderbookVolumes.{i}::1686918920142::cym8zumrk"
        }))
        await asyncio.sleep(TIMEOUT)
async def heartbeat(ws):
    while True:
        await ws.send(json.dumps("0"))
        await asyncio.sleep(PING_TIMEOUT)
def print_trade(data):
    for i in data['payload']['data']:
        print("!", round(time.time() * 1000), i['trade_pair'], i['taker_order_type'][0],
              i['price'], i['amount'], end='\n' )
def print_orderbooks(data, isSnapshot):
    if isSnapshot:
        if data['payload']['data']['volumes']['bids'] != {}:
            print("$", round(time.time()*1000), data['payload']['data']['trade_pair'], "B",
                  "|".join(value+"@"+key for key, value in data['payload']['data']['volumes']['bids'].items()), "R", end="\n")
        if data['payload']['data']['volumes']['asks'] != {}:
            print("$", round(time.time() * 1000), data['payload']['data']['trade_pair'], "S",
                  "|".join(value + "@" + key for key, value in data['payload']['data']['volumes']['asks'].items()), "R",
                  end="\n")
    else:
        if data['payload']['data']['volumes']['bids'] != {}:
            print("$", round(time.time() * 1000), data['payload']['data']['trade_pair'], "B",
                  "|".join(value + "@" + key for key, value in data['payload']['data']['volumes']['bids'].items()),
                  end="\n")
        if data['payload']['data']['volumes']['asks'] != {}:
            print("$", round(time.time() * 1000), data['payload']['data']['trade_pair'], "S",
                  "|".join(value + "@" + key for key, value in data['payload']['data']['volumes']['asks'].items()),
                  end="\n")
async def main():
    symbols_ = []
    async with websockets.connect(WSS_URL) as ws:
        try:
            await ws.send(json.dumps({"type":"subscribe",
                                      "channel":"photonInstruments.*",
                                      "request_id":"photonInstruments.*::::1686917500627::aukbqmvef"}))
            while symbols_ == []:
                response_raw = await ws.recv()
                response = json.loads(response_raw)
                if response['type'] == "subscriptionData":
                    symbols = [i for i in response['payload']['data']['instruments']]
                    symbols_.extend(symbols)
                    asyncio.create_task(meta(response))
                    await ws.close()
                    break
        except:
            await ws.close()
    trade_channels = ["photonTrades." + i for i in symbols_]
    orderbook_channels = ["photonOrderbookVolumes." + i for i in symbols_]
    orderbook_channels_frequency_dict = {}
    for i in orderbook_channels:
        orderbook_channels_frequency_dict[i] = 0

    async for ws in websockets.connect(WSS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbols_))
            ping_task = asyncio.create_task(heartbeat(ws))
            while True:
                try:
                    data = await ws.recv()
                    dataJSON = json.loads(data)
                    if dataJSON['type'] == "subscriptionData":
                        if dataJSON['payload']['channel'] in trade_channels:
                            print_trade(dataJSON)
                        if dataJSON['payload']['channel'] in orderbook_channels:
                            if orderbook_channels_frequency_dict[dataJSON['payload']['channel']] == 0:
                                print_orderbooks(dataJSON, 1)
                                orderbook_channels_frequency_dict[dataJSON['payload']['channel']] += 1
                            else:
                                print_orderbooks(dataJSON, 0)
                except KeyboardInterrupt:
                    exit(0)
                except:
                    continue
        except:
            continue
asyncio.run(main())