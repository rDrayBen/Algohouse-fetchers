import json
import websockets
import time
import asyncio
import requests
API_URL = 'https://api.bitunix.com'
API_SYMBOLS = '/web/api/v1/common/coin_pair/list'
WSS_URL = 'wss://api.bitunix.com/ws/'
SUB_TIMEOUT = 0.25
PING_TIMEOUT = 5
DEPTH_SNAPSHOT_SIZE = 15
response = requests.get(API_URL + API_SYMBOLS)
trade_channel = ['market_' + i['base'].lower() + i['quote'].lower() + '_deals' for i in response.json()['data']]
depth_channel = ['market_' + i['base'].lower() + i['quote'].lower() + '_depth_' + i['precisions'][j]
                 for i in response.json()['data'] for j in range(len(i['precisions']))]
historical_trades_dict = {}
snapshot_dict = {}
for i in response.json()['data']:
    historical_trades_dict['market_' + i['base'].lower() + i['quote'].lower() + '_deals'] = \
        [0, i['base'] + i['quote']]
    for j in range(len(i['precisions'])):
        snapshot_dict['market_' + i['base'].lower() + i['quote'].lower() + '_depth_' + i['precisions'][j]] \
            = i['base'] + i['quote']

async def meta(respose):
    for i in respose.json()['data']:
        if i['isShow'] == 1:
            print('@MD', i['base'] + i['quote'],
                  'spot', i['base'], i['quote'],
                  i['quotePrecision'], 1, 1, 0, 0, end='\n')
    print("@MDEND")
async def subscribe(ws, respose):
    await ws.send(json.dumps(
        {
            "event": "sub",
            "params": {"channel": f"market_{respose['base'].lower()+respose['quote'].lower()}_deals"}
        }
    ))
    await asyncio.sleep(SUB_TIMEOUT)
    for j in range(len(respose['precisions'])):
        await ws.send(json.dumps(
            {
                "event": "sub",
                "params": {"channel": f"market_{respose['base'].lower() + respose['quote'].lower()}_depth_{respose['precisions'][j]}"}
            }
        ))
        await asyncio.sleep(SUB_TIMEOUT)
    await asyncio.sleep(SUB_TIMEOUT)
async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({"ping": round(time.time() * 1000)}))
        await asyncio.sleep(PING_TIMEOUT)
def print_trades(data, symbol):
    for i in data['data']:
        print("!", round(time.time() * 1000), symbol, i['side'][0].upper(), i['price'], i['vol'], end='\n')
def print_snapshot(data, symbol):
    if data['data']['asks'] != []:
        if len(data['data']['asks']) >= DEPTH_SNAPSHOT_SIZE:
            print('$', round(time.time() * 1000), symbol, 'S',
                  '|'.join(i[1] + "@" + i[0] for i in data['data']['asks']),
                  'R', end='\n')
        else:
            print('$', round(time.time() * 1000), symbol, 'S',
                  '|'.join(i[1] + "@" + i[0] for i in data['data']['asks']),
                  end='\n')
    if data['data']['bids'] != []:
        if len(data['data']['bids']) >= DEPTH_SNAPSHOT_SIZE:
            print('$', round(time.time() * 1000), symbol, 'B',
                  '|'.join(i[1] + "@" + i[0] for i in data['data']['bids']),
                  'R', end='\n')
        else:
            print('$', round(time.time() * 1000), symbol, 'B',
                  '|'.join(i[1] + "@" + i[0] for i in data['data']['bids']),
                  end='\n')

async def handle_socket(symbol, ):
    async for ws in websockets.connect(WSS_URL, ping_interval=None):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            ping_task = asyncio.create_task(heartbeat(ws))
            async for data in ws:
                try:
                    dataJSON = json.loads(data)
                    if "ping" in dataJSON:
                        await ws.send(json.dumps({"ping": round(time.time() * 1000)}))
                        continue
                    if dataJSON['channel'] in trade_channel:
                        if historical_trades_dict[dataJSON['channel']][0] == 0:
                            historical_trades_dict[dataJSON['channel']][0] += 1
                            continue
                        else:
                            print_trades(dataJSON, historical_trades_dict[dataJSON['channel']][1])
                    if dataJSON['channel'] in depth_channel:
                        print_snapshot(dataJSON, snapshot_dict[dataJSON['channel']])
                except KeyboardInterrupt:
                    exit(0)
                except:
                    pass
        except KeyboardInterrupt:
            exit(0)
        except:
            continue

async def handler():
    meta_task = asyncio.create_task(meta(response))
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in response.json()['data']])
def main():
    asyncio.get_event_loop().run_until_complete(handler())
main()