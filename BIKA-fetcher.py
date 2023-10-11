import json
import requests
import asyncio
import time
import hashlib
import websockets
API_URL = 'https://www.bika.one'
API_SYMBOLS = '/v2/spot/quotationList?ts='
HEADERS = {
    'Deviceid':'12ca17b49af2289436U001e0166030a21e525d266e209267433801a8fd4071Y1',  # random SHA256 hash
    'T_s': 't=1691765174&s=6da4d9146d8178bc3f32b51bf07a4191'  # fixed timestamp
}
WSS_URL = ''
SUBSCRIBE_TIMEOUT = 0.001
dataSign = ''
dataSignEncoded = dataSign.encode('utf-8')
hash = hashlib.sha256(dataSignEncoded)
response = requests.get(API_URL + API_SYMBOLS + str(round(time.time() * 1000)) + '&dataSign=' + str(hash),
                        headers=HEADERS)
symbols = [i['quotationCoin'] + i['marginCoin'] for i in response.json()['data']]
trade_channels = ['market_' + i.lower() + '_trade_ticker' for i in symbols]
depth_channels = ['market_e_' + i.lower() + '_depth_20' for i in symbols]
precisions = {}
channels_dict = {}
for i in range(len(trade_channels)):
    channels_dict[trade_channels[i]] = symbols[i]
    channels_dict[depth_channels[i]] = symbols[i]
    precisions[trade_channels[i]] = response.json()['data'][i]['quantityPrecision']
    precisions[depth_channels[i]] = (response.json()['data'][i]['quantityPrecision'],
                                     response.json()['data'][i]['quotationPrecision'],
                                     )
async def meta(respose):
    if respose.status_code == 200:
        for i in respose.json()['data']:
            print("@MD", i['quotationCoin'] + i['marginCoin'], 'spot', i['quotationCoin'], i['marginCoin'],
                  i['quantityPrecision'], 1, 1, 0, 0, end='\n')
        print('@MDEND')
async def subscribe(ws, symbol):
    await ws.send(json.dumps(
        {
                "symbol": symbol,
                "interval": None,
                "topic": "sub"
        }
    ))
def print_trades(data, symbol, precision):
    print("!", round(time.time() * 1000), symbol, data['data']['side'][0].upper(),
          data['data']['price'], "{:.{}f}".format(data['data']['vol'], precision),
          end='\n')
def print_orderbooks(data, isSnapshot, symbol, precision, price_precision):
    if isSnapshot:
        if data['data']['tick']['asks'] != []:
            print("$", round(time.time() * 1000), symbol, 'S',
                  '|'.join(
                      "{:.{}f}".format(i[1], precision)
                       + "@" +
                      "{:.{}f}".format(i[0], price_precision)
                      for i in data['data']['tick']['asks']), 'R', end='\n')
        if data['data']['tick']['buys'] != []:
            print("$", round(time.time() * 1000), symbol, 'B',
                  '|'.join(
                      "{:.{}f}".format(i[1], precision)
                      + "@" +
                      "{:.{}f}".format(i[0], price_precision)
                      for i in data['data']['tick']['buys']), 'R', end='\n')
async def handle_socket(symbol):
    current_timestamp = str(round(time.time() * 1000))
    string_to_encode = 'BQMEX_' + current_timestamp + '##'
    encoded_string = string_to_encode.encode('utf-8')
    hashed_timestamp = str(hashlib.sha256(encoded_string).hexdigest())
    WSS_URL = 'wss://prod-bika-bq-spot-market-ws-p-group2.api-on-a.live/ws/quotation?ts=' \
              + current_timestamp + '&sign=' + hashed_timestamp
    async for ws in websockets.connect(WSS_URL, ping_interval=None):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            async for data in ws:
                try:
                    dataJSON = json.loads(data)
                    if dataJSON['data']['channel'] in trade_channels:
                        print_trades(dataJSON, channels_dict[dataJSON['data']['channel']],
                                     precisions[dataJSON['data']['channel']])
                    if dataJSON['data']['channel'] in depth_channels:
                        print_orderbooks(dataJSON, 1, channels_dict[dataJSON['data']['channel']],
                                         precisions[dataJSON['data']['channel']][0],
                                         precisions[dataJSON['data']['channel']][1])
                except KeyboardInterrupt:
                    exit(0)
                except:
                    pass
        except KeyboardInterrupt:
            print("Keyboard Interupt")
            exit(1)
        except Exception as conn_c:
            continue
async def handler():
    meta_task = asyncio.create_task(meta(response))
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in symbols])
def main():
    asyncio.get_event_loop().run_until_complete(handler())
main()