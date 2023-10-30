import json
import asyncio
import time
import websockets

WS_URL = 'wss://ace.io/socket.io/?EIO=3&transport=websocket'
WS_PUBLIC_SPOT_TRADE = 'tradeHistory'
WS_PUBLIC_SPOT_DEPTH_ORDERBOOK = 'orderBookNotify'
TIMEOUT = 0.1
PING_TIMEOUT = 10
symbol_names = {1: 'TWD', 2: 'BTC', 4: 'ETH', 7: 'LTC', 10: 'XRP', 13: 'TRX', 14: 'USDT', 17: 'BNB', 19: 'BTTC', 57: 'USDC', 66: 'MOCT', 70: 'DET', 72: 'QQQ', 74: 'HT', 75: 'UNIC', 76: 'QTC', 81: 'FTT', 83: 'BAAS', 84: 'OKB', 85: 'DAI', 88: 'ACEX', 89: 'LINK', 90: 'DEC', 93: 'HWGC', 94: 'KNC', 95: 'COMP', 96: 'DS', 97: 'CRO', 101: 'CREAM', 102: 'YFI', 103: 'WNXM', 104: 'MITH', 106: 'SGB', 107: 'ENJ',
                108: 'ANKR', 109: 'MANA', 110: 'SXP', 111: 'CHZ', 112: 'DOT', 114: 'CAKE', 115: 'SHIB', 116: 'DOGE', 117: 'MATIC', 119: 'WOO', 120: 'SLP', 121: 'AXS', 122: 'ADA', 123: 'QUICK', 124: 'FTM', 126: 'YGG', 127: 'GALA', 128: 'ILV', 129: 'DYDX', 130: 'SOL', 131: 'SAND', 132: 'AVAX', 133: 'LOOKS', 135: 'APE', 136: 'GMT', 139: 'GST', 141: 'TON', 144: 'SSV', 145: 'BUSD', 148: 'ARB'}
symbols = [[122, 1], [135, 1], [148, 1], [17, 1], [2, 1], [116, 1], [112, 1], [4, 1], [124, 1], [127, 1], [74, 1], [7, 1], [117, 1], [72, 1], [131, 1], [115, 1], [130, 1], [144, 1], [13, 1], [57, 1], [14, 1], [119, 1], [10, 1],
         [4, 2],
         [88, 14], [122, 14], [108, 14], [135, 14], [132, 14], [121, 14], [17, 14], [2, 14], [111, 14], [116, 14], [112, 14], [129, 14], [4, 14], [124, 14], [127, 14], [89, 14], [7, 14], [117, 14], [131, 14], [115, 14], [130, 14], [13, 14], [119, 14], [10, 14], [102, 14]]
old_trades = {}
for i in symbols:
        old_trades[str(i[0]) + '/' + str(i[1])] = 0

def format_decimal(value):
    try:
        if 'e' in str(value) or 'E' in str(value):
            parts = str(value).lower().split('e')
            print(int(parts[1]))
            precision = len(parts[0]) + abs(int(parts[1])) - 2
            return format(value, f'.{precision}f')
        else:
            return float(value)
    except:
        return value


def meta():
    for i in symbols:
        print("@MD", symbol_names[i[0]] + '/' + symbol_names[i[1]], "spot",
              symbol_names[i[0]], symbol_names[i[1]], -1, 1, 1, 0, 0, end="\n")
    print("@MDEND")


async def heartbeat(ws):
    while True:
        await ws.send(message=2)
        await asyncio.sleep(PING_TIMEOUT)


def print_trades(data, symbol):
    try:
        for i in data['content']:
            if i['buyOrSell'] == 1:
                bs = 'B'
            else:
                bs = 'S'
            print('!', round(time.time() * 1000),
                  symbol_names[symbol[0]] + '/' + symbol_names[symbol[1]], bs, str(format_decimal(i['current'])), str(format_decimal(i['amount'])))
    except:
        pass


def print_orderbooks(data, symbol):
    try:
        books_sell = data["updateSell"]
        books_buy = data["updateBuy"]
        if (len(books_sell) > 0):
            print('$', round(time.time() * 1000), symbol_names[symbol[0]] + '/' + symbol_names[symbol[1]], 'S', '|'.join(
                str(format_decimal(abs(value))) + '@' + str(format_decimal(key)) for key, value in books_sell.items()), 'R')
        if (len(books_buy) > 0):    
            print('$', round(time.time() * 1000), symbol_names[symbol[0]] + '/' + symbol_names[symbol[1]], 'B', '|'.join(
                str(format_decimal(abs(value))) + '@' + str(format_decimal(key)) for key, value in books_buy.items()), 'R')
    except:
        pass



async def subscribe(ws, symbol):
    await ws.send(message=f'42{json.dumps(["subscribe", {"baseCurrencyId": symbol[1], "tradeCurrencyId": symbol[0]}])}')
    await asyncio.sleep(TIMEOUT)


async def handle_socket(symbol):
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws, symbol))
            ping_task = asyncio.create_task(heartbeat(ws))
            while True:
                try:
                    data = await ws.recv()
                    if data[0] == '4' and data[1] == '2':
                        data_sliced = data[2:]
                        data_json = json.loads(data_sliced)
                        if WS_PUBLIC_SPOT_TRADE in data_sliced:
                            if old_trades[str(symbol[0]) + '/' + str(symbol[1])] < 2:
                                old_trades[str(symbol[0]) + '/' + str(symbol[1])] += 1
                            else:
                                print_trades(data_json[1], symbol)
                        if WS_PUBLIC_SPOT_DEPTH_ORDERBOOK in data_sliced:
                            print_orderbooks(data_json[1], symbol)
                except:
                    continue
        except KeyboardInterrupt:
            print("Keyboard Interupt")
            exit(1)
        except Exception as conn_c:
            print(f"WARNING: connection exception {conn_c} occurred")
            continue


async def handler():
    meta()
    await asyncio.wait([asyncio.create_task(handle_socket(symbol)) for symbol in symbols])


async def main():
    await handler()


asyncio.run(main())
