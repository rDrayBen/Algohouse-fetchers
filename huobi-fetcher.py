import json
import requests
import threading
import gzip
import websocket
import time

# get all cryptocoin symbols
currency_url = 'https://api.huobi.pro/v1/settings/common/market-symbols'
lock = threading.Lock()
curr_response = requests.get(currency_url)
resp = curr_response.json()
size_symbols = dict()
symbol_dict = dict()
for i in range(len(resp['data'])):
    if resp['data'][i]['state'] == 'online':
        size_symbols[resp['data'][i]['symbol']] = (resp['data'][i]['bc']).upper() + \
                                                  '-' + (resp['data'][i]['qc']).upper()
        symbol_dict[resp['data'][i]['bc']] = resp['data'][i]['qc']


def get_unix_time():
    return round(time.time() * 1000)


def get_trades(ws, message):
    trade_data = json.loads(gzip.decompress(message))

    if 'ping' in trade_data:
        ws.send(json.dumps({
            "pong": trade_data['ping']
        }))
    elif 'ch' in trade_data:
        coin_name = trade_data['ch'].replace('market.', '').replace('.trade.detail', '')
        for elem in trade_data['tick']['data']:
            lock.acquire()
            print('!', get_unix_time(),
                      'huobi', size_symbols[coin_name],
                      str(elem['direction'])[:1].upper(), str('{0:.9f}'.format(elem['price'])),
                      str('{0:.4f}'.format(elem['amount'])), flush=True)
            lock.release()


def trade(ws):
    # create trades for each cryptocoin symbol
    for key, value in symbol_dict.items():
        ws.send(json.dumps({
            "sub": f"market.{key + value}.trade.detail",
            'id': '428550639'
        }))


def order(ws):
    for key, value in symbol_dict.items():
        ws.send(json.dumps({
            "sub": f"market.{key+value}.depth.step0",
            'id': '428550639'
        }))


def get_orders(ws, message):
    order_data = json.loads(gzip.decompress(message))

    if 'ping' in order_data:
        ws.send(json.dumps({
            "pong": order_data['ping']
        }))
    elif 'ch' in order_data:
        coin_name = order_data['ch'].replace('market.', '').replace('.depth.step0', '')
        answer = ''
        if len(order_data['tick']['bids']) != 0:
            answer += '$ ' + str(get_unix_time()) + ' huobi ' + size_symbols[coin_name] + ' B '
            pq = '|'.join(f"{str('{0:.10f}'.format(elem[1]))}@{str('{0:.8f}'.format(elem[0]))}"
                               for elem in order_data['tick']['bids']) + ' R'
            lock.acquire()
            print(answer + pq, flush=True)
            lock.release()
        if len(order_data['tick']['asks']) != 0:
            answer = ''
            answer += '$ ' + str(get_unix_time()) + ' huobi ' + size_symbols[coin_name] + ' S '
            pq = '|'.join(f"{str('{0:.10f}'.format(elem[1]))}@{str('{0:.8f}'.format(elem[0]))}"
                               for elem in order_data['tick']['asks']) + ' R'
            lock.acquire()
            print(answer + pq, flush=True)
            lock.release()


def delta(ws):
    for key, value in symbol_dict.items():
        ws.send(json.dumps({
            "sub": f"market.{key+value}.mbp.150",
            'id': '428550639'
        }))


def get_deltas(ws, message):
    delta_data = json.loads(gzip.decompress(message))

    if 'ping' in delta_data:
        ws.send(json.dumps({
            "pong": delta_data['ping']
        }))
    elif 'ch' in delta_data:
        coin_name = delta_data['ch'].replace('market.', '').replace('.mbp.150', '')
        answer = ''
        if delta_data['tick']['bids']:
            answer += '$ ' + str(get_unix_time()) + ' huobi ' + size_symbols[coin_name] + ' B '
            pq = '|'.join(f"{str('{0:.10f}'.format(elem[1]))}@{str('{0:.8f}'.format(elem[0]))}"
                          for elem in delta_data['tick']['bids'])
            lock.acquire()
            print(answer + pq, flush=True)
            lock.release()

        if delta_data['tick']['asks']:
            answer = ''
            answer += '$ ' + str(get_unix_time()) + ' huobi ' + size_symbols[coin_name] + ' S '
            pq = '|'.join(f"{str('{0:.10f}'.format(elem[1]))}@{str('{0:.8f}'.format(elem[0]))}"
                          for elem in delta_data['tick']['asks'])
            lock.acquire()
            print(answer + pq, flush=True)
            lock.release()


def main1(ws):
    trade(ws)
    while True:
        message_trade = ws.recv()
        try:
            get_trades(ws, message_trade)
        except:
            pass


def main2(ws):
    order(ws)
    while True:
        message_order = ws.recv()
        try:
            get_orders(ws, message_order)
        except:
            pass


def main3(ws):
    delta(ws)
    while True:
        message_delta = ws.recv()
        try:
            get_deltas(ws, message_delta)
        except:
            pass


def main():
    ws = websocket.create_connection("wss://api.huobi.pro/ws")

    t1 = threading.Thread(target=main1, args=(ws,))
    t2 = threading.Thread(target=main2, args=(ws,))
    t3 = threading.Thread(target=main3, args=(ws,))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()


if __name__ == '__main__':
    main()
