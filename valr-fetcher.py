import json
import requests
import websockets
import time
import asyncio
import os
import sys
from CommonFunctions.CommonFunctions import get_unix_time, stats

CURRENT_MODE = 'SPOT'

# Extract command-line arguments starting from index 1
args = sys.argv[1:]

# Check if there are any arguments
if len(args) > 0:
    # Iterate through the arguments
    for arg in args:
        # Check if the argument starts with '-'
        if arg.startswith('-') and arg[1:] == 'perpetual':
            CURRENT_MODE = 'FUTURES'
            break
else:
    CURRENT_MODE = "SPOT"

# get all available symbol pairs from exchange
currency_url = 'https://api.valr.com/pairs?showFutures=true'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
check_activity = {}
trades_count_5min = {}
orders_count_5min = {}
resubscribe_aggregated_orderbook = set()
# base web socket url
WS_URL = 'wss://api.valr.com/ws/trade'

# fill the list with all available symbol pairs on exchange
for pair_s in currencies:
    if pair_s['active'] and CURRENT_MODE == 'SPOT' and pair_s['currencyPairType'] == 'SPOT':
        list_currencies.append(pair_s['symbol'])
        check_activity[pair_s['symbol']] = False
    elif pair_s['active'] and CURRENT_MODE == 'FUTURES' and pair_s['currencyPairType'] == 'FUTURE':
        list_currencies.append(pair_s['symbol'])
        check_activity[pair_s['symbol']] = False


async def metadata():
    for pair in currencies:
        if pair['active'] and CURRENT_MODE == 'SPOT' and pair['currencyPairType'] == 'SPOT':
            trades_count_5min[pair['symbol']] = 0
            orders_count_5min[pair['symbol']] = 0
            pair_data = '@MD ' + pair['symbol'] + ' spot ' + pair['baseCurrency']['symbol'] + ' ' \
                        + pair['quoteCurrency']['symbol'] + ' ' + pair['baseDecimalPlaces'] + ' 1 1 0 0'
            print(pair_data, flush=True)
        elif pair['active'] and CURRENT_MODE == 'FUTURES' and pair['currencyPairType'] == 'FUTURE':
            trades_count_5min[pair['symbol']] = 0
            orders_count_5min[pair['symbol']] = 0
            pair_data = '@MD ' + pair['symbol'] + ' perpetual ' + pair['baseCurrency']['symbol'] + ' ' \
                        + pair['quoteCurrency']['symbol'] + ' ' + pair['baseDecimalPlaces'] + ' 1 1 0 0'
            print(pair_data, flush=True)
    print('@MDEND')


# function to format the trades output
def get_trades(message):
    check_activity[message['currencyPairSymbol']] = True
    trades_count_5min[message['currencyPairSymbol']] += 1
    print('!', get_unix_time(), message['currencyPairSymbol'],
          message['data']['takerSide'][0].upper(), message['data']['price'],
          message['data']['quantity'], flush=True)


# function to format order books and deltas(order book updates) format
def get_order_books_and_deltas(message, update):
    check_activity[message['currencyPairSymbol']] = True
    orders_count_5min[message['currencyPairSymbol']] += len(message['data']['Bids']) + len(message['data']['Asks'])
    # check if bids array is not Null
    if 'Bids' in message['data'] and message['data']['Bids']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['currencyPairSymbol'] + ' B '
        pq = ''
        for order in message['data']['Bids']:
            for quan in order['Orders']:
                pq += quan['quantity'] + '@' + order['Price'] + '|'
        pq = pq[:-1]
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


    # check if asks array is not Null
    if 'Asks' in message['data'] and message['data']['Asks']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['currencyPairSymbol'] + ' S '
        pq = ''
        for order in message['data']['Asks']:
            for quan in order['Orders']:
                pq += quan['quantity'] + '@' + order['Price'] + '|'
        pq = pq[:-1]
        # check if the input data is full order book or just update
        if update:
            print(order_answer + pq, flush=True)
        elif not update:
            print(order_answer + pq + ' R', flush=True)


def get_aggregated_orderbook(message):
    # check if bids array is not Null
    if 'Bids' in message['data'] and message['data']['Bids']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['currencyPairSymbol'] + ' B '
        pq = ''
        for order in message['data']['Bids']:
            for amountOrders in range(order['orderCount']):
                pq += order['quantity'] + '@' + order['price'] + '|'
        pq = pq[:-1]
        print(order_answer + pq + ' R', flush=True)


    # check if asks array is not Null
    if 'Asks' in message['data'] and message['data']['Asks']:
        order_answer = '$ ' + str(get_unix_time()) + ' ' + message['currencyPairSymbol'] + ' S '
        pq = ''
        for order in message['data']['Asks']:
            for amountOrders in range(order['orderCount']):
                pq += order['quantity'] + '@' + order['price'] + '|'
        pq = pq[:-1]
        print(order_answer + pq + ' R', flush=True)


async def heartbeat(ws):
    while True:
        await ws.send(json.dumps({
            "type": "PING"
        }))
        await asyncio.sleep(20)


async def subscribe(ws):
    while True:
        # find all the element from dict that wasn`t in pipeline at least once in the last hour
        resub_list = [key for key, value in check_activity.items() if value == False]
        # subscribe for listed topics
        await ws.send(json.dumps({
            "type": "SUBSCRIBE",
            "subscriptions": [
                {
                    "event": "NEW_TRADE",
                    "pairs": resub_list
                }
            ]
        }))
        if os.getenv("SKIP_ORDERBOOKS") is None or os.getenv("SKIP_ORDERBOOKS") == '':
            await ws.send(json.dumps({
                "type": "SUBSCRIBE",
                "subscriptions": [
                    {
                        "event": "FULL_ORDERBOOK_UPDATE",
                        "pairs": resub_list
                    }
                ]
            }))
            await subscribe_agg_orderbook(ws)
        # print(check_activity, len(resub_list))
        for symbol in list(check_activity):
            check_activity[symbol] = False
        # print(check_activity)
        await asyncio.sleep(1800)


async def subscribe_agg_orderbook(ws):
    # subscribe for aggregated orderbooks
    await ws.send(json.dumps({
        "type": "SUBSCRIBE",
        "subscriptions": [
            {
                "event": "AGGREGATED_ORDERBOOK_UPDATE",
                "pairs": list(resubscribe_aggregated_orderbook)
            }
        ]
    }))


# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
    time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
    if time_to_wait != 300:
        await asyncio.sleep(time_to_wait)
    while True:
        stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
        time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
        await asyncio.sleep(time_to_wait)


async def main():
    # create connection with server via base ws url
    async for ws in websockets.connect(WS_URL):
        try:
            sub_task = asyncio.create_task(subscribe(ws))
            # create task to keep connection alive
            pong = asyncio.create_task(heartbeat(ws))
            # print metadata about each pair symbols
            meta_data = asyncio.create_task(metadata())
            # print stats for trades and orders
            statistics = asyncio.create_task(print_stats(trades_count_5min, orders_count_5min))
            full_orderbook_sub_finished = True
            while True:
                # receiving data from server
                data = await ws.recv()
                try:
                    # change format of received data to json format
                    dataJSON = json.loads(data)
                    # print(dataJSON)
                    # check if received data is about trades
                    if 'type' in dataJSON:
                        if dataJSON['type'] == 'NEW_TRADE':
                            get_trades(dataJSON)
                        # check if received data is about updates on order book
                        elif dataJSON['type'] == 'FULL_ORDERBOOK_UPDATE':
                            get_order_books_and_deltas(dataJSON,
                                                       update=True)
                        # check if received data is about order books
                        elif dataJSON['type'] == 'FULL_ORDERBOOK_SNAPSHOT':
                            if full_orderbook_sub_finished:
                                await subscribe_agg_orderbook(ws)
                                full_orderbook_sub_finished = False
                            get_order_books_and_deltas(dataJSON,
                                                       update=False)
                        elif dataJSON['type'] == 'AGGREGATED_ORDERBOOK_UPDATE':
                            get_aggregated_orderbook(dataJSON)
                        elif dataJSON['type'] == "ERROR":
                            print(dataJSON)
                            time.sleep(1)
                            resubscribe_aggregated_orderbook.add(dataJSON['message'].replace(
                                "Channel FULL_ORDERBOOK_UPDATE, pair ", '').replace(' combination invalid', ''))
                        else:
                            print(dataJSON)
                    else:
                        print(dataJSON)
                except Exception as e:
                    print(f"Exception {e} occurred", data)
                    time.sleep(1)
        except Exception as conn_e:
            print(f"WARNING: connection exception {conn_e} occurred")
            time.sleep(1)

# run main function
asyncio.run(main())
