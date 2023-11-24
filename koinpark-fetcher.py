import json
import requests
import asyncio
import time
import websockets
from CommonFunctions.CommonFunctions import get_unix_time, stats
import os

API_URL = 'https://api.koinpark.com'
API_SPOT_SYMBOLS_URL = '/public_api/markets'
WS_URL = 'wss://knprklvtrdkand.koinpark.com/socket.io/?EIO=4&transport=websocket'
is_subscribed_orderbooks = {}
is_subscribed_trades = {}

TIMEOUT = 0.1
PING_TIMEOUT = 25
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['trading_pairs'] for x in response.json()['data']]

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(symbols)):
	symbol_trade_count_for_5_minutes[symbols[i].upper()] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(symbols)):
	symbol_orderbook_count_for_5_minutes[symbols[i].upper()] = 0

# get metadata about each pair of symbols
async def metadata():
	for pair in response.json()["data"]:
		pair_data = '@MD ' + pair["trading_pairs"].split("_")[0] + '_' + pair["trading_pairs"].split("_")[1] + ' spot ' + \
					pair["trading_pairs"] + '-1 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

async def heartbeat(ws):
	while True:
		await ws.send(message='3')
		await asyncio.sleep(PING_TIMEOUT)

# put the trade information in output format
def get_trades(var):
	trade_data = var
	print('!', get_unix_time(), trade_data[0].split("_", 1)[1],
		  "B" if trade_data[1]['data']["isBuyerMaker"] else "S", trade_data[1]['data']['price'],
		  trade_data[1]['data']["amount"], flush=True)
	symbol_trade_count_for_5_minutes[trade_data[0].split("_", 1)[1]] += 1


# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	pq_S = ""
	pq_B = ""
	iteration = 0
	for elem in order_data[1]['data']['changes']:
		if 'sell' in elem and len(elem) != 0:
			symbol_orderbook_count_for_5_minutes[order_data[1]['productId']] += 1
			if iteration == 0:
				pq_S += elem[2] + "@" + elem[1]
			else:
				pq_S += "|" + elem[2] + "@" + elem[1]
		if 'buy' in elem and len(elem) != 0:
			symbol_orderbook_count_for_5_minutes[order_data[1]['productId']] += 1
			if iteration == 0:
				pq_B += elem[2] + "@" + elem[1]
			else:
				pq_B += "|" + elem[2] + "@" + elem[1]

		iteration += 1

	if len(pq_S) != 0:
		order_answer_S = '$ ' + str(get_unix_time()) + " " + order_data[1]['productId'] + ' S '
		answer_S = order_answer_S + pq_S
		print(answer_S)

	if len(pq_B) != 0:
		order_answer_B = '$ ' + str(get_unix_time()) + " " + order_data[1]['productId'] + ' B '
		answer_B = order_answer_B + pq_B
		print(answer_B)

async def subscribe(ws):
	await ws.send(message='40')

# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	if time_to_wait != 300:
		await asyncio.sleep(time_to_wait)
	while True:
		stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
		await asyncio.sleep(300)

async def main():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes))
	async for ws in websockets.connect(WS_URL):
		try:
			sub_task = asyncio.create_task(subscribe(ws))
			ping_task = asyncio.create_task(heartbeat(ws))
			while True:
				try:
					data = await ws.recv()
					if data[0] == '4' and data[1] == '2':
						data_sliced = data[2:]
						dataJSON = json.loads(data_sliced)

						# if received data is about trades
						if "tradehistory" in dataJSON[0]:
							get_trades(dataJSON)
						# if received data is about orderbooks
						elif "orderBookMatch" in dataJSON[0]:
							# possibility to not subscribe or report orderbook changes
							if os.getenv("SKIP_ORDERBOOKS") == None:
								get_order_books(dataJSON, depth_update=True)
						else:
							pass
				except:
					continue
		except:
			continue


asyncio.run(main())
