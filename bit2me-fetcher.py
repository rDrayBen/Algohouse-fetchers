import json
import requests
import websockets
import time
import asyncio
from CommonFunctions.CommonFunctions import get_unix_time, stats
import os

# get all available symbol pairs
currency_url = 'https://gateway.bit2me.com/v1/trading/market-config'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://ws.bit2me.com/v1/trading'

for element in currencies:
		list_currencies.append(element["symbol"])

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i]] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0

async def subscribe(ws):
	for pair in list_currencies:
		await ws.send(json.dumps({"event": "subscribe", "subscription": {"name": f"{pair}"}}))
		await asyncio.sleep(0.5)
	await asyncio.sleep(300)

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies:
		pair_data = '@MD ' + pair["symbol"].split("/")[0] + '/' + pair["symbol"].split("/")[1] + ' spot ' + \
					pair["symbol"].split("/")[0] + ' ' + pair["symbol"].split("/")[0] + \
					' ' + str(pair['pricePrecision']) + ' 1 1 0 0'
		print(pair_data, flush=True)
	print('@MDEND')

# put the trade information in output format
def get_trades(var):
	trade_data = var
	print('!', get_unix_time(), trade_data["pair"],
		  "B" if trade_data['data'][0] else "S", trade_data['data'][1],
		  trade_data['data'][2], flush=True)
	symbol_trade_count_for_5_minutes[trade_data["pair"]] += 1

# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	if 'asks' in order_data['data'] and len(order_data["data"]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['pair']] += len(order_data["data"]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['pair'] + ' S '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["data"]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")
	if 'bids' in order_data['data'] and len(order_data["data"]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['pair']] += len(order_data["data"]["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['pair'] + ' B '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["data"]["bids"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

# process the situations when the server awaits "ping" request
async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({"event":"ping"}))
		await asyncio.sleep(5)

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
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:

			# create task to subscribe to symbols` pair
			subscription = asyncio.create_task(subscribe(ws))

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			while True:

				data = await ws.recv()
				dataJSON = json.loads(data)

				if "event" in dataJSON:
					try:
						# if received data is about trades
						if dataJSON['event'] == 'last-trade':
							get_trades(dataJSON)
						# if received data is about updates
						elif dataJSON['event'] == 'order-book':
							if os.getenv("SKIP_ORDERBOOKS") == None:
								get_order_books(dataJSON, depth_update=False)
						else:
							pass
					except Exception as ex:
						print(f"Exception {ex} occurred")
		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
