import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

currency_url = 'https://api.icrypex.com/v1/exchange/info'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://istream.icrypex.com/'

for element in currencies["pairs"]:
	list_currencies.append(element["symbol"])

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i]] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["pairs"]:
		pair_data = '@MD ' + pair["base"] + pair["quote"] + ' spot ' + \
					pair["base"] + ' ' + pair["quote"] + \
					' ' + str(pair['pricePrecision']) + ' 1 1 0 0'
		print(pair_data, flush=True)
	print('@MDEND')

async def subscription(ws):
	for i in range(len(list_currencies)):
		if os.getenv("SKIP_ORDERBOOKS") == None:  # don't subscribe or report orderbook changes
			# create the subscription for full orderbooks and updates
			await ws.send(f'subscribe|{json.dumps({"c": f"orderbook@{list_currencies[i]}","s": True})}')

		# create the subscription for trades
		await ws.send(f'subscribe|{json.dumps({"c": f"trade@{list_currencies[i]}", "s": True})}')

def get_trades(var):
	trade_data = var
	print('!', get_unix_time(), trade_data["ps"],
		"B" if trade_data["s"] == 0 else "S", trade_data['p'], trade_data["q"], flush=True)
	symbol_trade_count_for_5_minutes[trade_data["ps"]] += 1

def get_order_books(var, update):
	order_data = var
	if 'a' in order_data and len(order_data["a"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['ps']] += len(order_data["a"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['ps'] + ' S '
		pq = "|".join(el["q"] + "@" + el["p"] for el in order_data["a"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'b' in order_data and len(order_data["b"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['ps']] += len(order_data["b"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['ps'] + ' B '
		pq = "|".join(el["q"] + "@" + el["p"] for el in order_data["b"])
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

# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	if time_to_wait != 300:
		await asyncio.sleep(time_to_wait)
	while True:
		stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
		await asyncio.sleep(1)
		time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
		await asyncio.sleep(time_to_wait)

async def main():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes))
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:

			# create task to keep connection alive
			#pong = asyncio.create_task(heartbeat(ws))

			subscribe = asyncio.create_task(subscription(ws))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data.split("|")[1])


				if data.split("|")[0] == "orderbook" or data.split("|")[0] == "trade" or data.split("|")[0] == "obd":

					try:

						# if received data is about trades
						if data.split("|")[0] == "trade":
							get_trades(dataJSON)

						# if received data is about updates
						if data.split("|")[0] == "obd":
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						if data.split("|")[0] == "orderbook":
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
