import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

currency_url = 'https://api.btcturk.com/api/v2/server/exchangeinfo'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://ws-feed-pro.btcturk.com/'

for element in currencies["data"]["symbols"]:
	list_currencies.append(element["name"])

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
	for pair in currencies["data"]["symbols"]:
		pair_data = '@MD ' + pair["numerator"] + pair["denominator"] + ' spot ' + \
					pair["numerator"] + ' ' + pair["denominator"] + \
					' ' + str(pair['denominatorScale']) + ' 1 1 0 0'
		print(pair_data, flush=True)
	print('@MDEND')

def get_trades(var):
	trade_data = var
	print('!', get_unix_time(), trade_data[1]["PS"],
		"B" if trade_data[1]["S"] == 0 else "S", trade_data[1]["P"],
		 trade_data[1]["A"], flush=True)
	symbol_trade_count_for_5_minutes[trade_data[1]["PS"]] += 1

def get_order_books(var, update):
	order_data = var
	if 'AO' in order_data[1] and len(order_data[1]["AO"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data[1]['PS']] += len(order_data[1]["AO"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data[1]['PS'] + ' S '
		pq = "|".join(el["A"] + "@" + el["P"] for el in order_data[1]["AO"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'BO' in order_data[1] and len(order_data[1]["BO"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data[1]['PS']] += len(order_data[1]["BO"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data[1]['PS'] + ' B '
		pq = "|".join(el["A"] + "@" + el["P"] for el in order_data[1]["BO"])
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
			pong = asyncio.create_task(heartbeat(ws))

			for i in range(len(list_currencies)):

				if os.getenv("SKIP_ORDERBOOKS") == None:  # don't subscribe or report orderbook changes
					# create the subscription for full orderbooks and updates
					await ws.send(json.dumps([151,
						 {"type":151,
						  "channel":"obdiff",
						  "event":f"{list_currencies[i]}",
						  "join":True}
						 ]))

				# create the subscription for trades
				await ws.send(json.dumps(
					[151,
					 {"type":151,
						"channel":"trade",
						"event":f"{list_currencies[i]}",
						"join":True}
						]
				))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "event" in dataJSON[1]:

					try:

						# if received data is about trades
						if dataJSON[0] == 422:
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON[0] == 432:
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						if dataJSON[0] == 431:
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred", data)
						time.sleep(1)

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")
			time.sleep(1)


asyncio.run(main())
