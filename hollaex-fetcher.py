import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

currency_url = 'https://api.hollaex.com/v2/constants'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.hollaex.com/stream'

for key, value in currencies["pairs"].items():
	if value["is_public"]==True:
		list_currencies.append(key.upper())

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i].upper()] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i].upper()] = 0

# get metadata about each pair of symbols
async def metadata():
	for key, value in currencies["pairs"].items():
		pair_data = '@MD ' + value["pair_base"].upper() + "-" + value["pair_2"].upper() + ' spot ' + \
					value["pair_base"].upper() + ' ' + value["pair_2"].upper() + ' -1 1 1 0 0'
		print(pair_data, flush=True)
	print('@MDEND')

def get_trades(var):
	trade_data = var
	for elem in trade_data["data"]:
		print('!', get_unix_time(), trade_data["symbol"].upper(),
			"B" if elem["side"] == "buy" else "S", str(elem["price"]),
			 elem["size"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data["symbol"].upper()] += 1

def get_order_books(var):
	order_data = var
	if 'asks' in order_data["data"] and len(order_data["data"]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data["symbol"].upper()] += len(order_data["data"]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["symbol"].upper() + ' S '
		pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data["data"]["asks"])
		answer = order_answer + pq

		print(answer + " R")

	if 'bids' in order_data["data"] and len(order_data["data"]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data["symbol"].upper()] += len(order_data["data"]["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["symbol"].upper() + ' B '
		pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data["data"]["bids"])
		answer = order_answer + pq

		print(answer + " R")

async def heartbeat(ws):
	while True:
		await ws.send("ping")
		await asyncio.sleep(30)

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
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create the subscription for full orderbooks, updates and trades
			await ws.send(json.dumps({
    			"op": "subscribe",
				"args": ["orderbook", "trade"]
			}))

			while True:
				data = await ws.recv()
				dataJSON = json.loads(data)

				if "topic" in dataJSON:
					try:
						# if received data is about trades
						if dataJSON["topic"] == "trade" and dataJSON["action"] == "insert":
							get_trades(dataJSON)

						if os.getenv("SKIP_ORDERBOOKS") == None:  # don't subscribe or report orderbook changes
							# if received data is about updates and full orderbooks
							if dataJSON["topic"] == "orderbook" and dataJSON["action"] == "partial":
								get_order_books(dataJSON)
						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")

asyncio.run(main())
