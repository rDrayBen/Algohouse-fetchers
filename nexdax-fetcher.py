import json
import requests
import websockets
import time
import asyncio
import re
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

currency_url = 'https://nexdax.com/exchange/getProducts'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://socket.nexdax.com/socket.io/?EIO=3&transport=websocket'

for element in currencies["data"]:
	list_currencies.append(element["symbol"].lower())


# for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i].upper()] = 0

# for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i].upper()] = 0

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["data"]:
		pair_data = '@MD ' + pair["symbol"] + ' spot ' + \
					pair["baseAsset"] + ' ' + pair["quoteAsset"] + \
					' ' + str(str(pair['tickPrice'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

def get_trades(var):
	trade_data = var
	if 'event' in trade_data:
		print('!', get_unix_time(), trade_data["symbol"],
				"B" if trade_data["isBuyerMaker"] == True else "S", str("{:.8f}".format(int(trade_data["price"]))),
				str(trade_data["qty"]), flush=True)
		symbol_trade_count_for_5_minutes[trade_data["symbol"]] += 1

def get_order_books(var):
	order_data = var
	if 'a' in order_data and len(order_data["a"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['s']] += len(order_data["a"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['s'] + ' S '
		pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data["a"])
		answer = order_answer + pq

		print(answer + " R")

	if 'b' in order_data and len(order_data["b"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['s']] += len(order_data["b"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['s'] + ' B '
		pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data["b"])
		answer = order_answer + pq

		print(answer + "R")

async def heartbeat(ws):
	while True:
		await ws.send(message='2')
		await asyncio.sleep(25)

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
	stats_task = asyncio.create_task(
		print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes))
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			for i in range(len(list_currencies)):
				# create the subscriptions for trades + full orderbooks and updates
				await ws.send(message=f'40/{list_currencies[i]}_aggTrade,')
				# resubscribe if orderbook subscription is not active + possibility to not subscribe or report orderbook changes:
				if os.getenv("SKIP_ORDERBOOKS") == None:
					await ws.send(message=f'40/{list_currencies[i]},')
				await asyncio.sleep(0.1)


			while True:
				data = await ws.recv()

				if data[0] != "0" and data != "3":
					if data[0] == "4" and data[1] == "2":

						pattern = r'\["message",(\{.*\})\]'
						match = re.search(pattern, data)

						if match:
							dataJSON = json.loads(match.group(1))

							if "e" in dataJSON:

								try:
									# if received data is about updates
									if dataJSON['e'] == 'depthUpdate':
										get_order_books(dataJSON)

									else:
										pass

								except Exception as ex:
									print(f"Exception {ex} occurred")

							elif "event" in dataJSON:
								try:

									# if received data is about trades
									if dataJSON['event'] == 'aggTrade':
										get_trades(dataJSON)

									else:
										pass

								except Exception as ex:
									print(f"Exception {ex} occurred")

						else:
							pass
		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
