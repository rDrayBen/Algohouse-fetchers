import json
import requests
import websockets
import time
import asyncio
from CommonFunctions.CommonFunctions import get_unix_time, stats
import os

currency_url = 'https://sapi.coinhub.mn/v1/market/tickers'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://sapi.coinhub.mn/lp/'

for key, value in currencies["data"].items():
	if key.split("/")[1] != "MNT":
		list_currencies.append(key.split("/")[0] + "-" + key.split("/")[1])

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
	for pair in list_currencies:
		pair_data = '@MD ' + pair + ' spot ' + \
					pair.split("-")[0] + ' ' + pair.split("-")[1] + \
					' -1 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

async def subscribe(ws):
	for i in range(len(list_currencies)):
		# create the subscription for trades
		await ws.send(json.dumps({
			"op": "subscribe",
			"args": [{
				"channel": "trades",
				"instId": f"{list_currencies[i]}"
			}]
		}))

		# resubscribe if orderbook subscription is not active + possibility to not subscribe or report orderbook changes:
		if os.getenv("SKIP_ORDERBOOKS") == None:
			# create the subscription for full orderbooks and updates
			await ws.send(json.dumps({
				"op": "subscribe",
				"args": [{
					"channel": "books",
					"instId": f"{list_currencies[i]}"
				}]
			}))

	await asyncio.sleep(2000)

def get_trades(var):
	trade_data = var
	if 'data' in trade_data:
		for elem in trade_data["data"]:
			print('!', get_unix_time(), trade_data["arg"]['instId'],
				  "B" if elem["side"] == "buy" else "S", elem['px'],
				  elem["sz"], flush=True)
			symbol_trade_count_for_5_minutes[trade_data["arg"]['instId']] += 1

def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data['data'][0] and len(order_data["data"][0]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['arg']['instId']] += len(order_data["data"][0]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['arg']['instId'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"][0]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data['data'][0] and len(order_data["data"][0]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['arg']['instId']] += len(order_data["data"][0]["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['arg']['instId'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"][0]["bids"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

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
			# create task to subscribe to symbols` pair
			subscription = asyncio.create_task(subscribe(ws))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "event" in dataJSON:
					pass

				elif "channel" in dataJSON['arg']:

					try:

						# if received data is about trades
						if dataJSON['arg']['channel'] == 'trades':
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['arg']['channel'] == 'books' and dataJSON['action'] == "update":
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						if dataJSON['arg']['channel'] == 'books' and dataJSON['action'] == "snapshot":
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
