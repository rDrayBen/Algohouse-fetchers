import json
import requests
import websockets
import time
import asyncio
from CommonFunctions.CommonFunctions import get_unix_time, stats
import os

currency_url = 'https://partner.gdac.com/v0.4/public/pairs'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
list_currencies_meta = list()
WS_URL = 'wss://api.gdac.com/socketcluster/'

for elem in currencies:
	list_currencies.append(elem["pair"])

# for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i]] = 0

# for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0

# get metadata about each pair of symbols
async def metadata():
	for elem in currencies:
		pair_data = '@MD ' + elem["pair"] + " " + elem["base_currency"] + " " + elem["quote_currency"] \
					+ ' spot ' + '-1 1 1 0 0'
		print(pair_data, flush=True)
	print('@MDEND')

async def subscribe(ws):
	await ws.send(json.dumps({"event": "#handshake", "data": {"authToken": 0}, "cid": 1}))

	for i in range(len(list_currencies)):
		# create the subscription for trades
		await ws.send(json.dumps({
			"event": "#subscribe",
			"data": {
				"channel": f"trades:{list_currencies[i]}"
			},
			"cid": 5
		}))

		# resubscribe if orderbook subscription is not active + possibility to not subscribe or report orderbook changes:
		if os.getenv("SKIP_ORDERBOOKS") == None:
			# create the subscription for full orderbooks and updates
			await ws.send(json.dumps({
				"event": "#subscribe",
				"data": {
					"channel": f"orderbook:{list_currencies[i]}"
				},
				"cid": 3
			}))

	await asyncio.sleep(2000)

def get_trades(var):
	trade_data = var
	if 'data' in trade_data["data"] and len(trade_data["data"]["data"]) < 3:
		for elem in trade_data["data"]["data"]:
			print('!', get_unix_time(), elem["pair"],
				  elem["side"], elem['price'],
				  elem["quantity"], flush=True)
			symbol_trade_count_for_5_minutes[elem["pair"]] += 1

def get_order_books(var):
	order_data = var
	if order_data['data']['data']['side'][0] == 'B' and len(order_data['data']['data']) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['data']['pair'][0]] += len(order_data['data']['data'])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['data']['pair'][0] + ' B '
		pq = order_data['data']['data']['volume'][0] + "@" + order_data['data']['data']['price'][0]
		answer = order_answer + pq

		print(answer + " R")

	if order_data['data']['data']['side'][0] == 'S' and len(order_data['data']['data']) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['data']['pair'][0]] += len(order_data['data']['data'])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['data']['pair'][0] + ' S '
		pq = order_data['data']['data']['volume'][0] + "@" + order_data['data']['data']['price'][0]
		answer = order_answer + pq

		print(answer + " R")

# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	if time_to_wait != 300:
		await asyncio.sleep(time_to_wait)
	while True:
		stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
		await asyncio.sleep(300)

# process the situations when the server awaits "ping" request
async def heartbeat(ws):
	while True:
		await ws.send("#2")
		await asyncio.sleep(5)

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

				if data == "#1":
					pass

				else:
					dataJSON = json.loads(data)

					if "event" in dataJSON and dataJSON["event"] == '#publish':
						try:

							# if received data is about trades
							if 'trades' in dataJSON["data"]["channel"]:
								get_trades(dataJSON)

							# if received data is about full orderbooks
							if 'orderbook' in dataJSON["data"]["channel"]:
								get_order_books(dataJSON)

							else:
								pass

						except Exception as ex:
							print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
