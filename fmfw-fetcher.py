import json
import requests
import websockets
import time
import asyncio
import os
import sys
from CommonFunctions.CommonFunctions import get_unix_time, stats

#default values
MODE = "SPOT"
currency_url = 'https://api.fmfw.io/api/2/public/symbol'
WS_URL = 'wss://api.fmfw.io/api/2/ws/public'

args = sys.argv[1:]
if len(args) > 0:
	for arg in args:
		if arg.startswith('-') and arg[1:] == "perpetual":
			MODE = "FUTURES"
			# get all available symbol pairs
			currency_url = 'https://api.fmfw.io/api/3/public/futures/info'
			WS_URL = 'wss://st.fmfw.io/'
			break
else:
	MODE = "SPOT"
	# get all available symbol pairs
	currency_url = 'https://api.fmfw.io/api/2/public/symbol'
	WS_URL = 'wss://api.fmfw.io/api/2/ws/public'

answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
is_subscribed_orderbooks = {}
is_subscribed_trades = {}

if MODE == "SPOT":
	for element in currencies:
		list_currencies.append(element["id"])
		is_subscribed_trades[element["id"]] = False
		is_subscribed_orderbooks[element["id"]] = False
elif MODE == "FUTURES":
	for key, value in currencies.items():
		list_currencies.append(key.split("_")[0])
		is_subscribed_trades[key.split("_")[0]] = False
		is_subscribed_orderbooks[key.split("_")[0]] = False

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
	if MODE == "SPOT":
		for pair in currencies:
			pair_data = '@MD ' + pair["baseCurrency"].upper() + pair["quoteCurrency"].upper() + ' spot ' + \
						pair["baseCurrency"].upper() + ' ' + pair["quoteCurrency"].upper() + \
						' ' + str(str(pair['tickSize'])[::-1].find('.')) + ' 1 1 0 0'

			print(pair_data, flush=True)
	elif MODE == "FUTURES":
		for key, value in currencies.items():
			pair_data = '@MD ' + key.split("_")[0] + ' perpetual ' + \
						key.split("USDT")[0] + " USDT " + '-1 1 1 0 0'

			print(pair_data, flush=True)

	print('@MDEND')


async def subscribe(ws):
	while True:
		if MODE == "SPOT":
			for key, value in is_subscribed_trades.items():

				if value == False:

					# create the subscription for trades
					await ws.send(json.dumps({
						"method": "subscribeTrades",
						"params": {
							"symbol": f"{key}",
							"limit": 100
						},
						"id": 123
					}))

					# resubscribe if orderbook subscription is not active + possibility to not subscribe or report orderbook changes:
					if is_subscribed_orderbooks[key] == False and os.getenv("SKIP_ORDERBOOKS") == None:
						# create the subscription for full orderbooks and updates
						await ws.send(json.dumps({
							"method": "subscribeOrderbook",
							"params": {
								"symbol": f"{key}"
							},
							"id": 123
						}))

						await asyncio.sleep(0.1)

		if MODE == "FUTURES":
			for key, value in is_subscribed_trades.items():

				if value == False:

					# create the subscription for trades
					await ws.send(json.dumps([
						1,
						"trades",
						f"{key}_PERP"
					]))

					# resubscribe if orderbook subscription is not active + possibility to not subscribe or report orderbook changes:
					if is_subscribed_orderbooks[key] == False and os.getenv("SKIP_ORDERBOOKS") == None:
						# create the subscription for full orderbooks and updates
						await ws.send(json.dumps([
							1,
							"orderbook",
							f"{key}_PERP"
						]))

						await asyncio.sleep(0.1)

		for el in list(is_subscribed_trades):
			is_subscribed_trades[el] = False

		for el in list(is_subscribed_orderbooks):
			is_subscribed_orderbooks[el] = False

		await asyncio.sleep(2000)

def get_trades(var):
	trade_data = var
	if MODE == "SPOT":
		if 'data' in trade_data["params"]:
			for elem in trade_data["params"]["data"]:
				print('!', get_unix_time(), trade_data["params"]['symbol'],
				  	"B" if elem["side"] == "buy" else "S", elem['price'],
				  	elem["quantity"], flush=True)
				symbol_trade_count_for_5_minutes[trade_data["params"]['symbol']] += 1
	elif MODE == "FUTURES":
		if len(trade_data[3]) != 0 and len(trade_data[3]) < 3:
			for elem in trade_data[3]:
				print('!', get_unix_time(), trade_data[2].split("_")[0],
				  	"B" if elem["side"] == "buy" else "S", str(elem['price']),
				  	str(elem["quantity"]), flush=True)
				symbol_trade_count_for_5_minutes[trade_data[2].split("_")[0]] += 1
def get_order_books(var, update):
	order_data = var
	if MODE == "SPOT":
		if 'ask' in order_data['params'] and len(order_data["params"]["ask"]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['params']['symbol']] += len(order_data["params"]["ask"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params']['symbol'] + ' S '
			pq = "|".join(str(el["size"]) + "@" + str(el["price"]) for el in order_data["params"]["ask"])
			answer = order_answer + pq
			# checking if the input data is full orderbook or just update
			if (update == True):
				print(answer)
			else:
				print(answer + " R")

		if 'bid' in order_data['params'] and len(order_data["params"]["bid"]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['params']['symbol']] += len(order_data["params"]["bid"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params']['symbol'] + ' B '
			pq = "|".join(str(el["size"]) + "@" + str(el["price"]) for el in order_data["params"]["bid"])
			answer = order_answer + pq
			# checking if the input data is full orderbook or just update
			if (update == True):
				print(answer)
			else:
				print(answer + " R")
	elif MODE == "FUTURES":
		if 'ask' in order_data[3] and len(order_data[3]["ask"]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data[2].split("_")[0]] += len(order_data[3]["ask"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data[2].split("_")[0] + ' S '
			pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data[3]["ask"])
			answer = order_answer + pq

			print(answer + " R")

		if 'bid' in order_data[3] and len(order_data[3]["bid"]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data[2].split("_")[0]] += len(order_data[3]["bid"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data[2].split("_")[0] + ' B '
			pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data[3]["bid"])
			answer = order_answer + pq

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

			# create task to subscribe to symbols` pair
			subscription = asyncio.create_task(subscribe(ws))

			if MODE == "SPOT":
				# create task to keep connection alive
				pong = asyncio.create_task(heartbeat(ws))
			elif MODE == "FUTURES":
				pass

			while True:
				data = await ws.recv()

				if data != "pong":
					dataJSON = json.loads(data)
					if MODE == "SPOT" and "method" in dataJSON:
						try:
							# if received data is about trades
							if dataJSON['method'] == 'updateTrades':
								is_subscribed_trades[dataJSON['params']["symbol"]] = True
								get_trades(dataJSON)

							# if received data is about updates
							if dataJSON['method'] == 'updateOrderbook':
								is_subscribed_orderbooks[dataJSON['params']["symbol"]] = True
								get_order_books(dataJSON, update=True)

							# if received data is about orderbooks
							if dataJSON['method'] == 'snapshotOrderbook':
								is_subscribed_orderbooks[dataJSON['params']["symbol"]] = True
								get_order_books(dataJSON, update=False)

							else:
								pass

						except Exception as ex:
							print(f"Exception {ex} occurred", data)
							time.sleep(1)

					elif MODE == "FUTURES":
						try:
							# if received data is about trades
							if dataJSON[1] == 'trades':
								is_subscribed_trades[dataJSON[2].split("_")[0]] = True
								get_trades(dataJSON)

							# if received data is about orderbooks
							if dataJSON[1] == 'orderbook':
								is_subscribed_orderbooks[dataJSON[2].split("_")[0]] = True
								get_order_books(dataJSON, update=False)

							else:
								pass

						except Exception as ex:
							print(f"Exception {ex} occurred", data)
							time.sleep(1)
					else:
						pass


		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")
			time.sleep(1)


asyncio.run(main())
