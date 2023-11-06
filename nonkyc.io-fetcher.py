import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://nonkyc.io/api/v2/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://ws.nonkyc.io'
is_subscribed_orderbooks = {}
is_subscribed_trades = {}

for element in currencies:
	if element["type"] == "spot":
		list_currencies.append(element["base"] + "/" + element["quote"])
		is_subscribed_trades[element["base"] + "/" + element["quote"]] = False
		is_subscribed_orderbooks[element["base"] + "/" + element["quote"]] = False

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
	for pair in currencies:
		pair_data = '@MD ' + pair["base"] + '/' + pair["quote"] + ' spot ' + \
					pair["base"] + ' ' + pair["quote"] + \
					' -1' + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


async def subscribe(ws):
	while True:
		for key, value in is_subscribed_trades.items():

			if value == False:

				# create the subscription for trades
				await ws.send(json.dumps({
					"method": "subscribeTrades",
					"params": {
						"symbol": f"{list_currencies[i]}"
					}
				}))

				#resubscribe if orderbook subscription is not active + possibility to not subscribe or report orderbook changes:
				if is_subscribed_orderbooks[key] == False and os.getenv("SKIP_ORDERBOOKS") == None:
					# create the subscription for full orderbooks and updates
					await ws.send(json.dumps({
						"method": "subscribeOrderbook",
						"params": {
							"symbol": f"{list_currencies[i]}",
							"limit": 100
						},
						"id": 123
					}))

					await asyncio.sleep(0.1)
		for el in list(is_subscribed_trades):
			is_subscribed_trades[el] = False

		for el in list(is_subscribed_orderbooks):
			is_subscribed_orderbooks[el] = False

		await asyncio.sleep(2000)


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if 'data' in trade_data["params"]:
		for elem in trade_data["params"]["data"]:
			print('!', get_unix_time(), trade_data["params"]['symbol'],
				  "B" if elem["side"] == "buy" else "S", elem['price'],
				  elem["quantity"], flush=True)
			symbol_trade_count_for_5_minutes[trade_data["params"]['symbol']] += 1


def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data['params'] and len(order_data["params"]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['params']['symbol']] += len(order_data["params"]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params']['symbol'] + ' S '
		pq = "|".join(str(el["quantity"]) + "@" + el["price"] for el in order_data["params"]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data['params'] and len(order_data["params"]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['params']['symbol']] += len(order_data["params"]["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params']['symbol'] + ' B '
		pq = "|".join(str(el["quantity"]) + "@" + el["price"] for el in order_data["params"]["bids"])
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


async def main():
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:

			start_time = time.time()
			tradestats_time = start_time

			# create task to subscribe to symbols` pair
			subscription = asyncio.create_task(subscribe(ws))

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())



			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				# trade and orderbook stats output
				if abs(time.time() - tradestats_time) >= 300:
					data1 = "# LOG:CAT=trades_stats:MSG= "
					data2 = " ".join(
						key.upper() + ":" + str(value) for key, value in symbol_trade_count_for_5_minutes.items() if
						value != 0)
					sys.stdout.write(data1 + data2)
					sys.stdout.write("\n")
					for key in symbol_trade_count_for_5_minutes:
						symbol_trade_count_for_5_minutes[key] = 0

					data3 = "# LOG:CAT=orderbooks_stats:MSG= "
					data4 = " ".join(
						key.upper() + ":" + str(value) for key, value in
						symbol_orderbook_count_for_5_minutes.items() if
						value != 0)
					sys.stdout.write(data3 + data4)
					sys.stdout.write("\n")
					for key in symbol_orderbook_count_for_5_minutes:
						symbol_orderbook_count_for_5_minutes[key] = 0

					tradestats_time = time.time()

				if "method" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['method'] == 'updateTrades':
							is_subscribed_trades[dataJSON['params']['symbol']] = True
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['method'] == 'updateOrderbook':
							is_subscribed_orderbooks[dataJSON['params']['symbol']] = True
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						if dataJSON['method'] == 'snapshotOrderbook':
							is_subscribed_orderbooks[dataJSON['params']["symbol"]] = True
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
