import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://api.zondacrypto.exchange/rest/trading/ticker'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.zondacrypto.exchange/websocket/'
is_subscribed_orderbooks = {}
is_subscribed_trades = {}

for key, value in currencies["items"].items():
	list_currencies.append(key.lower())
	is_subscribed_trades[key.lower()] = False
	is_subscribed_orderbooks[key.lower()] = False

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
	for key, value in currencies["items"].items():
		pair_data = '@MD ' + value["market"]["first"]["currency"] + '-' + value["market"]["second"]["currency"] + ' spot ' + \
					value["market"]["first"]["currency"] + ' ' + value["market"]["second"]["currency"] + \
					' ' + str(value['market']['pricePrecision']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


async def subscribe(ws):
	while True:
		for key, value in is_subscribed_trades.items():

			if value == False:

				# create the subscription for trades
				await ws.send(json.dumps({
					"action": "subscribe-public",
					"module": "trading",
					"path": f"transactions/{key}"
				}))

				await asyncio.sleep(0.01)

		for key, value in is_subscribed_orderbooks.items():

			# resubscribe if orderbook subscription is not active + possibility to not subscribe or report orderbook changes:
			if value == False and os.getenv("SKIP_ORDERBOOKS") == None:
				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"action": "subscribe-public",
					"module": "trading",
					"path": f"orderbook/{key}"
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
	if 'message' in trade_data:
		parts = trade_data["topic"].split("/")
		symbol = parts[-1].upper()
		is_subscribed_trades[symbol] = True
		for elem in trade_data["message"]["transactions"]:
			print('!', get_unix_time(), symbol,
				  "B" if elem["ty"] == "buy" else "S", elem['r'],
				  elem["a"], flush=True)
			symbol_trade_count_for_5_minutes[symbol] += 1


def get_order_books(var, update):
	order_data = var
	if order_data['message']['changes'][0]["entryType"] == 'Buy'  and len(order_data["message"]["changes"][0]["state"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['message']['changes'][0]['marketCode']] += len(order_data["message"]["changes"][0]["state"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['message']['changes'][0]['marketCode'] + ' S '
		pq = "|".join(el["state"]["ca"] + "@" + el["state"]["ra"] for el in order_data["message"]["changes"])
		answer = order_answer + pq

		print(answer)


	if order_data['message']['changes'][0]["entryType"] == 'Sell' and len(order_data["message"]["changes"][0]["state"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['message']['changes'][0]['marketCode']] += len(order_data["message"]["changes"][0]["state"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['message']['changes'][0]['marketCode'] + ' B '
		pq = "|".join(el["state"]["ca"] + "@" + el["state"]["ra"] for el in order_data["message"]["changes"])
		answer = order_answer + pq

		print(answer)


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"action": "ping"
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

				if dataJSON["action"] == 'push':

					try:

						# if received data is about trades
						if "transactions" in dataJSON['topic']:
							get_trades(dataJSON)

						# if received data is about updates
						if "orderbook" in dataJSON['topic'] and dataJSON["message"]["changes"][0]["action"] == 'update':
							is_subscribed_orderbooks[dataJSON['message']['changes'][0]['marketCode']] = True
							get_order_books(dataJSON, update=True)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
