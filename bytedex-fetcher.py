import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://apiv2.bytedex.io/config'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://apiv2.bytedex.io/streams'


for element in currencies["trade_setting"]:
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
	for pair in currencies["trade_setting"]:
		pair_data = '@MD ' + pair["symbol"] + ' spot ' + \
					pair["base"] + ' ' + pair["quote"] + \
					' -1' + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


async def subscribe(ws):

	for i in range(len(list_currencies)):

		if os.getenv("SKIP_ORDERBOOKS") == None:  # don't subscribe or report orderbook changes
			# create the subscription for orderbooks and updates
			await ws.send(json.dumps({
				"method":"subscribe",
				"channels": [
					f"books-delta.{list_currencies[i]}"
				]
			}))

		# create the subscription for trades
		await ws.send(json.dumps({
			"method": "subscribe",
			"channels": [
				f"trades.{list_currencies[i]}"
			]
		}))


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var, start_time):
	trade_data = var
	elapsed_time = time.time() - start_time
	if len(trade_data["data"]) != 0 and elapsed_time > 3:
		for elem in trade_data["data"]:
			print('!', get_unix_time(), trade_data['channel'].split(".")[1],
				  "B" if elem[3] == 1 else "S", str("{:.8f}".format(elem[1])),
				  str(elem[2]), flush=True)
			symbol_trade_count_for_5_minutes[trade_data['channel'].split(".")[1]] += 1


def get_order_books(var, update):
	order_data = var
	if "snapshot" in order_data["data"]:
		if len(order_data['data']['snapshot'][0]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['channel'].split(".")[1]] += len(order_data['data']['snapshot'][0])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['channel'].split(".")[1] + ' B '
			pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data['data']['snapshot'][0])
			answer = order_answer + pq
			print(answer + " R")

		if len(order_data['data']['snapshot'][1]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['channel'].split(".")[1]] += len(order_data['data']['snapshot'][1])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['channel'].split(".")[1] + ' S '
			pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data['data']['snapshot'][1])
			answer = order_answer + pq
			print(answer + " R")

	elif "updates" in order_data["data"]:
		if len(order_data['data']['updates'][0]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['channel'].split(".")[1]] += len(order_data['data']['updates'][0])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['channel'].split(".")[1] + ' B '
			pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data['data']['updates'][0])
			answer = order_answer + pq
			print(answer)

		if len(order_data['data']['updates'][1]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['channel'].split(".")[1]] += len(order_data['data']['updates'][1])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['channel'].split(".")[1] + ' S '
			pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data['data']['updates'][1])
			answer = order_answer + pq
			print(answer)


#trade and orderbook stats output
async def print_stats():
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	if time_to_wait != 300:
		await asyncio.sleep(time_to_wait)
	while True:
		data1 = "# LOG:CAT=trades_stats:MSG= "
		data2 = " ".join(
			key.upper() + ":" + str(value) for key, value in symbol_trade_count_for_5_minutes.items() if value != 0)
		sys.stdout.write(data1 + data2)
		sys.stdout.write("\n")
		for key in symbol_trade_count_for_5_minutes:
			symbol_trade_count_for_5_minutes[key] = 0

		data3 = "# LOG:CAT=orderbooks_stats:MSG= "
		data4 = " ".join(
			key.upper() + ":" + str(value) for key, value in symbol_orderbook_count_for_5_minutes.items() if
			value != 0)
		sys.stdout.write(data3 + data4)
		sys.stdout.write("\n")
		for key in symbol_orderbook_count_for_5_minutes:
			symbol_orderbook_count_for_5_minutes[key] = 0
		await asyncio.sleep(300)

async def main():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats())
	# create connection with server via base ws url
	while True:
		async for ws in websockets.connect(WS_URL, ping_interval=None):
			try:
				start_time = time.time()

				# create task to subscribe to symbols` pair
				subscription = asyncio.create_task(subscribe(ws))

				while True:
					try:
						data = await ws.recv()

						dataJSON = json.loads(data)


						if dataJSON["method"] == "stream":

							# if received data is about trades
							if dataJSON["channel"].split(".")[0] == "trades":
								get_trades(dataJSON, start_time)

							# if received data is about orderbook snapshots
							if dataJSON["channel"].split(".")[0] == "books-delta" and "snapshot" in dataJSON["data"]:
								get_order_books(dataJSON, update=False)

							# if received data is about orderbook updates
							if dataJSON["channel"].split(".")[0] == "books-delta" and "updates" in dataJSON["data"]:
								get_order_books(dataJSON, update=True)

							else:
								pass

					except Exception as ex:
						print(f"Exception {ex} occurred")


			except websockets.exceptions.ConnectionClosedError:
				print("WebSocket connection closed. Reconnecting...")
				await asyncio.sleep(10)

			except Exception as conn_ex:
				print(f"Connection exception {conn_ex} occurred")
				await asyncio.sleep(10)


asyncio.run(main())
