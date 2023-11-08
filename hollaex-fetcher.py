import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://api.hollaex.com/v2/constants'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.hollaex.com/stream'

for key, value in currencies["pairs"].items():
	if value["is_public"]==True:
		list_currencies.append(key)

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
	for key, value in currencies["pairs"].items():
		pair_data = '@MD ' + value["pair_base"] + "-" + value["pair_2"] + ' spot ' + \
					value["pair_base"] + ' ' + value["pair_2"] + ' -1 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	for elem in trade_data["data"]:
		print('!', get_unix_time(), trade_data["symbol"],
			"B" if elem["side"] == "buy" else "S", str(elem["price"]),
			 elem["size"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data["symbol"]] += 1


def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data["data"] and len(order_data["data"]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data["symbol"]] += len(order_data["data"]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["symbol"] + ' S '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["data"]["asks"])
		answer = order_answer + pq

		print(answer + " R")

	if 'bids' in order_data["data"] and len(order_data["data"]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data["symbol"]] += len(order_data["data"]["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["symbol"] + ' B '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["data"]["bids"])
		answer = order_answer + pq

		print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
            "op": "ping"
        }))
		await asyncio.sleep(5)

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
								get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
