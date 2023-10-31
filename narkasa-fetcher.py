import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://api.narkasa.com/v3/api/market/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.narkasa.com/v3'

for element in currencies["markets"]:
	list_currencies.append(element["symbol"])

#for trades count stats
symbol_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_count_for_5_minutes[list_currencies[i]] = 0

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["markets"]:
		pair_data = '@MD ' + pair["firstSymbol"] + pair["secondSymbol"] + ' spot ' + \
					pair["firstSymbol"] + ' ' + pair["secondSymbol"] + \
					' -1' + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if 'type' in trade_data:
		print('!', get_unix_time(), trade_data['symbol'],
			"B" if trade_data["tradeType"] == 0 else "S", str(trade_data['price']),
			str(trade_data["amount"]), flush=True)
		symbol_count_for_5_minutes[trade_data['symbol']] += 1


def get_order_books(var):
	order_data = var
	if 'bids' in order_data['depth'] and len(order_data["depth"]["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'] + ' S '
		pq = "|".join(str(el["amount"]) + "@" + str(el["price"]) for el in order_data["depth"]["bids"])
		answer = order_answer + pq
		print(answer + " R")

	if 'asks' in order_data['depth'] and len(order_data["depth"]["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'] + ' B '
		pq = "|".join(str(el["amount"]) + "@" + str(el["price"]) for el in order_data["depth"]["asks"])
		answer = order_answer + pq
		print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"data": "ping"
		}))
		await asyncio.sleep(5)


async def main():
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:

			start_time = time.time()
			tradestats_time = start_time

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())

			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"method": "SUBSCRIBE",
					"params": [{
						"type": "trade",
						"symbol": f"{list_currencies[i]}",
						"interval": "m5"
					}]
				}))

				if (os.getenv("SKIP_ORDERBOOKS") == None):
					# create the subscription for full orderbooks and updates
					await ws.send(json.dumps({
						"method": "SUBSCRIBE",
						"params": [{
							"type": "depth",
							"symbol": f"{list_currencies[i]}",
							"interval": "m5"
						}]
					}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if abs(time.time() - tradestats_time) >= 300:
					data1 = "# LOG:CAT=trades_stats:MSG= "
					data2 = " ".join(key.upper() + ":" + str(value) for key, value in symbol_count_for_5_minutes.items() if value != 0)
					sys.stdout.write(data1 + data2)
					sys.stdout.write("\n")
					for key in symbol_count_for_5_minutes:
						symbol_count_for_5_minutes[key] = 0
					tradestats_time = time.time()

				if "type" in dataJSON and "method" not in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['type'] == 'trade':
							get_trades(dataJSON)

						# if received data is about orderbooks
						if dataJSON['type'] == 'depth':
							get_order_books(dataJSON)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
