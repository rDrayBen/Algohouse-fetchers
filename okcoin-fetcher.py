import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://www.okcoin.com/api/v5/public/instruments?instType=SPOT'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://real.okcoin.com:8443/ws/v5/public'

for element in currencies["data"]:
	list_currencies.append(element["instId"])

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
	for pair in currencies["data"]:
		pair_data = '@MD ' + pair["baseCcy"] + '-' + pair["quoteCcy"] + ' spot ' + \
					pair["baseCcy"] + ' ' + pair["quoteCcy"] + \
					' ' + str(str(pair['tickSz'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var, start_time):
	trade_data = var
	elapsed_time = time.time() - start_time
	if 'data' in trade_data and elapsed_time > 2:
		for elem in trade_data["data"]:
			print('!', get_unix_time(), elem['instId'],
				  "B" if elem["side"] == "buy" else "S", elem['px'],
				  elem["sz"], flush=True)
			symbol_trade_count_for_5_minutes[elem['instId']] += 1


def get_order_books(var, update):
	order_data = var

	for i in order_data["data"]:
		if 'asks' in i and len(i["asks"]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['arg']['instId']] += 1
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['arg']['instId'] + ' S '
			pq = "|".join(el[1] + "@" + el[0] for el in i["asks"])
			answer = order_answer + pq
			# checking if the input data is full orderbook or just update
			if (update == True):
				print(answer)
			else:
				print(answer + " R")

		if 'bids' in i and len(i["bids"]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['arg']['instId']] += 1
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['arg']['instId'] + ' B '
			pq = "|".join(el[1] + "@" + el[0] for el in i["bids"])
			answer = order_answer + pq
			# checking if the input data is full orderbook or just update
			if (update == True):
				print(answer)
			else:
				print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"ping"
		}))
		await asyncio.sleep(0.05)


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
					"op": "subscribe",
					"args": [
						{
							"channel": "trades",
							"instId": f"{list_currencies[i]}"
						}
					]
				}))
				if (os.getenv("SKIP_ORDERBOOKS") == None):
					# create the subscription for full orderbooks and updates
					await ws.send(json.dumps({
						"op": "subscribe",
						"args": [
							{
								"channel": "books",
								"instId": f"{list_currencies[i]}"
							}
						]
					}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

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
					for key in symbol_trade_count_for_5_minutes:
						symbol_orderbook_count_for_5_minutes[key] = 0

					tradestats_time = time.time()

				if "event" not in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['arg']['channel'] == 'trades':
							get_trades(dataJSON, start_time)

						# if received data is about updates
						elif dataJSON['arg']['channel'] == 'books' and dataJSON["action"] == "update":
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						elif dataJSON['arg']['channel'] == 'books' and dataJSON["action"] == "snapshot":
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
