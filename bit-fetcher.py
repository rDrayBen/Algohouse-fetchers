import json
import requests
import websockets
import time
import asyncio
import os
import sys

# get all available symbol pairs
currency_url = 'https://api.bit.com/spot/v1/instruments'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://ws.bit.com'

# check if the certain symbol pair is available
for element in currencies["data"]:
	list_currencies.append(element["pair"])

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
		pair_data = '@MD ' + pair["base_currency"] + '-' + pair["quote_currency"] + ' spot ' + \
					pair["base_currency"] + ' ' + pair["quote_currency"] + \
					' ' + str(str(pair['price_step'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


# get time in unix format
def get_unix_time():
	return round(time.time() * 1000)


# put the trade information in output format
def get_trades(var):
	trade_data = var
	for element in trade_data['data']:
		print('!', get_unix_time(), element['instrument_id'].replace("-PERPETUAL", ""),
			  "B" if element["side"] == "buy" else "S", element['price'],
			  element["qty"], flush=True)
		symbol_trade_count_for_5_minutes[element['instrument_id'].replace("-PERPETUAL", "")] += 1


# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	if (depth_update == False):
		if 'asks' in order_data['data'] and len(order_data["data"]["asks"]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['data']['instrument_id'].replace("-PERPETUAL", "")] += len(order_data["data"]["asks"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['instrument_id'].replace("-PERPETUAL", "") + ' S '
			pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["asks"])
			answer = order_answer + pq
			print(answer + " R")

		if 'bids' in order_data['data'] and len(order_data["data"]["bids"]) != 0:
			symbol_orderbook_count_for_5_minutes[order_data['data']['instrument_id'].replace("-PERPETUAL", "")] += len(order_data["data"]["bids"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['instrument_id'].replace("-PERPETUAL", "") + ' B '
			pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["bids"])
			answer = order_answer + pq
			print(answer + " R")

	if (depth_update == True):
		index_sell = False
		index_buy = False
		if order_data['data']["changes"][0][0] == "sell" and len(order_data["data"]["changes"]) != 0:
			order_answer_S = '$ ' + str(get_unix_time()) + " " + order_data['data']['instrument_id'].replace("-PERPETUAL", "") + ' S '
			order_answer_B = '$ ' + str(get_unix_time()) + " " + order_data['data']['instrument_id'].replace("-PERPETUAL", "") + ' B '
			pq_el_S = []
			pq_el_B = []
			for el in order_data["data"]["changes"]:
				if (el[0] == "sell"):
					index_sell = True
					symbol_orderbook_count_for_5_minutes[order_data['data']['instrument_id'].replace("-PERPETUAL", "")] += 1
					pq_el_S.append(el[2] + "@" + el[1])
				elif (el[0] == "buy"):
					index_buy = True
					symbol_orderbook_count_for_5_minutes[order_data['data']['instrument_id'].replace("-PERPETUAL", "")] += 1
					pq_el_B.append(el[2] + "@" + el[1])
			pq_S = "|".join(pq_el_S)
			pq_B = "|".join(pq_el_B)
			answer_S = order_answer_S + pq_S
			answer_B = order_answer_B + pq_B
			if index_sell == True:
				print(answer_S)
			if index_buy == True:
				print(answer_B)


# process the situations when the server awaits "ping" request
async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"event": "ping"
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

			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"type": "subscribe",
					"instruments": [
						f"{list_currencies[i]}" + "-PERPETUAL"
					],
					"channels": [
						"trade"
					],
					"interval": "100ms"
				}))
				if os.getenv("SKIP_ORDERBOOKS") == None:
					# create the subscription for full orderbooks and updates
					await ws.send(json.dumps({
						"type": "subscribe",
						"instruments": [
							f"{list_currencies[i]}" + "-PERPETUAL"
						],
						"channels": [
							"depth"
						],
						"interval": "100ms"
					}))

			while True:

				data = await ws.recv()

				dataJSON = json.loads(data)

				if "channel" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['channel'] == 'trade':
							get_trades(dataJSON)

						# if received data is about updates
						elif dataJSON['channel'] == 'depth' and dataJSON["data"]["type"] == "update":
							get_order_books(dataJSON, depth_update=True)

						# if received data is about orderbooks
						elif dataJSON['channel'] == 'depth' and dataJSON["data"]["type"] == "snapshot":
							get_order_books(dataJSON, depth_update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
