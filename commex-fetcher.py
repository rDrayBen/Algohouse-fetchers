import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://www.commex.com/bapi/asset/v2/public/asset-service/product/get-products?includeEtf=true'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://stream.commex.com/stream'

for element in currencies["data"]:
	list_currencies.append(element["s"].lower())

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
	for pair in currencies["data"]:
		pair_data = '@MD ' + pair["b"] + pair["q"] + ' spot ' + \
					pair["b"] + ' ' + pair["q"] + \
					' ' + str(str(pair['ts'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


async def subscribe(ws):


	for i in range(len(list_currencies)):
		# create the subscription for trades + full orderbooks and updates
		await ws.send(json.dumps({
			"method": "SUBSCRIBE",
			"params": [
				f"{list_currencies[i]}@aggTrade",
			],
			"id": 1
		}))

		await asyncio.sleep(0.5)

		if os.getenv("SKIP_ORDERBOOKS") == None:  # don't subscribe or report orderbook changes
			# create the subscription for full orderbooks and updates
			await ws.send(json.dumps({
				"method": "SUBSCRIBE",
				"params": [
					f"{list_currencies[i]}@depth"
				],
				"id": 1
			}))


	await asyncio.sleep(300)


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if 'data' in trade_data:
		print('!', get_unix_time(), trade_data["data"]['s'],
				"B" if trade_data["data"]["m"] == True else "S", trade_data["data"]['p'],
				trade_data["data"]["q"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data["data"]['s']] += 1


def get_order_books(var):
	order_data = var
	if 'a' in order_data['data'] and len(order_data["data"]["a"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['s']] += len(order_data["data"]["a"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['s'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["a"])
		answer = order_answer + pq

		print(answer)

	if 'b' in order_data['data'] and len(order_data["data"]["b"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['s']] += len(order_data["data"]["b"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['s'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["b"])
		answer = order_answer + pq

		print(answer)


async def heartbeat(ws):
	id=2
	while True:
		id+=1
		await ws.send(json.dumps({"method":"GET_PROPERTY","params":["combined"],"id":id}))
		await asyncio.sleep(3)

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

			# create task to subscribe to symbols` pair
			subscription = asyncio.create_task(subscribe(ws))

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "stream" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['data']['e'] == 'aggTrade':
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['data']['e'] == 'depthUpdate':
							get_order_books(dataJSON)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
