import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

currency_url = 'https://back.bitsten.com/v2/market-list'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://back.bitsten.com/ws'

for element in currencies["response"]["marketList"]["result"]:
	list_currencies.append(element["name"])

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i]] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0

async def subscribe(ws, symbol):
	id1 = 1
	id2 = 1000

	# create the subscription for trades
	await ws.send(json.dumps({
		"method": "deals.subscribe",
		"params": [
			f"{symbol}"
		],
		"id": 7
	}))
	id1 += 1

	await asyncio.sleep(0.01)

	if os.getenv("SKIP_ORDERBOOKS") == None:
		# create the subscription for full orderbooks
		await ws.send(json.dumps({
			"method": "depth.subscribe",
			"params": [
				f"{symbol}",
				100,
				"0"
			],
			"id": 9
		}))

		id2 += 1

	await asyncio.sleep(300)

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["response"]["marketList"]["result"]:
		pair_data = '@MD ' + pair["name"] + ' spot ' + \
					pair["stock"] + ' ' + pair["money"] + \
					' ' + str(pair["money_prec"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

def get_trades(var):
	trade_data = var
	if len(trade_data["params"][1]) != 0 and len(trade_data["params"][1]) < 5:
		for elem in trade_data["params"][1]:
			print('!', get_unix_time(), trade_data["params"][0],
					"B" if elem["type"] == "buy" else "S", elem['price'],
					elem["amount"], flush=True)
			symbol_trade_count_for_5_minutes[trade_data["params"][0]] += 1


def get_order_books(var):
	order_data = var
	if 'asks' in order_data['params'][1] and len(order_data["params"][1]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['params'][2]] += len(order_data["params"][1]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params'][2] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["params"][1]["asks"])
		answer = order_answer + pq

		print(answer + " R")

	if 'bids' in order_data['params'][1] and len(order_data["params"][1]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['params'][2]] += len(order_data["params"][1]["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params'][2] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["params"][1]["bids"])
		answer = order_answer + pq

		print(answer + " R")


async def heartbeat(ws):
	id = 0
	while True:
		await ws.send(json.dumps({
			"method": "server.ping",
			"params": [],
			"id": id
		}))
		id += 1
		await asyncio.sleep(25)


# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	if time_to_wait != 300:
		await asyncio.sleep(time_to_wait)
	while True:
		stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
		await asyncio.sleep(300)

async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))
			# create task to subscribe trades and orderbooks
			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:

				try:

					dataJSON = json.loads(data)

					if "method" in dataJSON:

						# if received data is about trades
						if dataJSON['method'] == 'deals.update':
							get_trades(dataJSON)

						# if received data is about orderbooks
						if dataJSON['method'] == 'depth.update':
							get_order_books(dataJSON)


						else:
							pass

				except Exception as ex:
					print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


async def handler():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes))
	tasks=[]
	for symbol in list_currencies:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(1)

	await asyncio.wait(tasks)


async def main():
	await handler()


asyncio.run(main())

