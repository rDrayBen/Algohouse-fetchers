import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

# get all available symbol pairs
currency_url = 'https://coinsbit.io/api/v1/public/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = "wss://ws.coinsbit.io/"
# check if the certain symbol pair is available
for element in currencies["result"]:
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
		"id": id1
	}))
	id1 += 1

	await asyncio.sleep(0.01)

	if os.getenv("SKIP_ORDERBOOKS") == None:
		# create the subscription for full orderbooks and updates
		await ws.send(json.dumps({
			"method": "depth.subscribe",
			"params": [
				f"{symbol}",
				100,
				"0"
			],
			"id": id2
		}))
		id2 += 1

	await asyncio.sleep(300)

# get metadata about each pair of symbols
async def metadata():
	for element in currencies["result"]:
		pair_data = '@MD ' + element['stock'] + '_' + element['money'] + ' spot ' + \
					element['stock'] + ' ' + element['money'] + \
					' ' + str(element['moneyPrec']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

# put the trade information in output format
def get_trades(var, start_time):
	trade_data = var
	elapsed_time = time.time() - start_time
	if elapsed_time > 2:
		for elem in trade_data['params'][1]:
			print("!", get_unix_time(), trade_data["params"][0],
				  "S" if elem["type"][0] == "sell" else "B",
				  elem["price"], elem['amount'], end="\n")
			symbol_trade_count_for_5_minutes[trade_data['params'][0]] += 1

# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	if 'asks' in order_data['params'][1] and len(order_data['params'][1]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['params'][2]] += len(order_data['params'][1]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params'][2] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data['params'][1]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data['params'][1] and len(order_data['params'][1]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['params'][2]] += len(order_data['params'][1]["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params'][2] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data['params'][1]["bids"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

# process the situations when the server awaits "ping" request
async def heartbeat(ws):
	id3 = 2000
	while True:
		await ws.send(json.dumps({
			"method": "server.ping",
			"params": [],
			"id": 2000
		}))
		await asyncio.sleep(5)
		id3 += 1

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

async def socket(symbol):
		# create connection with server via base ws url
		async for ws in websockets.connect(WS_URL, ping_interval=None):
			try:
				start_time = time.time()
				# create task to keep connection alive
				pong = asyncio.create_task(heartbeat(ws))
				# create task to subscribe to symbols` pair
				subscription = asyncio.create_task(subscribe(ws, symbol))
				async for data in ws:

					try:
						dataJSON = json.loads(data)

						if "method" in dataJSON:
							# if received data is about trades
							if dataJSON['method'] == 'deals.update':
								get_trades(dataJSON,start_time)

							# if received data is about updates
							if dataJSON['method'] == 'depth.update' and dataJSON['params'][0] == False:
								get_order_books(dataJSON, depth_update=True)

							# if received data is about orderbooks
							if dataJSON['method'] == 'depth.update' and dataJSON['params'][0] == True:
								get_order_books(dataJSON, depth_update=False)

					except Exception as ex:
						print(f"Exception {ex} occurred", data)
						time.sleep(1)

					except:
						pass


			except Exception as conn_ex:
				print(f"Connection exception {conn_ex} occurred")
				time.sleep(1)

			except:
				continue


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
