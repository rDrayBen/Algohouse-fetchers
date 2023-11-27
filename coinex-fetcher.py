import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

# get all available symbol pairs
currency_url = 'https://api.coinex.com/v1/market/info'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://socket.coinex.com/'
is_subscribed_orderbooks = {}
is_subscribed_trades = {}

# check if the certain symbol pair is available
for key, value in currencies["data"].items():
	element = value['name']
	list_currencies.append(element)
	is_subscribed_trades[element] = False
	is_subscribed_orderbooks[element] = False

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i]] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0

async def subscribe(ws, symbol):
	while True:
		if is_subscribed_trades[symbol] == False:
			start_time = time.time()
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

		await asyncio.sleep(0.1)

		if is_subscribed_orderbooks[symbol] == False:
			if os.getenv("SKIP_ORDERBOOKS") == None:  # don't subscribe or report orderbook changes
				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"method": "depth.subscribe",
					"params": [
						f"{symbol}",
						50,
						"0",
						True
					],
					"id": id2
				}))
				id2 += 1

		is_subscribed_trades[symbol] = False
		is_subscribed_orderbooks[symbol] = False

		await asyncio.sleep(2000)

# get metadata about each pair of symbols
async def metadata():
	for key, value in currencies["data"].items():
		pair_data = '@MD ' + value['trading_name'] + value['pricing_name'] + ' spot ' + \
					value['trading_name'] + ' ' + value['pricing_name'] + \
					' ' + str(value['pricing_decimal']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

# put the trade information in output format
def get_trades(var):
	trade_data = var
	if len(trade_data["params"][1]) < 5:
		for element in trade_data["params"][1]:
			print('!', get_unix_time(), trade_data['params'][0],
				  "S" if element["type"] == "sell" else "B", element['price'],
				  element["amount"], flush=True)
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
	while True:
		await ws.send(json.dumps({
			"method": "server.ping",
			"params": [],
			"id": 11
		}))
		await asyncio.sleep(5)

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

			# create task to subscribe to symbols` pair
			subscription = asyncio.create_task(subscribe(ws, symbol))

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			async for data in ws:

				try:
					data = await ws.recv()

					dataJSON = json.loads(data)


					if "method" in dataJSON:

						# if received data is about trades
						if dataJSON['method'] == 'deals.update':
							is_subscribed_trades[dataJSON['params'][0]] = True
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['method'] == 'depth.update' and dataJSON['params'][0] == False:
							is_subscribed_orderbooks[dataJSON['params'][2]] = True
							get_order_books(dataJSON, depth_update=True)

						# if received data is about orderbooks
						if dataJSON['method'] == 'depth.update' and dataJSON['params'][0] == True:
							is_subscribed_orderbooks[dataJSON['params'][2]] = True
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