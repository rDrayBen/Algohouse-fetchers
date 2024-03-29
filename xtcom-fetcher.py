import json
import requests
import websockets
import time
import asyncio
import os
import sys
from CommonFunctions.CommonFunctions import get_unix_time, stats

#default values
MODE = "SPOT"
currency_url = 'https://sapi.xt.com/v4/public/symbol'
WS_URL = 'wss://stream.xt.com/public'

args = sys.argv[1:]
if len(args) > 0:
	for arg in args:
		if arg.startswith('-') and arg[1:] == "perpetual":
			MODE = "FUTURES"
			# get all available symbol pairs
			currency_url = 'https://dapi.xt.com/future/market/v3/public/symbol/list'
			WS_URL = 'wss://fstream.xt.com/ws/market?type=SYMBOL'
			break
else:
	MODE = "SPOT"
	# get all available symbol pairs
	currency_url = 'https://sapi.xt.com/v4/public/symbol'
	WS_URL = 'wss://stream.xt.com/public'

answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()

# check if the certain symbol pair is available
for element in currencies["result"]["symbols"]:
	if MODE == "SPOT" and element["state"] == "ONLINE":
		list_currencies.append(element["symbol"])

	elif MODE == "FUTURES" and element["productType"] == "perpetual":
		list_currencies.append(element["symbol"])

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i]] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0

#get metadata about each pair of symbols
async def metadata():
	for pair in currencies["result"]["symbols"]:
		if MODE == 'SPOT':
			pair_data = '@MD ' + pair["baseCurrency"].upper() + '_' + pair["quoteCurrency"].upper() + ' spot ' + \
					pair["baseCurrency"].upper() + ' ' + pair["quoteCurrency"].upper() + \
					' ' + str(pair['pricePrecision']) + ' 1 1 0 0'

			print(pair_data, flush=True)
		elif MODE == 'FUTURES':
			pair_data = '@MD ' + pair["baseCoin"].upper() + '_' + pair["quoteCoin"].upper() + ' perpetual ' + \
						pair["baseCoin"].upper() + ' ' + pair["quoteCoin"].upper() + \
						' ' + str(pair['pricePrecision']) + ' 1 1 0 0'

			print(pair_data, flush=True)

	print('@MDEND')

async def subscribe(ws, symbol):

	# create the subscription for trades
	await ws.send(json.dumps({
		"method": "subscribe",
		"params": [
			f"trade@{symbol}"
		],
		"id": "1"
	}))

	await asyncio.sleep(0.1)

	# possibility to not subscribe or report orderbook changes:
	if os.getenv("SKIP_ORDERBOOKS") == None:
		await ws.send(json.dumps({
			"method": "subscribe",
			"params": [
				f"depth@{symbol},50"
			],
			"id": "2"
		}))

		await asyncio.sleep(0.1)

		# create the subscription for updates
		await ws.send(json.dumps({
		"method": "subscribe",
		"params": [
			f"depth_update@{symbol}"
		],
		"id": "3"
		}))

		await asyncio.sleep(0.1)

	await asyncio.sleep(300)


# put the trade information in output format
def get_trades(var):
	trade_data = var
	if MODE == "SPOT":
		print('!', get_unix_time(), trade_data['data']['s'].upper(),
			  "B" if trade_data['data']["b"] else "S", trade_data['data']['p'],
			  trade_data['data']["q"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data['data']['s']] += 1
	elif MODE == "FUTURES":
		print('!', get_unix_time(), trade_data['data']['s'].upper(),
			  "B" if trade_data['data']["m"] == "BID" else "S", trade_data['data']['p'],
			  trade_data['data']["a"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data['data']['s']] += 1

# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	if 'a' in order_data['data'] and len(order_data["data"]["a"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['s']] += len(order_data["data"]["a"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['s'].upper() + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["a"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'b' in order_data['data'] and len(order_data["data"]["b"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['s']] += len(order_data["data"]["b"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['s'].upper() + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["b"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

# process the situations when the server awaits "ping" request
async def heartbeat(ws):
	while True:
		await ws.send(message="ping")
		await asyncio.sleep(10)


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
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))
			# create task to subscribe trades and orderbooks
			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:
				try:
					data = await ws.recv()

					if data != "pong" and "topic" in data:
						dataJSON = json.loads(data)

						# if received data is about trades
						if dataJSON['topic'] == 'trade':
							get_trades(dataJSON)

						# if received data is about updates
						elif dataJSON['topic'] == 'depth_update':
							get_order_books(dataJSON, depth_update=True)

						# if received data is about orderbooks
						elif dataJSON['topic'] == 'depth':
							get_order_books(dataJSON, depth_update=False)

						else:
							pass

				except Exception as ex:
					print(f"Exception {ex} occurred", data)
					time.sleep(1)

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")
			time.sleep(1)

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
