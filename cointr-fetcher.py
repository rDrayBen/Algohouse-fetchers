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
currency_url = 'https://api.cointr.pro/v1/spot/public/instruments'
WS_URL = 'wss://www.cointr.pro/ws'

args = sys.argv[1:]
if len(args) > 0:
	for arg in args:
		if arg.startswith('-') and arg[1:] == "perpetual":
			MODE = "FUTURES"
			# get all available symbol pairs
			currency_url = 'https://api.cointr.pro/v1/futures/public/instruments'
			WS_URL = 'wss://www.cointr.pro/ws'
			break
else:
	MODE = "SPOT"
	# get all available symbol pairs
	currency_url = 'https://api.cointr.pro/v1/spot/public/instruments'
	WS_URL = 'wss://www.cointr.pro/ws'

answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()

for element in currencies["data"]:
	if MODE == "SPOT":
		list_currencies.append(element["instId"])
	elif MODE == "FUTURES":
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
		if MODE == 'SPOT':
			pair_data = '@MD ' + pair["baseCcy"] + pair["quoteCcy"] + ' spot ' + \
						pair["baseCcy"] + ' ' + pair["quoteCcy"] + \
						' ' + str(pair["pxPrecision"]) + ' 1 1 0 0'

			print(pair_data, flush=True)
		elif MODE == 'FUTURES':
			pair_data = '@MD ' + pair["baseCcy"] + pair["quoteCcy"] + ' perpetual ' + \
						pair["baseCcy"] + ' ' + pair["quoteCcy"] + \
						' ' + str(pair["pxPrecision"]) + ' 1 1 0 0'

			print(pair_data, flush=True)
	print('@MDEND')


async def subscribe(ws, symbol):
	if MODE == 'SPOT':
		if os.getenv("SKIP_ORDERBOOKS") == None:
			# create the subscription for full orderbooks and updates
			await ws.send(json.dumps({
				"args": [{
					"limit": 30,
					"step": "0.001",
					"instId": f"{symbol}"
				}],
				"channel": "spot_depth",
				"op": "subscribe"
			}))

			await asyncio.sleep(0.001)

		# create the subscription for trades
		await ws.send(json.dumps({
			"args": [{
				"instId": f"{symbol}"
			}],
			"channel": "spot_trade",
			"op": "subscribe"
		}))
	elif MODE == 'FUTURES':
		if os.getenv("SKIP_ORDERBOOKS") == None:
			# create the subscription for full orderbooks and updates
			await ws.send(json.dumps({
				"args": [{
					"limit": 30,
					"step": "0.001",
					"instId": f"{symbol}"
				}],
				"channel": "future_depth",
				"op": "subscribe"
			}))

			await asyncio.sleep(0.001)

		# create the subscription for trades
		await ws.send(json.dumps({
			"args": [{
				"instId": f"{symbol}"
			}],
			"channel": "future_trade",
			"op": "subscribe"}))

	await asyncio.sleep(300)


def get_trades(var):
	trade_data = var
	for elem in trade_data["data"]:
		print('!', get_unix_time(), trade_data["instId"],
			  "B" if elem["side"] == "BUY" else "S", elem['px'],
			  elem["sz"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data["instId"]] += 1


def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data['data'] and len(order_data["data"]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['instId']] += len(order_data["data"]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['instId'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data['data'] and len(order_data["data"]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['instId']] += len(order_data["data"]["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['instId'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["bids"])
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
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:
				try:
					data = await ws.recv()

					dataJSON = json.loads(data)

					if "channel" in dataJSON and "event" not in dataJSON:

						# if received data is about trades
						if dataJSON['channel'] == 'spot_trade' and dataJSON['action'] == "update":
							get_trades(dataJSON)
						if dataJSON['channel'] == 'future_trade' and dataJSON['action'] == "update":
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['channel'] == 'spot_depth' and dataJSON['action'] == "update":
							get_order_books(dataJSON, update=True)
						if dataJSON['channel'] == 'future_depth' and dataJSON['action'] == "update":
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						if dataJSON['channel'] == 'spot_depth' and dataJSON['action'] == "snapshot":
							get_order_books(dataJSON, update=False)
						if dataJSON['channel'] == 'future_depth' and dataJSON['action'] == "snapshot":
							get_order_books(dataJSON, update=False)

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
