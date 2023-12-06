import json
import requests
import websockets
import time
import asyncio
from CommonFunctions.CommonFunctions import get_unix_time, stats
import os
import sys

#default values
MODE = "SPOT"
currency_url = 'https://api.fairdesk.com/api/v1/public/spot/pairs'
WS_URL = 'wss://www.fairdesk.com/ws?token=web.361414.5E4FCAB8020E5E94ED6DF56EB1D128AD'

args = sys.argv[1:]
if len(args) > 0:
	for arg in args:
		if arg.startswith('-') and arg[1:] == "perpetual":
			MODE = "FUTURES"
			# get all available symbol pairs
			currency_url = 'https://api.fairdesk.com/api/v1/public/products'
			WS_URL = 'wss://www.fairdesk.com/ws?token=web.361414.4619BA2207420942988E6D82BC038CEE'
			break
else:
	MODE = "SPOT"
	# get all available symbol pairs
	currency_url = 'https://api.fairdesk.com/api/v1/public/spot/pairs'
	WS_URL = 'wss://www.fairdesk.com/ws?token=web.361414.5E4FCAB8020E5E94ED6DF56EB1D128AD'
	answer = requests.get(currency_url)
	currencies = answer.json()
	list_currencies = list()

answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()

if MODE == "SPOT":
	for element in currencies["result"]:
		list_currencies.append(element["base"] + element["target"])
elif MODE == "FUTURES":
	for element in currencies["data"]:
		list_currencies.append(element["symbol"].upper())

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
	if MODE == "SPOT":
		for elem in currencies["result"]:
			pair_data = '@MD ' + elem["base"] + elem["target"] + ' spot ' + \
						elem["base"] + ' ' + elem["target"] + ' -1 1 1 0 0'

			print(pair_data, flush=True)

	elif MODE == "FUTURES":
		for elem in currencies["data"]:
			pair_data = '@MD ' + elem["symbol"].upper() + ' perpetual ' + \
						elem["baseCurrency"] + ' ' + elem["quoteCurrency"] + ' -1 1 1 0 0'

			print(pair_data, flush=True)

	print('@MDEND')

def get_trades(var, start_time):
	trade_data = var
	elapsed_time = time.time() - start_time
	if elapsed_time > 5:
		print('!', get_unix_time(), trade_data['s'].upper(),
			  'B' if trade_data['m'] else 'S', trade_data['p'],
			  trade_data["q"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data['s'].upper()] += 1

def get_order_books(var):
	order_data = var
	if 'a' in order_data and len(order_data["a"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['s'].upper()] += len(order_data["a"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['s'].upper() + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["a"])
		answer = order_answer + pq

		print(answer + " R")

	if 'b' in order_data and len(order_data["b"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['s'].upper()] += len(order_data["b"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['s'].upper() + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["b"])
		answer = order_answer + pq

		print(answer + " R")

async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"event": "ping"
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

async def main():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes))
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			start_time = time.time()

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			for i in range(len(list_currencies)):
				if MODE == "SPOT":
					# create the subscription for trades
					await ws.send(json.dumps({
						"method": "SUBSCRIBE",
						"params": [
							"web.361414.5E4FCAB8020E5E94ED6DF56EB1D128AD",
							f"{list_currencies[i].lower()}@spotTrade"
					]}))

					if os.getenv("SKIP_ORDERBOOKS") == None:
						# create the subscription for full orderbooks
						await ws.send(json.dumps({
							"method": "SUBSCRIBE",
							"params": [
								"web.361414.5E4FCAB8020E5E94ED6DF56EB1D128AD",
								f"{list_currencies[i].lower()}@spotDepth100"
							]}))
				elif MODE == "FUTURES":
					# create the subscription for trades
					await ws.send(json.dumps({
						"method": "SUBSCRIBE",
						"params": [
							"web.361414.02E4B9508ACD32DC4ACE1E0DAD7E8837",
							f"{list_currencies[i].lower()}@aggTrade"
					]}))

					if os.getenv("SKIP_ORDERBOOKS") == None:
						# create the subscription for full orderbooks
						await ws.send(json.dumps({
						"method": "SUBSCRIBE",
						"params": [
							"web.361414.02E4B9508ACD32DC4ACE1E0DAD7E8837",
							f"{list_currencies[i].lower()}@depth50"
					]}))

			while True:
				data = await ws.recv()
				dataJSON = json.loads(data)

				if "e" in dataJSON:

					try:
						# if received data is about trades
						if dataJSON['e'] == 'trade':
							get_trades(dataJSON, start_time)

						if dataJSON['e'] == 'aggTrade':
							get_trades(dataJSON, start_time)

						# if received data is about updates and full orderbooks
						if dataJSON['e'] == 'depthUpdate':
							get_order_books(dataJSON)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred", data)
						time.sleep(1)

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")
			time.sleep(1)


asyncio.run(main())
