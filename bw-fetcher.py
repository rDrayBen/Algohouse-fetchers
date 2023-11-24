import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

currency_url = 'https://api.bw6.com/data/v1/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://kline.bw.com/websocket'

for key, value in currencies.items():
	parts = key.split("_")
	currency1 = parts[0]
	currency2 = parts[1]
	list_currencies.append(currency1+currency2)

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
	for key, value in currencies.items():
		pair_data = '@MD ' + key.split("_")[0].upper() + key.split("_")[1].upper() + ' spot ' + \
					key.split("_")[0].upper() + ' ' + key.split("_")[1].upper() + \
					' ' + str(value["priceScale"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

def get_trades(var):
	trade_data = var
	if 'data' in trade_data and len(trade_data["data"]) == 1:
		print('!', get_unix_time(), trade_data["channel"].split("_")[0].upper(),
				"B" if trade_data["data"][0]["type"] == "buy" else "S", trade_data['data'][0]['price'],
				 trade_data['data'][0]["amount"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data["channel"].split("_")[0].upper()] += 1

def get_order_books(var):
	order_data = var

	if 'listDown' in order_data and len(order_data["listDown"]) != 0:
		symbol_orderbook_count_for_5_minutes[((order_data["channel"].split("_"))[3].split("default")[0]).upper()] += len(order_data["listDown"])
		order_answer = '$ ' + str(get_unix_time()) + " " + ((order_data["channel"].split("_"))[3].split("default")[0]).upper() + ' S '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["listDown"])
		answer = order_answer + pq

		print(answer + " R")

	if 'listUp' in order_data and len(order_data["listUp"]) != 0:
		symbol_orderbook_count_for_5_minutes[((order_data["channel"].split("_"))[3].split("default")[0]).upper()] += len(order_data["listDown"])
		order_answer = '$ ' + str(get_unix_time()) + " " + ((order_data["channel"].split("_"))[3].split("default")[0]).upper() + ' B '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["listUp"])
		answer = order_answer + pq

		print(answer + " R")

async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"channel": "ping",
			"event": "addChannel",
			"binary": False,
			"isZip": False
			}))
		await asyncio.sleep(3)

# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	if time_to_wait != 300:
		await asyncio.sleep(time_to_wait)
	while True:
		stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
		await asyncio.sleep(300)

async def main():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes))
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			for i in range(len(list_currencies)):
				#create the subscription for historical trades
				await ws.send(json.dumps({
					"event": "addChannel",
					"channel": f"{list_currencies[i]}_lasttrades",
					"binary": False,
					"isZip": False
				}))
				if (os.getenv("SKIP_ORDERBOOKS") == None):
					# create the subscription for full orderbooks
					await ws.send(json.dumps({
						"event": "addChannel",
						"channel": f"dish_depth_00001_{list_currencies[i]}default",
						"binary": False,
						"isZip": False
					}))

			while True:
				data = await ws.recv()

				if data[0] == "(":
					data = data[2:-2]

				dataJSON = json.loads(data)

				if dataJSON['channel'] == 'pong':
					pass

				elif "data" in dataJSON or "listUp" in dataJSON:
					try:

						if dataJSON['channel']=='pong':
							pass

						#if received data is about historical trades
						elif dataJSON['channel'].split("_")[1] == 'lasttrades':
							get_trades(dataJSON)

						# if received data is about full orderbooks
						elif "_".join(dataJSON['channel'].split("_", 2)[:2]) == 'dish_depth':
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
