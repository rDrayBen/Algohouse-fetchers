import json
import requests
import websockets
import time
import asyncio
from CommonFunctions.CommonFunctions import get_unix_time, stats
import os

currency_url = 'https://quotation.bibvip.com/api/v1/allticker'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
list_currencies_meta = list()
WS_URL = 'wss://ws.bibvip5.com/kline-api/ws'

for elem in currencies["ticker"]:
	list_currencies.append(elem["symbol"].split("_")[0].upper() + elem["symbol"].split("_")[1].upper())
	list_currencies_meta.append(elem["symbol"].upper())

# for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i]] = 0

# for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0

# get metadata about each pair of symbols
async def metadata():
	for pair in list_currencies_meta:
		pair_data = '@MD ' + pair.split("_")[0] + pair.split("_")[1] + ' spot ' + \
					pair.split("_")[0] + ' ' + pair.split("_")[1] + \
					' -1 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

async def subscribe(ws):
	for i in range(len(list_currencies)):
		# create the subscription for trades
		await ws.send(json.dumps({
			"event": "sub",
			"params": {
				"channel": f"market_{list_currencies[i].lower()}_trade_ticker",
				"cb_id": f"{list_currencies[i].lower()}",
				"top": 100,
				"id": "7330d06bf0574236b3bc5cdcb7a58c2f",
				"u": "9600251",
				"t": 1700218962161
			}
		}))

		# resubscribe if orderbook subscription is not active + possibility to not subscribe or report orderbook changes:
		if os.getenv("SKIP_ORDERBOOKS") == None:
			# create the subscription for full orderbooks and updates
			await ws.send(json.dumps({
				"event": "sub",
				"params": {
					"channel": f"market_{list_currencies[i].lower()}_depth_step0_diff",
					"cb_id": f"{list_currencies[i].lower()}",
					"id": "4548deb895204237bdf5a90d3058875d",
					"u": "2532451",
					"t": 1700218962162
				}
			}))

	await asyncio.sleep(2000)

def get_trades(var):
	trade_data = var
	if 'data' in trade_data["tick"]:
		for elem in trade_data["tick"]["data"]:
			print('!', get_unix_time(), trade_data["channel"].split("_")[1].upper(),
				  "B" if elem["side"] == "BUY" else "S", str(elem['price']),
				  str(elem["amount"]), flush=True)
			symbol_trade_count_for_5_minutes[trade_data["channel"].split("_")[1].upper()] += 1

def get_order_books(var):
	order_data = var
	if 'asks' in order_data['tick'] and len(order_data["tick"]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['cb_id'].upper()] += len(order_data["tick"]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['cb_id'].upper() + ' S '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["tick"]["asks"])
		answer = order_answer + pq

		print(answer + " R")

	if 'buys' in order_data['tick'] and len(order_data["tick"]["buys"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['cb_id'].upper()] += len(order_data["tick"]["buys"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['cb_id'].upper() + ' B '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["tick"]["buys"])
		answer = order_answer + pq

		print(answer + " R")

# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	if time_to_wait != 300:
		await asyncio.sleep(time_to_wait)
	while True:
		stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
		await asyncio.sleep(300)

# process the situations when the server awaits "ping" request
async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"pong": f"{get_unix_time()}"
		}))
		await ws.send(json.dumps({
			"pong": f"{get_unix_time()}"
		}))
		await asyncio.sleep(5)

async def main():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes))
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

				if "channel" in dataJSON:

					try:
						# if received data is about trades
						if dataJSON['channel'].split("_")[2] == 'trade':
							get_trades(dataJSON)

						# if received data is about full orderbooks
						if dataJSON['channel'].split("_")[2] == 'depth':
							get_order_books(dataJSON)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
