import json
import requests
import websockets
import time
import asyncio
import os
import sys

# get all available symbol pairs
currency_url = 'https://sapi.xt.com/v4/public/symbol'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://stream.xt.com/public'
is_subscribed_orderbooks = {}
is_subscribed_trades = {}

# check if the certain symbol pair is available
for element in currencies["result"]["symbols"]:
	list_currencies.append(element["symbol"])
	is_subscribed_trades[element["symbol"]] = False
	is_subscribed_orderbooks[element["symbol"]] = False

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
		pair_data = '@MD ' + pair["baseCurrency"].upper() + '_' + pair["quoteCurrency"].upper() + ' spot ' + \
					pair["baseCurrency"].upper() + ' ' + pair["quoteCurrency"].upper() + \
					' ' + str(pair['pricePrecision']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


async def subscribe(ws):
	while True:
		for key, value in is_subscribed_trades.items():

			if value == False:

				# create the subscription for trades
				await ws.send(json.dumps({
					"method": "subscribe",
					"params": [
						f"trade@{key}"
					],
					"id": "1"
				}))

				if is_subscribed_orderbooks[key] == False:
					await ws.send(json.dumps({
						"method": "subscribe",
						"params": [
							f"depth@{key},50"
						],
						"id": "2"
					}))

					# create the subscription for updates
					await ws.send(json.dumps({
						"method": "subscribe",
						"params": [
							f"depth_update@{key}"
						],
						"id": "3"
					}))

					await asyncio.sleep(0.1)
		for el in list(is_subscribed_trades):
			is_subscribed_trades[el] = False

		for el in list(is_subscribed_orderbooks):
			is_subscribed_orderbooks[el] = False

		await asyncio.sleep(2000)


# get time in unix format
def get_unix_time():
	return round(time.time() * 1000)


# put the trade information in output format
def get_trades(var):
	trade_data = var
	print('!', get_unix_time(), trade_data['data']['s'].upper(),
		  "B" if trade_data['data']["b"] else "S", trade_data['data']['p'],
		  trade_data['data']["q"], flush=True)
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
		await ws.send(json.dumps({
			"event": "ping"
		}))
		await asyncio.sleep(5)


async def main():
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			start_time = time.time()
			tradestats_time = start_time
			# create task to subscribe to symbols` pair
			subscription = asyncio.create_task(subscribe(ws))

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())

			print(meta_data)

			while True:

				data = await ws.recv()

				dataJSON = json.loads(data)

				#trade and orderbook stats output
				if abs(time.time() - tradestats_time) >= 300:
					data1 = "# LOG:CAT=trades_stats:MSG= "
					data2 = " ".join(key.upper() + ":" + str(value) for key, value in symbol_trade_count_for_5_minutes.items() if value != 0)
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
					for key in symbol_trade_count_for_5_minutes:
						symbol_orderbook_count_for_5_minutes[key] = 0


					tradestats_time = time.time()

				if "topic" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['topic'] == 'trade':
							is_subscribed_trades[dataJSON['data']['s']] = True
							get_trades(dataJSON)

						# if received data is about updates
						elif dataJSON['topic'] == 'depth_update':
							is_subscribed_orderbooks[dataJSON['data']['s']] = True
							get_order_books(dataJSON, depth_update=True)

						# if received data is about orderbooks
						elif dataJSON['topic'] == 'depth':
							is_subscribed_orderbooks[dataJSON['data']['s']] = True
							get_order_books(dataJSON, depth_update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
