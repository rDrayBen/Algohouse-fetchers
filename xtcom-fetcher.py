import json
import requests
import websockets
import time
import asyncio
import os
import sys

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
is_subscribed_orderbooks = {}
is_subscribed_trades = {}

# check if the certain symbol pair is available
for element in currencies["result"]["symbols"]:
	if MODE == "SPOT" and element["state"] == "ONLINE":
		list_currencies.append(element["symbol"])
		is_subscribed_trades[element["symbol"]] = False
		is_subscribed_orderbooks[element["symbol"]] = False
	elif MODE == "FUTURES" and element["state"] == "ONLINE" and element["productType"] == "perpetual":
		list_currencies.append(element["symbol"])
		is_subscribed_trades[element["symbol"]] = False
		is_subscribed_orderbooks[element["symbol"]] = False

timer = round(time.time())

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
			pair_data = '@MD ' + pair["baseCurrency"].upper() + '_' + pair["quoteCurrency"].upper() + ' perpetual ' + \
						pair["baseCurrency"].upper() + ' ' + pair["quoteCurrency"].upper() + \
						' ' + str(pair['pricePrecision']) + ' 1 1 0 0'

			print(pair_data, flush=True)




	print('@MDEND')


async def subscribe(ws, symbol):
	while True:
		if is_subscribed_trades[symbol] == False:

			# create the subscription for trades
			await ws.send(json.dumps({
				"method": "subscribe",
				"params": [
					f"trade@{symbol}"
				],
				"id": "1"
			}))

			await asyncio.sleep(0.1)


		# resubscribe if orderbook subscription is not active + possibility to not subscribe or report orderbook changes:
		if is_subscribed_orderbooks[symbol] == False and os.getenv("SKIP_ORDERBOOKS") == None:
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


		is_subscribed_trades[symbol] = False
		is_subscribed_orderbooks[symbol] = False

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
		await ws.send(message="ping")
		await asyncio.sleep(14)


#trade and orderbook stats output
async def print_stats():
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	await asyncio.sleep(time_to_wait)
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


async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))
			# create task to subscribe trades and orderbooks
			subscription = asyncio.create_task(subscribe(ws,symbol))

			async for data in ws:
				try:

					data = await ws.recv()

					if(data != "pong"):
						dataJSON = json.loads(data)

					if "topic" in dataJSON:

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


async def handler():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats())
	tasks=[]
	for symbol in list_currencies:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(1)

	await asyncio.wait(tasks)


async def main():
	await handler()


asyncio.run(main())
