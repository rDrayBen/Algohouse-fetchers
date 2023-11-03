import json
import requests
import websockets
import time
import asyncio
import os
import sys

# get all available symbol pairs
currency_url = 'https://api.jbex.com/openapi/v1/brokerInfo'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://ws.jbex.com/ws/quote/v1'

# check if the certain symbol pair is available
for element in currencies["symbols"]:
	if element["status"] == "TRADING":
		list_currencies.append(element["symbol"])

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
		"symbol": f"{symbol}",
		"topic": "trade",
		"event": "sub",
		"params": {
			"binary": False
		}
	}))


	id1 += 1

	await asyncio.sleep(0.01)

	if os.getenv("SKIP_ORDERBOOKS") == None:
		# create the subscription for full orderbooks and updates
		await ws.send(json.dumps({
			"symbol": f"{symbol}",
			"topic": "diffDepth",
			"event": "sub",
			"params": {
				"binary": False
			}
		}))

		id2 += 1

	await asyncio.sleep(300)

# get metadata about each pair of symbols
async def metadata():
	for element in currencies["symbols"]:
		if element["status"] == "TRADING":
			pair_data = '@MD ' + element['baseAsset'] + element['quoteAsset'] + ' spot ' + \
						element['baseAsset'] + ' ' + element['quoteAsset'] + \
						' ' + str(str(element['quotePrecision'])[::-1].find('.')) + ' 1 1 0 0'
			print(pair_data, flush=True)
	print('@MDEND')


# get time in unix format
def get_unix_time():
	return round(time.time() * 1000)


# put the trade information in output format
def get_trades(var):
	trade_data = var
	for element in trade_data['data']:
		print('!', get_unix_time(), trade_data['symbol'],
			  "B" if element['m'] else "S", element['p'],
			  element["q"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data['symbol']] += 1


# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	if 'a' in order_data['data'][0] and len(order_data['data'][0]["a"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['symbol']] += len(order_data['data'][0]["a"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data['data'][0]['a'])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'b' in order_data['data'][0] and len(order_data['data'][0]["b"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['symbol']] += len(order_data['data'][0]["b"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data['data'][0]['b'])
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
			"ping": get_unix_time()
		}))
		await asyncio.sleep(0.01)


# trade and orderbook stats output
async def stats():
	while True:
		data1 = "# LOG:CAT=trades_stats:MSG= "
		data2 = " ".join(
			key.upper() + ":" + str(value) for key, value in symbol_trade_count_for_5_minutes.items() if
			value != 0)
		sys.stdout.write(data1 + data2)
		sys.stdout.write("\n")
		for key in symbol_trade_count_for_5_minutes:
			symbol_trade_count_for_5_minutes[key] = 0

		data3 = "# LOG:CAT=orderbooks_stats:MSG= "
		data4 = " ".join(
			key.upper() + ":" + str(value) for key, value in
			symbol_orderbook_count_for_5_minutes.items() if
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

					dataJSON = json.loads(data)

					if 'error' in dataJSON:
						pass

					elif 'topic' in dataJSON:

						# if received data is about trades
						if dataJSON['topic'] == 'trade' and not dataJSON['f']:
							get_trades(dataJSON)

						elif dataJSON['topic'] == 'trade' and dataJSON['f']:
							pass

						# if received data is about updates
						elif dataJSON['topic'] == 'diffDepth' and not dataJSON['f']:
							get_order_books(dataJSON, depth_update=True)

						# if received data is about orderbooks
						elif dataJSON['topic'] == 'diffDepth' and dataJSON['f']:
							get_order_books(dataJSON, depth_update=False)

						else:
							pass


				except Exception as ex:
					print(f"Exception {ex} occurred")

				except:
					pass


		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")

		except:
			continue


async def handler():
	meta_data = asyncio.create_task(metadata())
	stats_data = asyncio.create_task(stats())
	tasks=[]
	for symbol in list_currencies:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(1)

	await asyncio.wait(tasks)


async def main():
	await handler()


asyncio.run(main())
