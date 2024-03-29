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
currency_url = 'https://api.jbex.com/openapi/v1/brokerInfo'
WS_URL = 'wss://ws.jbex.com/ws/quote/v1'

args = sys.argv[1:]
if len(args) > 0:
	for arg in args:
		if arg.startswith('-') and arg[1:] == "perpetual":
			MODE = "FUTURES"
			# get all available symbol pairs
			WS_URL = 'wss://ws.jbex.com/ws/quote/v1?lang=zh-cn'
			list_currencies = ["BTCUSDT", "ETHUSDT"]
			break
else:
	MODE = "SPOT"
	# get all available symbol pairs
	currency_url = 'https://api.jbex.com/openapi/v1/brokerInfo'
	WS_URL = 'wss://ws.jbex.com/ws/quote/v1'
	answer = requests.get(currency_url)
	currencies = answer.json()
	list_currencies = list()

if MODE == "SPOT":
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
	if MODE == "SPOT":
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

	elif MODE == "FUTURES":
		# create the subscription for trades
		await ws.send(json.dumps({
			"id": f"trade301.{symbol.split('USDT')[0]}-SWAP-USDT",
			"topic": "trade",
			"event": "sub",
			"limit": 60,
			"symbol": f"301.{symbol.split('USDT')[0]}-SWAP-USDT",
			"params": {
				"org": 9001,
				"binary": False
			}
		}))

		await asyncio.sleep(0.01)

		if os.getenv("SKIP_ORDERBOOKS") == None:
			# create the subscription for full orderbooks and updates
			await ws.send(json.dumps({
				"id": f"301.{symbol.split('USDT')[0]}-SWAP-USDT1",
				"topic": "mergedDepth",
				"event": "sub",
				"symbol": f"301.{symbol.split('USDT')[0]}-SWAP-USDT",
				"limit": 22,
				"params": {
					"dumpScale": 1,
					"binary": False
				}
			}))

	await asyncio.sleep(300)

# get metadata about each pair of symbols
async def metadata():
	if MODE == "SPOT":
		for element in currencies["symbols"]:
			if element["status"] == "TRADING":
				pair_data = '@MD ' + element['baseAsset'] + element['quoteAsset'] + ' spot ' + \
							element['baseAsset'] + ' ' + element['quoteAsset'] + \
							' ' + str(str(element['quotePrecision'])[::-1].find('.')) + ' 1 1 0 0'
				print(pair_data, flush=True)
	elif MODE == "FUTURES":
		for element in list_currencies:
			pair_data = '@MD ' + element + ' perpetual ' + \
						element.split("USDT")[0] + " USDT " + '-1 1 1 0 0'

			print(pair_data, flush=True)
	print('@MDEND')

# put the trade information in output format
def get_trades(var):
	trade_data = var
	for element in trade_data['data']:
		print('!', get_unix_time(), trade_data['symbol'],
			  "B" if element['m'] else "S", element['p'],
			  element["q"], flush=True)
		if MODE == "SPOT":
			symbol_trade_count_for_5_minutes[trade_data['symbol']] += 1
		elif MODE == "FUTURES":
			symbol_trade_count_for_5_minutes[trade_data['symbol'].split("-")[0]+"USDT"] += 1

# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	if 'a' in order_data['data'][0] and len(order_data['data'][0]["a"]) != 0:
		if MODE == "SPOT":
			symbol_orderbook_count_for_5_minutes[order_data['symbol']] += len(order_data['data'][0]["a"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'] + ' S '
		elif MODE == "FUTURES":
			symbol_orderbook_count_for_5_minutes[order_data['symbol'].split("-")[0]+"USDT"] += len(order_data['data'][0]["a"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'].split("-")[0] + "USDT" + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data['data'][0]['a'])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'b' in order_data['data'][0] and len(order_data['data'][0]["b"]) != 0:
		if MODE == "SPOT":
			symbol_orderbook_count_for_5_minutes[order_data['symbol']] += len(order_data['data'][0]["b"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'] + ' B '
		elif MODE == "FUTURES":
			symbol_orderbook_count_for_5_minutes[order_data['symbol'].split("-")[0] + "USDT"] += len(
				order_data['data'][0]["b"])
			order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'].split("-")[0] + "USDT" + ' B '
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
		await asyncio.sleep(4)

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

						# if received data is about updates (spot)
						elif dataJSON['topic'] == 'diffDepth' and not dataJSON['f']:
							get_order_books(dataJSON, depth_update=True)
						elif dataJSON['topic'] == 'diffDepth' and not dataJSON['f']:
							get_order_books(dataJSON, depth_update=True)

						# if received data is about orderbooks (futures)
						elif dataJSON['topic'] == 'mergedDepth' and dataJSON['f']:
							get_order_books(dataJSON, depth_update=False)
						elif dataJSON['topic'] == 'mergedDepth' and not dataJSON['f']:
							get_order_books(dataJSON, depth_update=True)

						else:
							pass

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
	while True:
		await handler()
		await asyncio.sleep(300)



asyncio.run(main())
