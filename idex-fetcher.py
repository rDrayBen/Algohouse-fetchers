import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

currency_url = 'https://api-matic.idex.io/v1/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://websocket-matic.idex.io/v1'
is_subscribed_orderbooks = {}
is_subscribed_trades = {}

for element in currencies:
	list_currencies.append(element["market"])
	is_subscribed_trades[element["market"]] = False
	is_subscribed_orderbooks[element["market"]] = False

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
		if not is_subscribed_trades[symbol]:
			# create the subscription for trades
			await ws.send(json.dumps({
				"method": "subscribe",
				"subscriptions":
					[{
						"name": "trades",
						"markets": [
							f"{symbol}"
						]}]
			}))
			await asyncio.sleep(0.01)

		if not is_subscribed_trades[symbol] and os.getenv("SKIP_ORDERBOOKS") == None:  # don't subscribe or report orderbook changes
			# create the subscription for full orderbooks and updates
			await ws.send(json.dumps({
				"method": "subscribe",
				"subscriptions":
					[{
						"name": "l2orderbook",
						"markets": [
							f"{symbol}"
						]}]
			}))

			await asyncio.sleep(0.01)

		is_subscribed_trades[symbol] = False
		is_subscribed_orderbooks[symbol] = False

		await asyncio.sleep(2000)

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies:
		pair_data = '@MD ' + pair["baseAsset"].upper() + '-' + pair["quoteAsset"].upper() + ' spot ' + \
					pair["baseAsset"].upper() + ' ' + pair["quoteAsset"].upper() + \
					' ' + str(pair["baseAssetPrecision"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

def get_trades(var):
	trade_data = var
	if 'm' in trade_data["data"]:
		print('!', get_unix_time(), trade_data["data"]["m"],
				"B" if trade_data["data"]["s"] == "buy" else "S", trade_data["data"]['p'],
				trade_data["data"]["q"], flush=True)
		symbol_trade_count_for_5_minutes[trade_data["data"]["m"]] += 1

def get_order_books(var):
	order_data = var
	if 'a' in order_data['data'] and len(order_data["data"]["a"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['m']] += len(order_data["data"]["a"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['m'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["a"])
		answer = order_answer + pq
		print(answer)

	if 'b' in order_data['data'] and len(order_data["data"]["b"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['m']] += len(order_data["data"]["b"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['m'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["b"])
		answer = order_answer + pq
		print(answer)

async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"method":"ping"
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
			# create task to subscribe trades and orderbooks
			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:

				dataJSON = json.loads(data)

				if "type" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['type'] == 'trades':
							is_subscribed_trades[dataJSON["data"]["m"].upper()] = True
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['type'] == 'l2orderbook':
							is_subscribed_orderbooks[dataJSON["data"]["m"].upper()] = True
							get_order_books(dataJSON)

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
	tasks = []
	for symbol in list_currencies:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(1)

	await asyncio.wait(tasks)


async def main():
	await handler()


asyncio.run(main())
