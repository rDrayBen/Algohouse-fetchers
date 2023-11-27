import json
import requests
import websockets
import time
import asyncio
import os
from CommonFunctions.CommonFunctions import get_unix_time, stats

currency_url = 'https://exmarkets.com/api/v1/general/info'
answer = requests.get(currency_url)
currencies = answer.json()
WS_URL = 'wss://exmarkets.com/ws'
list_currencies_id = list()
list_currencies_name = list()
is_subscribed_orderbooks = {}
is_subscribed_trades = {}

for element in currencies["markets"]:
	list_currencies_id.append(element["id"])
	list_currencies_name.append(element["name"])
	is_subscribed_trades[element["id"]] = False
	is_subscribed_orderbooks[element["id"]] = False

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies_name)):
	symbol_trade_count_for_5_minutes[list_currencies_name[i].upper()] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies_name)):
	symbol_orderbook_count_for_5_minutes[list_currencies_name[i].upper()] = 0

async def subscribe(ws, symbol):
	while True:
		if not is_subscribed_trades[symbol] and not is_subscribed_orderbooks[symbol]:

			await ws.send(json.dumps({
				"e": "init",
			}))

			# create the subscription for trades and orderbooks
			await ws.send(json.dumps({
				"e": "market",
				"chartInterval": "30m",
				"marketId": symbol
			}))

		is_subscribed_trades[symbol] = False
		is_subscribed_orderbooks[symbol] = False

		await asyncio.sleep(2000)

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["markets"]:
		pair_data = '@MD ' + pair["name"].split("-")[0] + '-' + pair["name"].split("-")[1] + ' spot ' + \
					pair["name"].split("-")[0] + ' ' + pair["name"].split("-")[1] + \
					' ' + str(pair["quote_precision"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')

def get_trades(var):
	trade_data = var
	if 'data' in trade_data:
		print('!', get_unix_time(), trade_data["data"]["market"].upper(),
				"S" if trade_data["data"]["side"] == "SELL" else "B", str(format(trade_data["data"]['price'], '.5f')),
				str(trade_data["data"]['amount']), flush=True)
		symbol_trade_count_for_5_minutes[trade_data["data"]["market"].upper()] += 1

def get_order_books(var):
	order_data = var
	if 'sell' in order_data['data'] and len(order_data["data"]["sell"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['market'].upper()] += len(order_data["data"]["sell"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['market'].upper() + ' S '
		pq = "|".join(str(el['amount']) + "@" + str(format(el['price'], '.5f')) for el in order_data["data"]["sell"])
		answer = order_answer + pq
		print(answer + " R")

	if 'buy' in order_data['data'] and len(order_data["data"]["buy"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['data']['market'].upper()] += len(order_data["data"]["buy"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['market'].upper() + ' B '
		pq = "|".join(str(el['amount']) + "@" + str(format(el['price'], '.5f')) for el in order_data["data"]["buy"])
		answer = order_answer + pq
		print(answer + " R")

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
			subscription = asyncio.create_task(subscribe(ws, symbol))
			async for data in ws:
				try:
					dataJSON = json.loads(data)

					# if received data is about trades
					if dataJSON['type'] == 'market-trade':
						is_subscribed_trades[dataJSON["data"]["market"].upper()] = True
						get_trades(dataJSON)

					# if received data is about orderbooks + possibility to not subscribe or report orderbook changes
					if dataJSON['type'] == 'market-orderbook' and os.getenv("SKIP_ORDERBOOKS") == None:
						is_subscribed_orderbooks[dataJSON['data']['market'].upper()] = True
						get_order_books(dataJSON)
					else:
						pass

				except Exception as ex:
					print(f"Exception {ex} occurred")
				except:
					pass

		except:
			continue


async def handler():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes))
	tasks = []
	for symbol in list_currencies_id:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(0.1)

	await asyncio.wait(tasks)

async def main():
	await handler()



asyncio.run(main())
