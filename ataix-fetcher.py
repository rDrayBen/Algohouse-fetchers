import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://api.ataix.com/api/symbols'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://ws.ataix.com/'


for elem in currencies["result"]:
	list_currencies.append(elem["symbol"])

#for trades count stats
symbol_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_count_for_5_minutes[list_currencies[i]] = 0

async def subscribe(ws, symbol):
	id1 = 1
	id2 = 1000
	# create the subscription for trades
	await ws.send(json.dumps({
		"method": "subscribeTrades",
		"params": {
			"symbol": f"{symbol}",
			"limit": 100
		},
		"id": id1
	}))

	id1 += 1

	await asyncio.sleep(0.01)
	if os.getenv("SKIP_ORDERBOOKS") == None:
		# create the subscription for full orderbooks and updates
		await ws.send(json.dumps({
			"method": "subscribeBook",
			"params": {
				"symbol": f"{symbol}"
			},
			"id": id2
		}))

		id2 += 1

	await asyncio.sleep(300)

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["result"]:
		pair_data = '@MD ' + pair["base"] + '/' + pair["quote"] + ' spot ' + \
					pair["base"] + ' ' + pair["quote"] + \
					' ' + str(pair["pricePrecision"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	print('!', get_unix_time(), trade_data["result"]["pair"],
		  "B" if trade_data["result"]["side"] == "BUY" else "S", trade_data["result"]['price'],
		  trade_data["result"]["quantity"], flush=True)
	symbol_count_for_5_minutes[trade_data["result"]["pair"]] += 1


def get_order_books(var, update):
	order_data = var
	if 'sell' in order_data['result'] and len(order_data["result"]["sell"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['result']['symbol'] + ' S '
		pq = "|".join(el["quantity"] + "@" + el["price"] for el in order_data["result"]["sell"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'buy' in order_data['result'] and len(order_data["result"]["buy"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['result']['symbol'] + ' B '
		pq = "|".join(el["quantity"] + "@" + el["price"] for el in order_data["result"]["buy"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

async def trade_stats(tradestats_time):
	while True:
		if abs(time.time() - tradestats_time) >= 5:
			data1 = "# LOG:CAT=trades_stats:MSG= "
			data2 = " ".join(
				key.upper() + ":" + str(value) for key, value in symbol_count_for_5_minutes.items() if
				value != 0)
			sys.stdout.write(data1 + data2)
			sys.stdout.write("\n")
			for key in symbol_count_for_5_minutes:
				symbol_count_for_5_minutes[key] = 0
			tradestats_time = time.time()

async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL):
		try:

			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:

				try:
					dataJSON = json.loads(data)

					if 'error' in dataJSON and 'code' in dataJSON['error'] and dataJSON['error'][
						'code'] == 552:
							pass
					else:

						if "method" in dataJSON:

							# if received data is about trades
							if dataJSON['method'] == 'newTrade':
								get_trades(dataJSON)

							# if received data is about updates
							if dataJSON['method'] == 'bookUpdate':
								get_order_books(dataJSON, update=True)

							# if received data is about orderbooks
							if dataJSON['method'] == 'snapshotBook':
								get_order_books(dataJSON, update=False)

							else:
								pass

				except Exception as ex:
					print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


async def handler(tradestats_time):
	meta_data = asyncio.create_task(metadata())
	tasks = []
	for symbol in list_currencies:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(0.1)

	await asyncio.wait(tasks)


async def main():
	start_time = time.time()
	tradestats_time = start_time
	while True:
		await handler(tradestats_time)
		trade_stats_task = asyncio.create_task(trade_stats(tradestats_time))
		await asyncio.sleep(300)


asyncio.run(main())