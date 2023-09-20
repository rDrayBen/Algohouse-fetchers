import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.ataix.com/api/symbols'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://ws.ataix.com/'


for elem in currencies["result"]:
	list_currencies.append(elem["symbol"])


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


async def handler():
	meta_data = asyncio.create_task(metadata())
	tasks = []
	for symbol in list_currencies:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(0.1)

	await asyncio.wait(tasks)


async def main():
	while True:
		await handler()
		await asyncio.sleep(300)


asyncio.run(main())