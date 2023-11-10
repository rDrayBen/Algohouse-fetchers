import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://back.bitsten.com/v2/market-list'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://back.bitsten.com/ws'

for element in currencies["response"]["marketList"]["result"]:
	list_currencies.append(element["name"])

async def subscribe(ws, symbol):
	id1 = 1
	id2 = 1000

	# create the subscription for trades
	await ws.send(json.dumps({
		"method": "deals.subscribe",
		"params": [
			f"{symbol}"
		],
		"id": 7
	}))


	id1 += 1

	await asyncio.sleep(0.01)

	# create the subscription for full orderbooks
	await ws.send(json.dumps({
		"method": "depth.subscribe",
		"params": [
			f"{symbol}",
			100,
			"0"
		],
		"id": 9
	}))

	id2 += 1

	await asyncio.sleep(300)

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["response"]["marketList"]["result"]:
		pair_data = '@MD ' + pair["name"] + ' spot ' + \
					pair["stock"] + ' ' + pair["money"] + \
					' ' + str(pair["money_prec"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if len(trade_data["params"][1]) != 0 and len(trade_data["params"][1]) < 3:
		for elem in trade_data["params"][1]:
			print('!', get_unix_time(), trade_data["params"][0],
					"B" if elem["type"] == "buy" else "S", elem['price'],
					elem["amount"], flush=True)


def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data['params'][1] and len(order_data["params"][1]["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params'][2] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["params"][1]["asks"])
		answer = order_answer + pq

		print(answer + " R")

	if 'bids' in order_data['params'][1] and len(order_data["params"][1]["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params'][2] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["params"][1]["bids"])
		answer = order_answer + pq

		print(answer + " R")


async def heartbeat(ws):
	id = 0
	while True:
		await ws.send(json.dumps({
			"method": "server.ping",
			"params": [],
			"id": id
		}))
		id += 1
		await asyncio.sleep(25)


async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))
			# create task to subscribe trades and orderbooks
			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:

				try:

					dataJSON = json.loads(data)

					print(dataJSON)

					if "method" in dataJSON:

						# if received data is about trades
						if dataJSON['method'] == 'deals.update':
							get_trades(dataJSON)

						# if received data is about orderbooks
						if dataJSON['method'] == 'depth.update':
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
