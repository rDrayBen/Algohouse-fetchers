import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.cointr.pro/v1/spot/public/instruments'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://www.cointr.pro/ws'

for element in currencies["data"]:
	list_currencies.append(element["instId"])


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["data"]:
		pair_data = '@MD ' + pair["baseCcy"] + pair["quoteCcy"] + ' spot ' + \
					pair["baseCcy"] + ' ' + pair["quoteCcy"] + \
					' ' + str(pair["pxPrecision"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


async def subscribe(ws, symbol):

	# create the subscription for full orderbooks and updates
	await ws.send(json.dumps({
		"args": [{
			"limit": 30,
			"step": "0.001",
			"instId": f"{symbol}"
		}],
		"channel": "spot_depth",
		"op": "subscribe"
	}))

	await asyncio.sleep(0.001)

	# create the subscription for trades
	await ws.send(json.dumps({
		"args": [{
			"instId": f"{symbol}"
		}],
		"channel": "spot_trade",
		"op": "subscribe"
	}))

	await asyncio.sleep(300)


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	for elem in trade_data["data"]:
		print('!', get_unix_time(), trade_data["instId"],
			  "B" if elem["side"] == "BUY" else "S", elem['px'],
			  elem["sz"], flush=True)


def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data['data'] and len(order_data["data"]["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['instId'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data['data'] and len(order_data["data"]["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['instId'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["bids"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"ping"
		}))
		await asyncio.sleep(5)


async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:
				try:
					data = await ws.recv()

					dataJSON = json.loads(data)

					if "channel" in dataJSON and "event" not in dataJSON:

						# if received data is about trades
						if dataJSON['channel'] == 'spot_trade' and dataJSON['action'] == "update":
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['channel'] == 'spot_depth' and dataJSON['action'] == "update":
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						if dataJSON['channel'] == 'spot_depth' and dataJSON['action'] == "snapshot":
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
