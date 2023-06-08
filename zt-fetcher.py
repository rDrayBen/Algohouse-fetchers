import json
import requests
import websockets
import time
import asyncio

# get all available symbol pairs
currency_url = 'https://www.ztb.im/api/v1/exchangeInfo'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://www.ztb.im/ws'

# check if the certain symbol pair is available
for element in currencies:
	if element["status"] == "trading":
		list_currencies.append(element["symbol"])


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies:
		pair_data = '@MD ' + pair["baseAsset"].upper() + '_' + pair["quoteAsset"].upper() + ' spot ' + \
					pair["baseAsset"].upper() + ' ' + pair["quoteAsset"].upper() + \
					' ' + str(str(pair['tick_size'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


# get time in unix format
def get_unix_time():
	return round(time.time() * 1000)


async def subscribe(ws, symbol):
	id1 = 1
	id2 = 1000

	# create the subscription for full orderbooks and updates
	await ws.send(json.dumps({
		"method": "depth.subscribe",
		"params": [
			f"{symbol}",
			50,
			"0.000000001"
		],
		"id": 10086
	}))

	id1 += 1

	await asyncio.sleep(0.001)

	# create the subscription for trades
	await ws.send(json.dumps({
		"method": "deals.subscribe",
		"params": [
			f"{symbol}"
		],
		"id": 10086
	}))

	id2 += 1

	await asyncio.sleep(300)


# put the trade information in output format
def get_trades(var):
	trade_data = var
	if len(trade_data["params"][1]) != 0:
		for elem in trade_data["params"][1]:
			print('!', get_unix_time(), trade_data['params'][0],
				  "S" if elem["type"] == "sell" else "B", elem['price'],
				  elem["amount"], flush=True)


# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	if 'asks' in order_data['params'][1] and len(order_data["params"][1]["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params'][2] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["params"][1]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data['params'][1] and len(order_data["params"][1]["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params'][2] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["params"][1]["bids"])
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
			"method": "server.ping",
			"params": [],
			"id": 1629
		}))
		await asyncio.sleep(5)


async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:
				try:
					data = await ws.recv()

					dataJSON = json.loads(data)

					if "method" in dataJSON:

						# if received data is about trades
						if dataJSON['method'] == 'deals.update':
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['method'] == 'depth.update' and dataJSON['params'][0] == False:
							get_order_books(dataJSON, depth_update=True)

						# if received data is about orderbooks
						if dataJSON['method'] == 'depth.update' and dataJSON['params'][0] == True:
							get_order_books(dataJSON, depth_update=False)

						else:
							pass

				except Exception as ex:
					print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


async def handler():
	meta_data = asyncio.create_task(metadata())
	tasks=[]
	for symbol in list_currencies:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(0.1)

	await asyncio.wait(tasks)


async def main():
	while True:
		await handler()
		await asyncio.sleep(300)



asyncio.run(main())


main()
