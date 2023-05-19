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


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"type": "ping"
		}))
		await asyncio.sleep(5)


async def main():
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())

			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"method": "subscribeTrades",
					"params": {
						"symbol": f"{list_currencies[i]}",
						"limit": 100
					},
					"id": 1
				}))

				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"method": "subscribeBook",
					"params": {
						"symbol": f"{list_currencies[i]}"
					},
					"id": 1
				}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "method" in dataJSON:

					try:

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


asyncio.run(main())
