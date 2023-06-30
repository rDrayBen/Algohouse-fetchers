import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.bitopro.com/v3/provisioning/trading-pairs'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://stream.bitopro.com/ws/v1/interior/pub'


for element in currencies["data"]:
	list_currencies.append(element["pair"])


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["data"]:
		pair_data = '@MD ' + pair["base"].upper() + '_' + pair["quote"].upper() + ' spot ' + \
					pair["base"].upper() + ' ' + pair["quote"].upper() + \
					' ' + pair['quotePrecision'] + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var,start_time):
	trade_data = var
	elapsed_time = time.time() - start_time
	if 'data' in trade_data and elapsed_time>7:
		for elem in trade_data["data"]:
			print('!', get_unix_time(), trade_data["pair"],
				  "S" if elem["isBuyer"] == "False" else "B", elem['price'],
				  elem["amount"], flush=True)


def get_order_books(var):
	order_data = var
	if 'asks' in order_data and len(order_data["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['pair'] + ' S '
		pq = "|".join(el["amount"] + "@" + el["price"] for el in order_data["asks"])
		answer = order_answer + pq
		print(answer + " R")

	if 'bids' in order_data and len(order_data["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['pair'] + ' B '
		pq = "|".join(el["amount"] + "@" + el["price"] for el in order_data["bids"])
		answer = order_answer + pq
		print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"event": "ping"
		}))
		await asyncio.sleep(5)


async def main():
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			start_time = time.time()

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())

			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"stream": "ORDER_BOOK",
                    "pair": f"{list_currencies[i]}",
                    "action": "+"
				}))

				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"stream": "TRADE",
                    "pair": f"{list_currencies[i]}",
                    "action": "+"
				}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "event" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['event'] == 'TRADE':
							get_trades(dataJSON, start_time)

						# if received data is about updates
						if dataJSON['event'] == 'ORDER_BOOK':
							get_order_books(dataJSON)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
