import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://www.fubthk.com/api/v2/trade/public/markets?limit=1000'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://www.fubthk.com/api/v2/ranger/public/?stream=global.tickers'

for element in currencies:
	list_currencies.append(element["id"])


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies:
		pair_data = '@MD ' + pair["id"] + ' spot ' + \
					pair["base_unit"] + ' ' + pair["quote_unit"] + \
					' ' + str(pair['price_precision']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var, currency):
	trade_data = var
	if len(trade_data[f"{currency}.trades"]["trades"]) != 0:
		for elem in trade_data[f"{currency}.trades"]["trades"]:
			print('!', get_unix_time(), currency,
				  "B" if elem["taker_type"] == "buy" else "S", elem['price'],
				  elem["amount"], flush=True)


# def get_order_books(var, update):
# 	order_data = var
# 	if 'ask' in order_data['params'] and len(order_data["params"]["ask"]) != 0:
# 		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params']['symbol'] + ' S '
# 		pq = "|".join(el["size"] + "@" + el["price"] for el in order_data["params"]["ask"])
# 		answer = order_answer + pq
# 		# checking if the input data is full orderbook or just update
# 		if (update == True):
# 			print(answer)
# 		else:
# 			print(answer + " R")
#
# 	if 'bid' in order_data['params'] and len(order_data["params"]["bid"]) != 0:
# 		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['params']['symbol'] + ' B '
# 		pq = "|".join(el["size"] + "@" + el["price"] for el in order_data["params"]["bid"])
# 		answer = order_answer + pq
# 		# checking if the input data is full orderbook or just update
# 		if (update == True):
# 			print(answer)
# 		else:
# 			print(answer + " R")


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
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())

			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"event":"subscribe",
					"streams":[
						f"{list_currencies[i]}.trades",
						f"{list_currencies[i]}.update"
					]
				}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				for i in range(len(list_currencies)):

					if f"{list_currencies[i]}.trades" in dataJSON:

						try:

							currency = list_currencies[i]

							get_trades(dataJSON, currency)

						except Exception as ex:
							print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
