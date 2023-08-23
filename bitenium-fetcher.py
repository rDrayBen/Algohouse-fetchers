import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.bitenium.com/spotapi/api/exchangeInfo'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'ws://stream.bitenium.com:9443'

for element in currencies["symbols"]:
	list_currencies.append(element["symbol"])

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["symbols"]:
		pair_data = '@MD ' + pair["baseAsset"] + pair["quoteAsset"] + ' spot ' + \
					pair["baseAsset"] + ' ' + pair["quoteAsset"] + \
					' ' + str(pair["quotePrecision"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	print('!', get_unix_time(), trade_data["market"],
		"B" if trade_data["data"]["m"] == True else "S", trade_data["data"]['p'],
			trade_data["data"]["q"], flush=True)


def get_order_books(var):
	order_data = var
	if 'asks' in order_data['data'] and len(order_data["data"]["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["market"] + ' S '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["data"]["asks"])
		answer = order_answer + pq
		print(answer + " R")

	if 'bids' in order_data['data'] and len(order_data["data"]["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["market"] + ' B '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["data"]["bids"])
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
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())

			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"channel": "tick",
					"market": f"{list_currencies[i]}",
					"event": "add"
				}))

				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"channel": "depth",
					"market": f"{list_currencies[i]}",
					"event": "add"
				}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "data" in dataJSON:

					try:

						# if received data is about updates and full orderbooks
						if "bids" in dataJSON['data'] or "asks" in dataJSON['data'] and 'e' not in dataJSON["data"]:
							get_order_books(dataJSON)

						# if received data is about trades
						elif dataJSON["data"]['e'] == 'trade':
							get_trades(dataJSON)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
