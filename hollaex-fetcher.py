import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.hollaex.com/v2/constants'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.hollaex.com/stream'

for key, value in currencies["pairs"].items():
	if value["is_public"]==True:
		list_currencies.append(key)


# get metadata about each pair of symbols
async def metadata():
	for key, value in currencies["pairs"].items():
		pair_data = '@MD ' + value["pair_base"] + "-" + value["pair_2"] + ' spot ' + \
					value["pair_base"] + ' ' + value["pair_2"] + ' -1 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	for elem in trade_data["data"]:
		print('!', get_unix_time(), trade_data["symbol"],
			"B" if elem["side"] == "buy" else "S", str(elem["price"]),
			 elem["size"], flush=True)


def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data["data"] and len(order_data["data"]["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["symbol"] + ' S '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["data"]["asks"])
		answer = order_answer + pq

		print(answer + " R")

	if 'bids' in order_data["data"] and len(order_data["data"]["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["symbol"] + ' B '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["data"]["bids"])
		answer = order_answer + pq

		print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
            "op": "ping"
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

			# create the subscription for full orderbooks, updates and trades
			await ws.send(json.dumps({
    			"op": "subscribe",
				"args": ["orderbook", "trade"]
			}))


			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "topic" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON["topic"] == "trade" and dataJSON["action"] == "insert":
							get_trades(dataJSON)

						# if received data is about updates and full orderbooks
						if dataJSON["topic"] == "orderbook" and dataJSON["action"] == "partial":
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
