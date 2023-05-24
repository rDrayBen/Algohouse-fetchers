import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.dydx.exchange/v3/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.dydx.exchange/v3/ws'

for key,value in currencies["markets"].items():
	list_currencies.append(value["market"])

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies:
		pair_data = '@MD ' + pair["baseCurrency"].upper() + '-' + pair["quoteCurrency"].upper() + ' spot ' + \
					pair["baseCurrency"].upper() + ' ' + pair["quoteCurrency"].upper() + \
					' ' + str(str(pair['tickSize'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	for elem in trade_data["contents"]["trades"]:
		print('!', get_unix_time(), trade_data["id"],
			  "B" if elem["side"] == "BUY" else "S", elem['price'],
			  elem["size"], flush=True)


def get_orderbook_snapshots(var):
	order_data = var
	if ('asks' in order_data['contents'] and len(order_data["contents"]["asks"]) != 0) \
			or ('bids' in order_data['contents'] and len(order_data["contents"]["bids"]) != 0):

		order_answer_S = '$ ' + str(get_unix_time()) + " " + order_data['id'] + ' S '
		pq_S = "|".join(el["size"] + "@" + el["price"] for el in order_data["contents"]["asks"])
		answer_S = order_answer_S + pq_S
		print(answer_S + " R")

		order_answer_B = '$ ' + str(get_unix_time()) + " " + order_data['id'] + ' B '
		pq_B = "|".join(el["size"] + "@" + el["price"] for el in order_data["contents"]["bids"])
		answer_B = order_answer_B + pq_B
		print(answer_B + " R")


def get_orderbook_updates(var):
	order_data = var
	if 'asks' in order_data["contents"] and len(order_data["contents"]["asks"]) != 0:
		order_answer_S = '$ ' + str(get_unix_time()) + " " + order_data['id'] + ' S '
		pq_S = "|".join(el[0] + "@" + el[1] for el in order_data["contents"]["asks"])
		answer_S = order_answer_S + pq_S
		print(answer_S)

	if 'bids' in order_data["contents"] and len(order_data["contents"]["bids"]) != 0:
		order_answer_B = '$ ' + str(get_unix_time()) + " " + order_data['id'] + ' B '
		pq_B = "|".join(el[0] + "@" + el[1] for el in order_data["contents"]["bids"])
		answer_B = order_answer_B + pq_B
		print(answer_B)




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
			# meta_data = asyncio.create_task(metadata())

			for i in range(len(list_currencies)):

				# create the subscription for trades
				await ws.send(json.dumps({
					"type": "subscribe",
                    "channel": "v3_orderbook",
                    "id": f"{list_currencies[i]}",
                    "batched": False
				}))

				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"type": "subscribe",
                    "channel": "v3_trades",
                    "id": f"{list_currencies[i]}"
				}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				print(dataJSON)

				if "channel" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['channel'] == 'v3_trades':
							get_trades(dataJSON)

						elif dataJSON['channel'] == 'v3_orderbook':
							# if received data is orderbook snapshots
							if dataJSON["type"]=="subscribed":
								get_orderbook_snapshots(dataJSON)

							# if received data is orderbook updates
							if dataJSON["type"]=="channel_data":
								get_orderbook_updates(dataJSON)

							else:
								pass

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())

