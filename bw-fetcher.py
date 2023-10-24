import json
import requests
import websockets
import time
import asyncio
import os

currency_url = 'https://api.bw6.com/data/v1/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.bw6.com/websocket'


for key, value in currencies.items():
	parts = key.split("_")
	currency1 = parts[0]
	currency2 = parts[1]
	list_currencies.append(currency1+currency2)


# get metadata about each pair of symbols
async def metadata():
	for key, value in currencies.items():
		pair_data = '@MD ' + key.split("_")[0].upper() + key.split("_")[1].upper() + ' spot ' + \
					key.split("_")[0].upper() + ' ' + key.split("_")[1].upper() + \
					' ' + str(value["priceScale"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if 'data' in trade_data:
		for elem in trade_data["data"]:
			print('!', get_unix_time(), trade_data["channel"].split("_")[0].upper(),
				  "B" if elem["type"] == "buy" else "S", elem['price'],
				  elem["amount"], flush=True)


def get_order_books(var, update):
	order_data = var

	if 'asks' in order_data and len(order_data["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["channel"].split("_")[0].upper() + ' S '
		pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data["asks"])
		answer = order_answer + pq

		print(answer + " R")

	if 'bids' in order_data and len(order_data["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data["channel"].split("_")[0].upper() + ' B '
		pq = "|".join(str(el[1]) + "@" + str("{:.8f}".format(el[0])) for el in order_data["bids"])
		answer = order_answer + pq

		print(answer + " R")

async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"channel": "ping",
			"event": "addChannel",
			"binary": True,
			"isZip": True
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
					"event": "addChannel",
					"channel": f"{list_currencies[i]}_trades"
				}))
				if (os.getenv("SKIP_ORDERBOOKS") == None):
					# create the subscription for full orderbooks and updates
					await ws.send(json.dumps({
						"event":"addChannel",
						"channel":f"{list_currencies[i]}_depth"
					}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "channel" in dataJSON:
					try:

						if dataJSON['channel']=='pong':
							pass

						# if received data is about trades
						elif dataJSON['channel'].split("_")[1] == 'trades':
							get_trades(dataJSON)

						# if received data is about orderbooks and updates
						elif dataJSON['channel'].split("_")[1] == 'depth':
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
