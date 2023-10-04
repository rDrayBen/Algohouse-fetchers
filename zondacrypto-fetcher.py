import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.zondacrypto.exchange/rest/trading/ticker'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.zondacrypto.exchange/websocket/'

for key, value in currencies["items"].items():
	list_currencies.append(key.lower())


# get metadata about each pair of symbols
async def metadata():
	for key, value in currencies["items"].items():
		pair_data = '@MD ' + value["market"]["first"]["currency"] + '-' + value["market"]["second"]["currency"] + ' spot ' + \
					value["market"]["first"]["currency"] + ' ' + value["market"]["second"]["currency"] + \
					' ' + str(value['market']['pricePrecision']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if 'message' in trade_data:
		parts = trade_data["topic"].split("/")
		symbol = parts[-1].upper()
		for elem in trade_data["message"]["transactions"]:
			print('!', get_unix_time(), symbol,
				  "B" if elem["ty"] == "buy" else "S", elem['r'],
				  elem["a"], flush=True)


def get_order_books(var, update):
	order_data = var
	if order_data['message']['changes'][0]["entryType"] == 'Buy'  and len(order_data["message"]["changes"][0]["state"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['message']['changes'][0]['marketCode'] + ' S '
		pq = "|".join(el["state"]["ca"] + "@" + el["state"]["ra"] for el in order_data["message"]["changes"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if order_data['message']['changes'][0]["entryType"] == 'Sell' and len(order_data["message"]["changes"][0]["state"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['message']['changes'][0]['marketCode'] + ' B '
		pq = "|".join(el["state"]["ca"] + "@" + el["state"]["ra"] for el in order_data["message"]["changes"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"action": "ping"
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
					"action": "subscribe-public",
					"module": "trading",
					"path": f"transactions/{list_currencies[i]}"
				}))

				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"action": "subscribe-public",
					"module": "trading",
					"path": f"orderbook/{list_currencies[i]}"
				}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if dataJSON["action"] == 'push':

					try:

						# if received data is about trades
						if "transactions" in dataJSON['topic']:
							get_trades(dataJSON)

						# if received data is about updates
						if "orderbook" in dataJSON['topic'] and dataJSON["message"]["changes"][0]["action"] == 'update':
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						# if dataJSON['method'] == 'snapshotOrderbook':
						# 	get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
