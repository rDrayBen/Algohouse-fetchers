import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://www.fairdesk.com/user/v1/public/spot/settings/product'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://www.fairdesk.com/ws'


for element in currencies["data"]:
	list_currencies.append(element["name"])


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["data"]:
		pair_data = '@MD ' + pair["baseCcyName"] + '-' + pair["quoteCcyName"] + ' spot ' + \
					pair["baseCcyName"] + ' ' + pair["quoteCcyName"] + \
					' ' + str(str(pair['tickSize'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var, start_time):
	trade_data = var
	elapsed_time = time.time() - start_time
	if elapsed_time > 3:
		print('!', get_unix_time(), trade_data['s'],
			'B' if trade_data['m'] else 'S', trade_data['p'],
			trade_data["q"], flush=True)


def get_order_books(var, update):
	order_data = var
	if 'a' in order_data and len(order_data["a"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['s'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["a"])
		answer = order_answer + pq

		print(answer + " R")

	if 'b' in order_data and len(order_data["b"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['s'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["b"])
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
					"method":"SUBSCRIBE",
					"params":[
						f"{list_currencies[i]}@spotTrade"
					]
				}))

				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"method": "SUBSCRIBE",
					"params": [
						f"{list_currencies[i]}@spotDepth100"
					]
				}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "e" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['e'] == 'trade':
							get_trades(dataJSON, start_time)

						# if received data is about updates and full orderbooks
						if dataJSON['e'] == 'depthUpdate':
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
