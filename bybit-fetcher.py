import json
import requests
import websockets
import time
import asyncio


def get_unix_time():
	return round(time.time() * 1000)


currency_url = 'https://api2.bybit.com/spot/api/basic/symbol_list'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = f'wss://ws2.bybit.com/spot/ws/quote/v1'

for element in currencies["result"][0]["quoteTokenSymbols"]:
	list_currencies.append(element["symbolId"])


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["result"][0]["quoteTokenSymbols"]:
		pair_data = '@MD ' + pair["baseTokenName"].upper() + pair["quoteTokenName"].upper() + ' spot ' + \
					pair["baseTokenName"].upper() + ' ' + pair["quoteTokenName"].upper() + \
					' ' + str(str(pair['quotePrecision'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_trades(var, start_time):
	trade_data = var
	elapsed_time = time.time() - start_time
	if 'data' in trade_data and elapsed_time > 7:
		for elem in trade_data["data"]:
			print('!', get_unix_time(), trade_data['symbol'],
				  "B" if elem["m"] == True else "S", elem['p'],
				  elem["q"], flush=True)


def get_order_books(var):
	order_data = var
	if 'a' in order_data['data'][0] and len(order_data["data"][0]["a"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbolName'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"][0]["a"])
		answer = order_answer + pq

		print(answer + " R")

	if 'b' in order_data['data'][0] and len(order_data["data"][0]["b"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbolName'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"][0]["b"])
		answer = order_answer + pq

		print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"ping": get_unix_time()
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
					"topic": "trade",
					"symbol": f"301.{list_currencies[i]}",
					"limit": 40,
					"id": f"trade@sub@301.{list_currencies[i]}",
					"event": "sub",
					"params": {
						"binary": False
					}
				}))

				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"topic":"mergedDepth",
					"symbol":f"301.{list_currencies[i]}",
					"limit":40,
					"params":{
						"binary": False,
						"dumpScale": 2
					},
					"id": f"mergedDepth@sub@301.{list_currencies[i]}",
					"event":"sub"
				}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "topic" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['topic'] == 'trade':
							get_trades(dataJSON, start_time)

						# if received data is about full orderbooks
						if dataJSON['topic'] == 'mergedDepth':
							get_order_books(dataJSON)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
