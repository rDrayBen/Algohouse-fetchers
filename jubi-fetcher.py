import json
import requests
import websockets
import time
import asyncio

# get all available symbol pairs
currency_url = 'https://api.jbex.com/openapi/v1/brokerInfo'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://wsapi.jbex.com/openapi/quote/ws/v1'

# check if the certain symbol pair is available
for element in currencies["symbols"]:
	if element["status"] == "TRADING":
		list_currencies.append(element["symbol"])


# get metadata about each pair of symbols
async def metadata():
	for element in currencies["symbols"]:
		if element["status"] == "TRADING":
			pair_data = '@MD ' + element['baseAsset'] + '-' + element['quoteAsset'] + ' spot ' + \
						element['baseAsset'] + ' ' + element['quoteAsset'] + \
						' ' + str(str(element['quotePrecision'])[::-1].find('.')) + ' 1 1 0 0'
			print(pair_data, flush=True)


print('@MDEND')


# get time in unix format
def get_unix_time():
	return round(time.time() * 1000)


# put the trade information in output format
def get_trades(var):
	trade_data = var
	for element in trade_data['data']:
		print('!', get_unix_time(), trade_data['symbol'],
			  "B" if element['m'] else "S", element['p'],
			  element["q"], flush=True)


# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	if 'a' in order_data['data'][0] and len(order_data['data'][0]["a"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data['data'][0]['a'])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'b' in order_data['data'][0] and len(order_data['data'][0]["b"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data['data'][0]['b'])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")


# process the situations when the server awaits "ping" request
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
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())

			print(meta_data)

			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"symbol": f"{list_currencies[i]}",
					"topic": "trade",
					"event": "sub",
					"params": {
						"binary": False
					}
				}))

				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"symbol": f"{list_currencies[i]}",
					"topic": "diffDepth",
					"event": "sub",
					"params": {
						"binary": False
					}
				}))

			while True:

				data = await ws.recv()
				try:

					dataJSON = json.loads(data)

					if 'topic' in dataJSON and 'data' in dataJSON:

						# if received data is about trades
						if dataJSON['topic'] == 'trade' and not dataJSON['f']:
							get_trades(dataJSON)

						elif dataJSON['topic'] == 'trade' and dataJSON['f']:
							pass

						# if received data is about updates
						elif dataJSON['topic'] == 'diffDepth' and not dataJSON['f']:
							get_order_books(dataJSON, depth_update=True)

						# if received data is about orderbooks
						elif dataJSON['topic'] == 'diffDepth' and dataJSON['f']:
							get_order_books(dataJSON, depth_update=False)

						else:
							pass

					elif 'error' in dataJSON:
						pass

				except Exception as ex:
					print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
