import json
import requests
import websockets
import time
import asyncio
import os

# currency_url = 'https://www.fairdesk.com/user/v1/public/spot/settings/product'        #inactive
# answer = requests.get(currency_url)
# currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://www.fairdesk.com/ws?token=web.361414.5E4FCAB8020E5E94ED6DF56EB1D128AD'

list_currencies = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "TRXUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "LINKUSDT", "ADAUSDT",
				   "DOTUSDT", "ETCUSDT", "AAVEUSDT", "CHZUSDT", "LDOUSDT", "ATOMUSDT", "FILUSDT", "SANDUSDT", "ARBUSDT"]

baseCcyName = ["BTC", "ETH", "LTC", "TRX", "BNB", "XRP", "DOGE", "LINK", "ADA",
				   "DOT", "ETC", "AAVE", "CHZ", "LDO", "ATOM", "FIL", "SAND", "ARB"]

quoteCcyName = "USDT"

tickSize = [0.1, 0.01, 0.01, 1.0E-5, 0.01, 1.0E-4, 1.0E-5, 0.001, 1.0E-4, 0.001, 0.001, 0.01, 1.0E-4, 0.001,
			0.001, 0.001, 1.0E-4, 0.001]

# get metadata about each pair of symbols
async def metadata():
	for i in range(len(list_currencies)):
		pair_data = '@MD ' + list_currencies[i].lower() + ' spot ' + \
					baseCcyName[i].lower() + ' ' + quoteCcyName.lower() + \
					' ' + str(str(tickSize[i])[::-1].find('.')) + ' 1 1 0 0'

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
<<<<<<< HEAD
				# create the subscription for trades, full orderbooks and updates
				await ws.send(json.dumps({
					"method": "SUBSCRIBE",
					"params": [
						"web.361414.5E4FCAB8020E5E94ED6DF56EB1D128AD",
						f"{list_currencies[i].lower()}@spotDepth100",
						f"{list_currencies[i].lower()}@spotTrade"
				]}))
=======
				# create the subscription for trades
				await ws.send(json.dumps({
					"method":"SUBSCRIBE",
					"params":[
						f"{list_currencies[i]}@spotTrade"
					]
				}))

				if os.getenv("SKIP_ORDERBOOKS") == None:  # don't subscribe or report orderbook changes
					# create the subscription for full orderbooks and updates
					await ws.send(json.dumps({
						"method": "SUBSCRIBE",
						"params": [
							f"{list_currencies[i]}@spotDepth100"
						]
					}))
>>>>>>> 187a84021ec75378e888478a885dd48d052639f5

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
