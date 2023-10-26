import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://api.aex.zone/v3/allpair.php'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://aex2.yxds.net.cn/wsv3'
is_subscribed_orderbooks = {}
is_subscribed_trades = {}


for element in currencies["data"]:
	list_currencies.append(element["coin"]+"_"+element["market"])
	is_subscribed_trades[element["coin"]+"_"+element["market"]] = False
	is_subscribed_orderbooks[element["coin"] + "_" + element["market"]] = False

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["data"]:
		pair_data = '@MD ' + pair["coin"].upper() + '_' + pair["market"].upper() + ' spot ' + \
					pair["coin"].upper() + ' ' + pair["market"].upper() + \
					' ' + str(pair["limits"]['PricePrecision']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


async def subscribe(ws):
	while True:
		for key, value in is_subscribed_trades.items():

			if value == False:

				# create the subscription for trades
				await ws.send(json.dumps({
					"cmd": 1,
					"action": "sub",
					"symbol": f"{key}"
				}))

				if is_subscribed_orderbooks[key] == False:

					# create the subscription for full orderbooks
					await ws.send(json.dumps({
						"cmd": 3,
						"action": "sub",
						"symbol": f"{key}"
					}))

					await asyncio.sleep(0.1)

			else:
				pass

		for el in list(is_subscribed_trades):
			is_subscribed_trades[el] = False

		for el in list(is_subscribed_orderbooks):
			is_subscribed_orderbooks[el] = False

		await asyncio.sleep(2000)



def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if 'trade' in trade_data:
		for elem in trade_data["trade"]:
			print('!', get_unix_time(), trade_data['symbol'].upper(),
				  "B" if elem[3] == "buy" else "S", elem[2],
				  elem[1], flush=True)


def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data['depth'] and len(order_data["depth"]["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'].upper() + ' S '
		pq = "|".join(el[0] + "@" + el[1] for el in order_data["depth"]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data['depth'] and len(order_data["depth"]["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'].upper() + ' B '
		pq = "|".join(el[0] + "@" + el[1] for el in order_data["depth"]["bids"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
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
			# create task to subscribe to symbols` pair
			subscription = asyncio.create_task(subscribe(ws))

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())


			while True:
				try:
					data = await ws.recv()

					dataJSON = json.loads(data)

					if "trade" in dataJSON or "depth" in dataJSON:

						# if received data is about trades
						if dataJSON["cmd"] == 1:
							is_subscribed_trades[dataJSON["symbol"]] = True
							get_trades(dataJSON)


						# if received data is about orderbooks
						if dataJSON["cmd"] == 3:
							is_subscribed_orderbooks[dataJSON["symbol"]] = True
							get_order_books(dataJSON, update=False)

						else:
							pass

				except Exception as ex:
					print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
