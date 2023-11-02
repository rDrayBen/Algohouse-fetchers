import json
import requests
import websockets
import time
import asyncio
import sys

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

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i].upper()] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0

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
			symbol_trade_count_for_5_minutes[trade_data['symbol'].upper()] += 1



def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data['depth'] and len(order_data["params"]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['symbol'].upper()] += len(order_data["params"]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['symbol'].upper() + ' S '
		pq = "|".join(el[0] + "@" + el[1] for el in order_data["depth"]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data['depth'] and len(order_data["depth"]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['symbol'].upper()] += len(order_data["depth"]["bids"])
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

			start_time = time.time()
			tradestats_time = start_time

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

					# trade and orderbook stats output
					if abs(time.time() - tradestats_time) >= 300:
						data1 = "# LOG:CAT=trades_stats:MSG= "
						data2 = " ".join(
							key.upper() + ":" + str(value) for key, value in symbol_trade_count_for_5_minutes.items() if
							value != 0)
						sys.stdout.write(data1 + data2)
						sys.stdout.write("\n")
						for key in symbol_trade_count_for_5_minutes:
							symbol_trade_count_for_5_minutes[key] = 0

						data3 = "# LOG:CAT=orderbooks_stats:MSG= "
						data4 = " ".join(
							key.upper() + ":" + str(value) for key, value in
							symbol_orderbook_count_for_5_minutes.items() if
							value != 0)
						sys.stdout.write(data3 + data4)
						sys.stdout.write("\n")
						for key in symbol_trade_count_for_5_minutes:
							symbol_orderbook_count_for_5_minutes[key] = 0

						tradestats_time = time.time()

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
