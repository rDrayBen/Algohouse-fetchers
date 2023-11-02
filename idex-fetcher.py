import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://api-matic.idex.io/v1/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://websocket-matic.idex.io/v1'

for element in currencies:
	list_currencies.append(element["market"])

#for trades count stats
symbol_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_count_for_5_minutes[list_currencies[i]] = 0


async def subscribe(ws, symbol):
	id1 = 1
	id2 = 1000

	# create the subscription for trades
	await ws.send(json.dumps({
		"method": "subscribe",
		"subscriptions":
			[{
				"name": "trades",
				"markets": [
					f"{symbol}"
				]}]
	}))


	id1 += 1

	await asyncio.sleep(0.01)

	if os.getenv("SKIP_ORDERBOOKS") == None:  # don't subscribe or report orderbook changes
		# create the subscription for full orderbooks and updates
		await ws.send(json.dumps({
			"method": "subscribe",
			"subscriptions":
				[{
					"name": "l2orderbook",
					"markets": [
						f"{symbol}"
					]}]
		}))

		id2 += 1

	await asyncio.sleep(300)


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies:
		pair_data = '@MD ' + pair["baseAsset"].upper() + '-' + pair["quoteAsset"].upper() + ' spot ' + \
					pair["baseAsset"].upper() + ' ' + pair["quoteAsset"].upper() + \
					' ' + str(pair["baseAssetPrecision"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if 'm' in trade_data["data"]:
		print('!', get_unix_time(), trade_data["data"]["m"],
				"B" if trade_data["data"]["s"] == "buy" else "S", trade_data["data"]['p'],
				trade_data["data"]["q"], flush=True)
		symbol_count_for_5_minutes[trade_data['params'][0]] += 1


def get_order_books(var):
	order_data = var
	if 'a' in order_data['data'] and len(order_data["data"]["a"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['m'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["a"])
		answer = order_answer + pq

		print(answer)


	if 'b' in order_data['data'] and len(order_data["data"]["b"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['m'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["b"])
		answer = order_answer + pq

		print(answer)



async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"method":"ping"
		}))
		await asyncio.sleep(5)


async def stats():
	while True:
		data1 = "# LOG:CAT=trades_stats:MSG= "
		data2 = " ".join(
			key.upper() + ":" + str(value) for key, value in symbol_count_for_5_minutes.items() if
			value != 0)
		sys.stdout.write(data1 + data2)
		sys.stdout.write("\n")
		for key in symbol_count_for_5_minutes:
			symbol_count_for_5_minutes[key] = 0

		await asyncio.sleep(300)

async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))
			# create task to subscribe trades and orderbooks
			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:

				dataJSON = json.loads(data)

				if "type" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['type'] == 'trades':
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['type'] == 'l2orderbook':
							get_order_books(dataJSON)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


async def handler():
	meta_data = asyncio.create_task(metadata())
	stats_data = asyncio.create_task(stats())
	tasks=[]
	for symbol in list_currencies:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(1)

	await asyncio.wait(tasks)


async def main():
	await handler()


asyncio.run(main())
