import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://pro.apex.exchange/api/v1/symbols'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = f'wss://quote.pro.apex.exchange/realtime_public'

for element in currencies["data"]["perpetualContract"]:
	list_currencies.append(element["crossSymbolName"])

#for trades count stats
symbol_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_count_for_5_minutes[list_currencies[i]] = 0


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["data"]["perpetualContract"]:
		pair_data = '@MD ' + pair["underlyingCurrencyId"] + '-' + pair["settleCurrencyId"] + ' spot ' + \
					pair["underlyingCurrencyId"] + ' ' + pair["settleCurrencyId"] + \
					' ' + str(str(pair['stepSize'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)

def get_trades(var):
	trade_data = var
	for elem in trade_data["data"]:
		print('!', get_unix_time(), elem['s'],
				"B" if elem["S"] == "Buy" else "S", elem['p'],
				elem["v"], flush=True)
		symbol_count_for_5_minutes[elem['s']] += 1


def get_order_books(var, update):
	order_data = var
	if 'a' in order_data['data'] and len(order_data["data"]["a"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['s'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["a"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'b' in order_data['data'] and len(order_data["data"]["b"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['s'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["b"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"op":"ping",
			"args":[f"{get_unix_time()}"]
		}))
		await asyncio.sleep(5)


async def main():
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:

			start_time = time.time()
			tradestats_time = start_time

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())

			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"op":"subscribe",
					"args":[
						f"recentlyTrade.H.{list_currencies[i]}"
					]
				}))

				if os.getenv("SKIP_ORDERBOOKS") == None:
					# create the subscription for full orderbooks and updates
					await ws.send(json.dumps({
						"op":"subscribe",
						"args":[f"orderBook200.H.{list_currencies[i]}"]
					}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if abs(time.time() - tradestats_time) >= 300:
					data1 = "# LOG:CAT=trades_stats:MSG= "
					data2 = " ".join(key.upper() + ":" + str(value) for key, value in symbol_count_for_5_minutes.items() if value != 0)
					sys.stdout.write(data1 + data2)
					sys.stdout.write("\n")
					for key in symbol_count_for_5_minutes:
						symbol_count_for_5_minutes[key] = 0
					tradestats_time = time.time()


				if "topic" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['topic'].split(".")[0] == 'recentlyTrade' and dataJSON["type"]=="delta":
							get_trades(dataJSON)

						#if received data is about updates
						if dataJSON['topic'].split(".")[0] == 'orderBook200' and dataJSON["type"]=="delta":
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						if dataJSON['topic'].split(".")[0] == 'orderBook200' and dataJSON["type"]=="snapshot":
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())

