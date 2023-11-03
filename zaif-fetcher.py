import json
import requests
import websockets
import time
import asyncio
import sys

currency_url = 'https://zaif.jp/api/v2/orderbook/info'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://ws.zaif.jp/stream?currency_pair='

for key,value in currencies["currency_configs"].items():
	list_currencies.append(value["currency_pair"])

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0

# get metadata about each pair of symbols
async def metadata():
	for key,value in currencies["currency_configs"].items():
		pair_data = '@MD ' + value["currency_pair"] + ' spot ' + \
					value["item"].lower() + ' ' + value["aux"].lower() + \
					' ' + str(str(value['aux_unit_min'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_order_books(var):
	order_data = var
	if 'asks' in order_data and len(order_data["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['trades'][0]['currency_pair']] += len(order_data["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['trades'][0]['currency_pair'] + ' S '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["asks"])
		answer = order_answer + pq
		print(answer + " R")

	if 'bids' in order_data and len(order_data["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['trades'][0]['currency_pair']] += len(order_data["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['trades'][0]['currency_pair'] + ' B '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["bids"])
		answer = order_answer + pq
		print(answer + " R")


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"event": "ping"
		}))
		await asyncio.sleep(5)

# trade and orderbook stats output
async def stats():
	while True:

		data1 = "# LOG:CAT=orderbooks_stats:MSG= "
		data2 = " ".join(
			key + ":" + str(value) for key, value in
			symbol_orderbook_count_for_5_minutes.items() if
			value != 0)
		sys.stdout.write(data1 + data2)
		sys.stdout.write("\n")
		for key in symbol_orderbook_count_for_5_minutes:
			symbol_orderbook_count_for_5_minutes[key] = 0

		await asyncio.sleep(300)

async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL+f"{symbol}", ping_interval=None):
		try:

			async for data in ws:

				while True:
					data = await ws.recv()

					dataJSON = json.loads(data)

					if "asks" in dataJSON or "bids" in dataJSON:

						try:

							if len(dataJSON['asks']) != 0 or len(dataJSON['bids']) != 0:
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
