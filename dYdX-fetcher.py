import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://api.dydx.exchange/v3/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://api.dydx.exchange/v3/ws'

for key, value in currencies["markets"].items():
	if value["status"] == 'ONLINE':
		list_currencies.append(value["market"])

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i]] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i]] = 0


# get metadata about each pair of symbols
async def metadata():
	for key, value in currencies["markets"].items():
		pair_data = '@MD ' + value["baseAsset"] + '-' + value["quoteAsset"] + ' spot ' + \
					value["baseAsset"] + ' ' + value["quoteAsset"] + \
					' ' + str(str(value['tickSize'])[::-1].find('.')) + ' 1 1 0 0'

		print(pair_data, flush=True)
	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if (len(trade_data["contents"]["trades"]) < 15):
		for elem in trade_data["contents"]["trades"]:
			print('!', get_unix_time(), trade_data["id"],
				  "B" if elem["side"] == "BUY" else "S", elem['price'],
				  elem["size"], flush=True)
			symbol_trade_count_for_5_minutes[trade_data["id"]] += 1

def get_orderbook_snapshots(var):
	order_data = var
	if ('asks' in order_data['contents'] and len(order_data["contents"]["asks"]) != 0) \
			or ('bids' in order_data['contents'] and len(order_data["contents"]["bids"]) != 0):
		symbol_orderbook_count_for_5_minutes[order_data['id']] += len(order_data["contents"]["asks"])
		order_answer_S = '$ ' + str(get_unix_time()) + " " + order_data['id'] + ' S '
		pq_S = "|".join(el["size"] + "@" + el["price"] for el in order_data["contents"]["asks"])
		answer_S = order_answer_S + pq_S
		print(answer_S + " R")

		symbol_orderbook_count_for_5_minutes[order_data['id']] += len(order_data["contents"]["bids"])
		order_answer_B = '$ ' + str(get_unix_time()) + " " + order_data['id'] + ' B '
		pq_B = "|".join(el["size"] + "@" + el["price"] for el in order_data["contents"]["bids"])
		answer_B = order_answer_B + pq_B
		print(answer_B + " R")


def get_orderbook_updates(var):
	order_data = var
	if 'asks' in order_data["contents"] and len(order_data["contents"]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['id']] += len(order_data["contents"]["asks"])
		order_answer_S = '$ ' + str(get_unix_time()) + " " + order_data['id'] + ' S '
		pq_S = "|".join(el[1] + "@" + el[0] for el in order_data["contents"]["asks"])
		answer_S = order_answer_S + pq_S
		print(answer_S)

	if 'bids' in order_data["contents"] and len(order_data["contents"]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['id']] += len(order_data["contents"]["bids"])
		order_answer_B = '$ ' + str(get_unix_time()) + " " + order_data['id'] + ' B '
		pq_B = "|".join(el[1] + "@" + el[0] for el in order_data["contents"]["bids"])
		answer_B = order_answer_B + pq_B
		print(answer_B)


async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"type": "ping"
		}))
		await asyncio.sleep(5)

#trade and orderbook stats output
async def print_stats():
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	if time_to_wait != 300:
		await asyncio.sleep(time_to_wait)
	while True:
		data1 = "# LOG:CAT=trades_stats:MSG= "
		data2 = " ".join(
			key.upper() + ":" + str(value) for key, value in symbol_trade_count_for_5_minutes.items() if value != 0)
		sys.stdout.write(data1 + data2)
		sys.stdout.write("\n")
		for key in symbol_trade_count_for_5_minutes:
			symbol_trade_count_for_5_minutes[key] = 0

		data3 = "# LOG:CAT=orderbooks_stats:MSG= "
		data4 = " ".join(
			key.upper() + ":" + str(value) for key, value in symbol_orderbook_count_for_5_minutes.items() if
			value != 0)
		sys.stdout.write(data3 + data4)
		sys.stdout.write("\n")
		for key in symbol_orderbook_count_for_5_minutes:
			symbol_orderbook_count_for_5_minutes[key] = 0
		await asyncio.sleep(300)


async def main():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats())
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			for i in range(len(list_currencies)):
				# create the subscription for full orderbooks and updates
				if (os.getenv("SKIP_ORDERBOOKS") == None): # don't subscribe or report orderbook changes
					await ws.send(json.dumps({
						"type": "subscribe",
						"channel": "v3_orderbook",
						"id": f"{list_currencies[i]}",
						"batched": False
					}))
				# create the subscription for trades
				await ws.send(json.dumps({
					"type": "subscribe",
					"channel": "v3_trades",
					"id": f"{list_currencies[i]}"
				}))
			while True:

				data = await ws.recv()
				dataJSON = json.loads(data)

				if "channel" in dataJSON:
					try:
						# if received data is about trades
						if dataJSON['channel'] == 'v3_trades':
							get_trades(dataJSON)
						elif dataJSON['channel'] == 'v3_orderbook':
							# if received data is orderbook snapshots
							if dataJSON["type"] == "subscribed":
								get_orderbook_snapshots(dataJSON)
							# if received data is orderbook updates
							if dataJSON["type"] == "channel_data":
								get_orderbook_updates(dataJSON)
							else:
								pass
						else:
							pass
					except Exception as ex:
						print(f"Exception {ex} occurred")
		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
