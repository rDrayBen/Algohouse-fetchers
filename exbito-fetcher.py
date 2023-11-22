import json
import requests
import websockets
import time
import asyncio
import os
import sys

currency_url = 'https://api.exbito.com/apiv2/markets'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://wsapi.exbito.com/wsapiv2'

for element in currencies:
	list_currencies.append(element["name"])

#for trades count stats
symbol_trade_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_trade_count_for_5_minutes[list_currencies[i].upper()] = 0

#for orderbooks count stats
symbol_orderbook_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_orderbook_count_for_5_minutes[list_currencies[i].upper()] = 0


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies:
		pair_data = '@MD ' + pair["baseCurrencySymbol"] + '_' + pair["quoteCurrencySymbol"] + ' spot ' + \
					pair["baseCurrencySymbol"] + ' ' + pair["quoteCurrencySymbol"] + \
					' ' + str(pair['moneyPrec']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if 'deals' in trade_data["body"]:
		for elem in trade_data["body"]["deals"]:
			print('!', get_unix_time(), trade_data["body"]['market'],
				  "S" if elem["type"] == "sell" else "B", elem['price'],
				  elem["amount"], flush=True)
			symbol_trade_count_for_5_minutes[trade_data["body"]['market']] += 1


def get_order_books(var, update):
	order_data = var
	if 'asks' in order_data['body'] and len(order_data["body"]["asks"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['body']['market']] += len(order_data["body"]["asks"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['body']['market'] + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["body"]["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data['body'] and len(order_data["body"]["bids"]) != 0:
		symbol_orderbook_count_for_5_minutes[order_data['body']['market']] += len(order_data["body"]["bids"])
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['body']['market'] + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["body"]["bids"])
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
				# create the subscription for trades
				await ws.send(json.dumps({
					"action": "subscribe",
					"channel": "market.deals",
					"params": {
						"market": f"{list_currencies[i]}",
					}
				}))

				if (os.getenv("SKIP_ORDERBOOKS") == None):
					# create the subscription for full orderbooks and updates
					await ws.send(json.dumps({
						"action": "subscribe",
						"channel": "market.depth",
						"params": {
							"market": f"{list_currencies[i]}",
							"interval": "0"
						}
					}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "event" in dataJSON and dataJSON["event"]!="subscribed" and dataJSON["event"]!="error":

					try:

						# if received data is about trades
						if dataJSON['channel'] == 'market.deals' and dataJSON['event'] == 'insert':
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['channel'] == 'market.depth' and dataJSON['event'] == 'update':
							get_order_books(dataJSON, update=True)

						# if received data is about orderbooks
						if dataJSON['channel'] == 'market.depth' and dataJSON['event'] == 'init':
							get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred", data)
						time.sleep(1)

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")
			time.sleep(1)


asyncio.run(main())
