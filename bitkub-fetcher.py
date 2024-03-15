import json
import requests
import websockets
import time
import asyncio
from CommonFunctions.CommonFunctions import get_unix_time, print_stats

currency_url = 'https://api.bitkub.com/api/market/symbols'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
list_currencies_id = list()
WS_URL = 'wss://api.bitkub.com/websocket-api/'
WS_URL_trades = WS_URL + f"market.trade."
WS_URL_orderbooks = WS_URL + f"orderbook/"

trades_stats = {}
orderbooks_stats = {}

for element in currencies["result"]:
	list_currencies.append(element["symbol"])
	list_currencies_id.append(element["id"])
	trades_stats[element["symbol"].upper()] = 0
	orderbooks_stats[element["symbol"].upper()] = 0


currency_dict = {currency: currency_id for currency, currency_id in zip(list_currencies, list_currencies_id)}


def create_trades_chanels(symbol: str):
	return WS_URL_trades + symbol.lower()


def create_orderbook_chanels(symbol_id: int):
	return WS_URL_orderbooks + str(symbol_id)


response = requests.get(currency_url)
trade_messages = [create_trades_chanels(x) for x in list_currencies]
orderbook_messages = [create_orderbook_chanels(x) for x in list_currencies_id]
symbols_ = {x['id']: x['symbol'] for x in response.json()['result']}


# get metadata about each pair of symbols
async def metadata():
	data = requests.get(currency_url)
	for pair in data.json()['result']:
		print("@MD", pair['symbol'], "spot", pair['symbol'].split("_")[0], pair['symbol'].split("_")[1],
			  "-1", "1", "1", "0", "0", end="\n")
	print('@MDEND')


def print_trade(var):
	trade_data = var
	print("!", get_unix_time(), trade_data['sym'], "B" if trade_data['txn'].find("SELL") == -1 else "S", trade_data['rat'],
		  "{:f}".format(trade_data['amt']).rstrip('0').rstrip('.'), end="\n")
	trades_stats[trade_data['sym'].upper()] += 1


def print_orderbook(var, askschanged):
	order_data = var
	symbol_id = order_data["pairing_id"]
	symbol = -1

	for sym, id in currency_dict.items():
		if id == symbol_id:
			symbol = sym
			break

	if symbol == -1:
		print(f"Exception symbol_id occurred")

	data = []

	for i in order_data['data']:
		data.append(i)


	if askschanged == True:
		order_answer1 = f"$ {str(get_unix_time())} {symbol} S "
		order_answer2 = ""
		index = 0
		for el in data:
			if index == 0:
				order_answer2 += f"{el[2]:.8f}@{el[1]}"
			order_answer2 += f"|{el[2]:.8f}@{el[1]}"
			index += 1
		order_answer = order_answer1 + order_answer2
		print(order_answer + " R")
		orderbooks_stats[symbol.upper()] += 1

	if askschanged == False:
		order_answer1 = f"$ {str(get_unix_time())} {symbol} B "
		order_answer2 = ""
		index = 0
		for el in data:
			if index == 0:
				order_answer2 += f"{el[2]:.8f}@{el[1]}"
			order_answer2 += f"|{el[2]:.8f}@{el[1]}"
			index += 1
		order_answer = order_answer1 + order_answer2
		print(order_answer + " R")
		orderbooks_stats[symbol.upper()] += 1


async def socket(url):
	while True:
		try:
			ws = await websockets.connect(url)
			async for data in ws:
				try:
					dataJSON = json.loads(data)
					if 'txn' in dataJSON or 'event' in dataJSON:
						if 'txn' in dataJSON:
							print_trade(dataJSON)
						elif dataJSON['event'] == "askschanged":
							print_orderbook(dataJSON, askschanged=True)
						elif dataJSON['event'] == "bidschanged":
							print_orderbook(dataJSON, askschanged=False)
						else:
							pass
				except Exception as e:
					print(f"Exception: {e}")
					break
		except KeyboardInterrupt:
			print("Keyboard Interupt")
			exit(1)
		except Exception as conn_c:
			print(f"WARNING: connection exception {conn_c} occurred")
			await ws.close()
			await asyncio.sleep(1)
			continue


async def handler():
	meta_data = asyncio.create_task(metadata())
	stats_task = asyncio.create_task(print_stats(trades_stats, orderbooks_stats))
	tasks = []
	for element in trade_messages + orderbook_messages:
		tasks.append(asyncio.create_task(socket(element)))
		await asyncio.sleep(0.1)

	await asyncio.wait(tasks)


async def main():
	while True:
		await handler()
		await asyncio.sleep(300)


asyncio.run(main())
