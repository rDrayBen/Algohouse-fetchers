import json
import requests
import websockets
import time
import asyncio
from CommonFunctions.CommonFunctions import get_unix_time, stats

currency_url = 'https://www.fubthk.com/api/v2/trade/public/markets?limit=1000'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://www.fubthk.com/api/v2/ranger/public/?stream=global.tickers'

#for trades count stats and for orderbooks count stats
symbol_trade_count_for_5_minutes = {}
symbol_orderbook_count_for_5_minutes = {}

for element in currencies:
	list_currencies.append(element["id"].upper())
	symbol_trade_count_for_5_minutes[element["id"].upper()] = 0
	symbol_orderbook_count_for_5_minutes[element["id"].upper()] = 0

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies:
		pair_data = '@MD ' + pair["id"].upper() + ' spot ' + \
					pair["base_unit"].upper() + ' ' + pair["quote_unit"].upper() + \
					' ' + str(pair['price_precision']) + ' 1 1 0 0'
		print(pair_data, flush=True)
	print('@MDEND')

async def subscribe(ws, symbol):
	# create the subscription for trades
	await ws.send(json.dumps({
		"event": "subscribe",
		"streams": [
			f"{symbol.lower()}.trades",
			f"{symbol.lower()}.update"
		]
	}))

	await ws.send(json.dumps({
		"event": "subscribe",
		"streams": [
			f"{symbol.lower()}.trades",
			f"{symbol.lower()}.update"
		]
	}))

	await ws.send(json.dumps({
		"event": "subscribe",
		"streams": [
			f"{symbol.lower()}.trades",
			f"{symbol.lower()}.update"
		]
	}))

	await asyncio.sleep(300)

def get_trades(var, symbol):
	trade_data = var
	if len(trade_data[f"{symbol.lower()}.trades"]["trades"]) != 0:
		for elem in trade_data[f"{symbol.lower()}.trades"]["trades"]:
			print('!', get_unix_time(), symbol.upper(),
				  "B" if elem["taker_type"] == "buy" else "S", elem['price'],
				  elem["amount"], flush=True)
			symbol_trade_count_for_5_minutes[symbol] += 1

async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"event": "ping"
		}))
		await asyncio.sleep(5)

# trade and orderbook stats output
async def print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes):
	time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
	if time_to_wait != 300:
		await asyncio.sleep(time_to_wait)
	while True:
		stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes)
		await asyncio.sleep(1)
		time_to_wait = (5 - ((time.time() / 60) % 5)) * 60
		await asyncio.sleep(time_to_wait)

async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))
			# create task to subscribe trades and orderbooks
			subscription = asyncio.create_task(subscribe(ws, symbol))
			async for data in ws:
				try:
					dataJSON = json.loads(data)

					if f"{symbol.lower()}.trades" in dataJSON:
						get_trades(dataJSON, symbol)

				except Exception as ex:
					print(f"Exception {ex} occurred", data)
					time.sleep(1)
				except:
					pass

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")
			time.sleep(1)
		except:
			continue

async def handler():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(
		print_stats(symbol_trade_count_for_5_minutes, symbol_orderbook_count_for_5_minutes))
	tasks = []
	for symbol in list_currencies:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(1)

	await asyncio.wait(tasks)

async def main():
	while True:
		await handler()
		await asyncio.sleep(300)

asyncio.run(main())
