import json
import requests
import websockets
import time
import asyncio
import sys

currency_url = 'https://www.fubthk.com/api/v2/trade/public/markets?limit=1000'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://www.fubthk.com/api/v2/ranger/public/?stream=global.tickers'
symbol_trade_count_for_5_minutes = {}

for element in currencies:
	list_currencies.append(element["id"])
	symbol_trade_count_for_5_minutes[element["id"]] = False

#for trades count stats
symbol_count_for_5_minutes = {}
for i in range(len(list_currencies)):
	symbol_count_for_5_minutes[list_currencies[i]] = 0

# get metadata about each pair of symbols
async def metadata():
	for pair in currencies:
		pair_data = '@MD ' + pair["id"] + ' spot ' + \
					pair["base_unit"] + ' ' + pair["quote_unit"] + \
					' ' + str(pair['price_precision']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


async def subscribe(ws):
	while True:
		for key, value in symbol_trade_count_for_5_minutes.items():

			if value == False:

				# create the subscription for trades
				await ws.send(json.dumps({
					"event": "subscribe",
					"streams": [
						f"{list_currencies[i]}.trades",
						f"{list_currencies[i]}.update"
					]
				}))


				await asyncio.sleep(0.1)

		for el in list(symbol_trade_count_for_5_minutes):
			symbol_trade_count_for_5_minutes[el] = False

		await asyncio.sleep(2000)

def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var, currency):
	trade_data = var
	if len(trade_data[f"{currency}.trades"]["trades"]) != 0:
		symbol_trade_count_for_5_minutes[currency] = True
		for elem in trade_data[f"{currency}.trades"]["trades"]:
			print('!', get_unix_time(), currency,
				  "B" if elem["taker_type"] == "buy" else "S", elem['price'],
				  elem["amount"], flush=True)
			symbol_count_for_5_minutes[currency] += 1



async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"event": "ping"
		}))
		await asyncio.sleep(5)


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


async def main():
	# create task to get metadata about each pair of symbols
	meta_data = asyncio.create_task(metadata())
	# create task to get trades and orderbooks stats output
	stats_task = asyncio.create_task(print_stats())
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:

			# create task to subscribe to symbols` pair
			subscription = asyncio.create_task(subscribe(ws))

			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))


			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				for i in range(len(list_currencies)):

					if f"{list_currencies[i]}.trades" in dataJSON:

						try:

							currency = list_currencies[i]

							get_trades(dataJSON, currency)

						except Exception as ex:
							print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
