import json
import requests
import websockets
import time
import asyncio


currency_url = 'https://exmarkets.com/api/v1/general/info'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies_id = list()
list_currencies_name = list()
WS_URL = 'wss://exmarkets.com/ws'


for element in currencies["markets"]:
	list_currencies_id.append(element["id"])
	list_currencies_name.append(element["name"])

async def subscribe(ws, symbol):

	await ws.send(json.dumps({
		"e": "init",
	}))

	# create the subscription for trades and orderbooks
	await ws.send(json.dumps({
		"e": "market",
		"chartInterval": "1w",
		"marketId": symbol
	}))

	await asyncio.sleep(300)


# get metadata about each pair of symbols
async def metadata():
	for pair in currencies["markets"]:
		pair_data = '@MD ' + pair["name"].split("-")[0] + '-' + pair["name"].split("-")[1] + ' spot ' + \
					pair["name"].split("-")[0] + ' ' + pair["name"].split("-")[1] + \
					' ' + str(pair["quote_precision"]) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	trade_data = var
	if 'data' in trade_data:
		print('!', get_unix_time(), trade_data["data"]["market"].upper(),
				"S" if trade_data["data"]["side"] == "SELL" else "B", str(format(trade_data["data"]['price'], '.5f')),
				str(trade_data["data"]['amount']), flush=True)


def get_order_books(var):
	order_data = var
	if 'sell' in order_data['data'] and len(order_data["data"]["sell"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['market'].upper() + ' S '
		pq = "|".join(str(el['amount']) + "@" + str(format(el['price'], '.5f')) for el in order_data["data"]["sell"])
		answer = order_answer + pq

		print(answer + " R")

	if 'buy' in order_data['data'] and len(order_data["data"]["buy"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['market'].upper() + ' B '
		pq = "|".join(str(el['amount']) + "@" + str(format(el['price'], '.5f')) for el in order_data["data"]["buy"])
		answer = order_answer + pq

		print(answer + " R")


async def socket(symbol):
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:

			subscription = asyncio.create_task(subscribe(ws, symbol))

			async for data in ws:

				try:
					dataJSON = json.loads(data)

					# if received data is about trades
					if dataJSON['type'] == 'market-trade':
						get_trades(dataJSON)

					# if received data is about orderbooks
					if dataJSON['type'] == 'market-orderbook':
						get_order_books(dataJSON)

					else:
						pass

				except Exception as ex:
					print(f"Exception {ex} occurred")
				except:
					pass

		except:
			continue


async def handler():
	meta_data = asyncio.create_task(metadata())
	tasks = []
	for symbol in list_currencies_id:
		tasks.append(asyncio.create_task(socket(symbol)))
		await asyncio.sleep(0.1)

	await asyncio.wait(tasks)

async def main():
	while True:
		await handler()
		await asyncio.sleep(300)


asyncio.run(main())
