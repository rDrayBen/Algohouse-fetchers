import json
import requests
import asyncio
import time
import websockets

API_URL = 'https://sapi.xt.com/v4'
API_SPOT_SYMBOLS_URL = '/public/symbol'
WS_URL = 'wss://knprklvtrdkand.koinpark.com/socket.io/?EIO=4&transport=websocket'

TIMEOUT = 0.1
PING_TIMEOUT = 25
response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
symbols = [x['symbol'] for x in response.json()['result']['symbols']]

# get time in unix format
def get_unix_time():
	return round(time.time() * 1000)

# get metadata about each pair of symbols
async def metadata():
	for pair in response.json()["result"]["symbols"]:
		pair_data = '@MD ' + pair["baseCurrency"].upper() + '_' + pair["quoteCurrency"].upper() + ' spot ' + \
					pair["baseCurrency"].upper() + ' ' + pair["quoteCurrency"].upper() + \
					' ' + str(pair['pricePrecision']) + ' 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


async def heartbeat(ws):
	while True:
		await ws.send(message='3')
		await asyncio.sleep(PING_TIMEOUT)


# put the trade information in output format
def get_trades(var):
	trade_data = var
	print('!', get_unix_time(), trade_data[0].split("_", 1)[1],
		  "B" if trade_data[1]['data']["isBuyerMaker"] else "S", trade_data[1]['data']['price'],
		  trade_data[1]['data']["amount"], flush=True)


# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	pq_S = ""
	pq_B = ""
	iteration = 0
	for elem in order_data[1]['data']['changes']:
		if 'sell' in elem and len(elem) != 0:
			if iteration == 0:
				pq_S += elem[2] + "@" + elem[1]
			else:
				pq_S += "|" + elem[2] + "@" + elem[1]
		if 'buy' in elem and len(elem) != 0:
			if iteration == 0:
				pq_B += elem[2] + "@" + elem[1]
			else:
				pq_B += "|" + elem[2] + "@" + elem[1]

		iteration += 1

	if len(pq_S) != 0:
		order_answer_S = '$ ' + str(get_unix_time()) + " " + order_data[1]['productId'] + ' S '
		answer_S = order_answer_S + pq_S
		print(answer_S)

	if len(pq_B) != 0:
		order_answer_B = '$ ' + str(get_unix_time()) + " " + order_data[1]['productId'] + ' B '
		answer_B = order_answer_B + pq_B
		print(answer_B)



async def subscribe(ws):
	await ws.send(message='40')


async def main():
	meta_task = asyncio.create_task(metadata())
	async for ws in websockets.connect(WS_URL):
		try:
			sub_task = asyncio.create_task(subscribe(ws))
			ping_task = asyncio.create_task(heartbeat(ws))
			while True:
				try:
					data = await ws.recv()
					if data[0] == '4' and data[1] == '2':
						data_sliced = data[2:]
						dataJSON = json.loads(data_sliced)

						# if received data is about trades
						if "tradehistory" in dataJSON[0]:
							get_trades(dataJSON)
						# if received data is about orderbooks
						elif "orderBookMatch" in dataJSON[0]:
							get_order_books(dataJSON, depth_update=True)
						else:
							pass
				except:
					continue
		except:
			continue


asyncio.run(main())
