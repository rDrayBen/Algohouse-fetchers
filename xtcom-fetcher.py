import json
import requests
import websockets
import time
import asyncio

# get all available symbol pairs
currency_url = 'https://sapi.xt.com/v4/public/symbol'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://stream.xt.com/public'

# check if the certain symbol pair is available
for element in currencies["result"]["symbols"]:
	if element["state"] == "ONLINE":
		list_currencies.append(element["symbol"])


# get time in unix format
def get_unix_time():
	return round(time.time() * 1000)


# put the trade information in output format
def get_trades(var):
	trade_data = var
	print('!', get_unix_time(), trade_data['data']['s'].upper(),
		  "B" if trade_data['data']["b"] else "S", trade_data['data']['p'],
		  trade_data['data']["q"], flush=True)


# put the orderbook and deltas information in output format
def get_order_books(var, depth_update):
	order_data = var
	if 'a' in order_data['data'] and len(order_data["data"]["a"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['s'].upper() + ' S '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["a"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'b' in order_data['data'] and len(order_data["data"]["b"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + order_data['data']['s'].upper() + ' B '
		pq = "|".join(el[1] + "@" + el[0] for el in order_data["data"]["b"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (depth_update == True):
			print(answer)
		else:
			print(answer + " R")


# process the situations when the server awaits "ping" request
async def heartbeat(ws):
	while True:
		await ws.send(json.dumps({
			"event": "ping"
		}))
		await asyncio.sleep(5)


async def main():
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))
			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"method": "subscribe",
					"params": [
						f"trade@{list_currencies[i]}"
					],
					"id": "123"
				}))

				# create the subscription for full orderbooks
				await ws.send(json.dumps({
					"method": "subscribe",
					"params": [
						f"depth@{list_currencies[i]},50"
					],
					"id": "123"
				}))

				# create the subscription for updates
				await ws.send(json.dumps({
					"method": "subscribe",
					"params": [
						f"depth_update@{list_currencies[i]}"
					],
					"id": "123"
				}))

			while True:

				data = await ws.recv()

				dataJSON = json.loads(data)

				if "topic" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['topic'] == 'trade':
							get_trades(dataJSON)

						# if received data is about updates
						elif dataJSON['topic'] == 'depth_update':
							get_order_books(dataJSON, depth_update=True)

						# if received data is about orderbooks
						elif dataJSON['topic'] == 'depth':
							get_order_books(dataJSON, depth_update=False)

						else:
							print(dataJSON)

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
