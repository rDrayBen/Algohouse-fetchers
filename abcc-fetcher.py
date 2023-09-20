import json
import requests
import websockets
import time
import asyncio

currency_url = 'https://abcc.com/graphql'
answer = requests.get(currency_url)
currencies = answer.json()
list_currencies = list()
WS_URL = 'wss://push.abcc.com/app/2d1974bfdde17e8ecd3e7f0f6e39816b?protocol=7&client=js&version=4.4.0&flash=false'

list_currencies = ["ethbtc", "btcusdt", "ethusdt", "bateth", "zrxusdt", "atusdt", "tskusdt", "hkmtusdt",
				   "dotusdt", "sandusdt", "linkusdt", "uniusdt", "batusdt", "sushiusdt", "bnbusdt",
				   "chzusdt", "manausdt", "usdcusdt", "tusdusdt", "aaveusdt", "crvusdt", "maticusdt",
				   "enjusdt", "shibusdt", "rlcusdt", "hyveusdt", "btcusdc", "ethusdc", "yfiusdt",
				   "compusdt", "snxusdt", "mkrusdt", "grtusdt", "reefusdt", "renusdt", "kncusdt",
				   "ankrusdt", "1inchusdt", "axsusdt", "cakeusdt", "aliceusdt", "lrcusdt", "bakeusdt",
				   "degousdt", "ontusdt", "linkusdc", "dotusdc", "bnbbtc", "dotbtc", "unibtc", "linkbtc",
				   "maticbtc", "axsbtc", "aavebtc", "cakebtc", "uniusdc", "bnbusdc", "maticusdc", "aaveusdc",
				   "shibusdc", "cakeusdc", "axsusdc", "bnbeth", "linketh", "maticeth", "axseth", "grteth",
				   "manaeth", "aaveeth", "enjeth", "sandeth", "lrcusdc", "manausdc", "sandusdc", "batusdc",
				   "enjusdc", "chzusdc", "yfiusdc", "crvusdc", "mkrusdc", "grtusdc", "sushiusdc", "aliceusdc",
				   "compusdc", "snxusdc", "1inchusdc", "ankrusdc", "zrxusdc", "kncusdc", "snapusdt", "rlcusdc",
				   "reefusdc", "renusdc", "bakeusdc", "degousdc", "ontusdc", "mcbusdt", "ethaxusdt", "xsgdusdt",
				   "xsgdusdc", "usdmusdt", "usdmusdc"]

list_currencies_splited = ["eth/btc", "btc/usdt", "eth/usdt", "bat/eth", "zrx/usdt", "at/usdt", "tsk/usdt", "hkmt/usdt",
				   "dot/usdt", "sand/usdt", "link/usdt", "uni/usdt", "bat/usdt", "sushi/usdt", "bnb/usdt",
				   "chz/usdt", "mana/usdt", "usdc/usdt", "tusd/usdt", "aave/usdt", "crv/usdt", "matic/usdt",
				   "enj/usdt", "shib/usdt", "rlc/usdt", "hyve/usdt", "btc/usdc", "eth/usdc", "yfi/usdt",
				   "comp/usdt", "snx/usdt", "mkr/usdt", "grt/usdt", "reef/usdt", "ren/usdt", "knc/usdt",
				   "ankr/usdt", "1inch/usdt", "axs/usdt", "cake/usdt", "alice/usdt", "lrc/usdt", "bake/usdt",
				   "dego/usdt", "ont/usdt", "link/usdc", "dot/usdc", "bnb/btc", "dot/btc", "uni/btc", "link/btc",
				   "matic/btc", "axs/btc", "aave/btc", "cake/btc", "uni/usdc", "bnb/usdc", "matic/usdc", "aave/usdc",
				   "shib/usdc", "cake/usdc", "axs/usdc", "bnb/eth", "link/eth", "matic/eth", "axs/eth", "grt/eth",
				   "mana/eth", "aave/eth", "enj/eth", "sand/eth", "lrc/usdc", "mana/usdc", "sand/usdc", "bat/usdc",
				   "enj/usdc", "chz/usdc", "yfi/usdc", "crv/usdc", "mkr/usdc", "grt/usdc", "sushi/usdc", "alice/usdc",
				   "comp/usdc", "snx/usdc", "1inch/usdc", "ankr/usdc", "zrx/usdc", "knc/usdc", "snap/usdt", "rlc/usdc",
				   "reef/usdc", "ren/usdc", "bake/usdc", "dego/usdc", "ont/usdc", "mcb/usdt", "ethax/usdt", "xsgd/usdt",
				   "xsgd/usdc", "usdm/usdt", "usdm/usdc"]

# get metadata about each pair of symbols
async def metadata():
	for pair in list_currencies_splited:
		pair_data = '@MD ' + pair.split("/")[0] + pair.split("/")[1] + ' spot ' + \
					pair.split("/")[0] + ' ' + pair.split("/")[1] + \
					' ' + '-1 1 1 0 0'

		print(pair_data, flush=True)

	print('@MDEND')


def get_unix_time():
	return round(time.time() * 1000)


def get_trades(var):
	if 'data' in var:
		trade_data = json.loads(var["data"])
		for elem in trade_data["trades"]:
			print('!', get_unix_time(), var["channel"].split("-")[1],
				"B" if elem["type"] == "buy" else "S", elem['price'],
				elem["amount"], flush=True)


def get_order_books(var, update):
	order_data = json.loads(var["data"])
	if 'asks' in order_data and len(order_data["asks"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + var["channel"].split("-")[1] + ' S '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["asks"])
		answer = order_answer + pq
		# checking if the input data is full orderbook or just update
		if (update == True):
			print(answer)
		else:
			print(answer + " R")

	if 'bids' in order_data and len(order_data["bids"]) != 0:
		order_answer = '$ ' + str(get_unix_time()) + " " + var["channel"].split("-")[1] + ' B '
		pq = "|".join(str(el[1]) + "@" + str(el[0]) for el in order_data["bids"])
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


async def main():
	# create connection with server via base ws url
	async for ws in websockets.connect(WS_URL, ping_interval=None):
		try:
			# create task to keep connection alive
			pong = asyncio.create_task(heartbeat(ws))

			# create task to get metadata about each pair of symbols
			meta_data = asyncio.create_task(metadata())

			for i in range(len(list_currencies)):
				# create the subscription for trades
				await ws.send(json.dumps({
					"event": "pusher:subscribe",
					"data": {
						"channel": f"trade-{list_currencies[i]}"
					}
				}))

				# create the subscription for full orderbooks and updates
				await ws.send(json.dumps({
					"event": "pusher:subscribe",
					"data": {
						"channel": f"incr-{list_currencies[i]}"
					}
				}))

			while True:
				data = await ws.recv()

				dataJSON = json.loads(data)

				if "event" in dataJSON:

					try:

						# if received data is about trades
						if dataJSON['event'] == 'trades':
							get_trades(dataJSON)

						# if received data is about updates
						if dataJSON['event'] == 'depth-update':
							get_order_books(dataJSON, update=True)

						# # if received data is about orderbooks
						# if dataJSON['method'] == 'snapshotOrderbook':
						# 	get_order_books(dataJSON, update=False)

						else:
							pass

					except Exception as ex:
						print(f"Exception {ex} occurred")

		except Exception as conn_ex:
			print(f"Connection exception {conn_ex} occurred")


asyncio.run(main())
