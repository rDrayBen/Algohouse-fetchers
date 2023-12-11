import time
import sys
import asyncio


# get time in unix format
def get_unix_time():
    return round(time.time() * 1000)


# trade and orderbook stats output
def stats(trade_count_5_minutes, orderbook_count_5_minutes):
    data1 = "# LOG:CAT=trades_stats:MSG= "
    data2 = " ".join(
        key.upper() + ":" + str(value) for key, value in trade_count_5_minutes.items() if value != 0)
    sys.stdout.write(data1 + data2)
    sys.stdout.write("\n")
    for key in trade_count_5_minutes:
        trade_count_5_minutes[key] = 0

    data3 = "# LOG:CAT=orderbooks_stats:MSG= "
    data4 = " ".join(
        key.upper() + ":" + str(value) for key, value in orderbook_count_5_minutes.items() if
        value != 0)
    sys.stdout.write(data3 + data4)
    sys.stdout.write("\n")
    for key in orderbook_count_5_minutes:
        orderbook_count_5_minutes[key] = 0


async def print_stats(trades_stats, orderbooks_stats):
    await asyncio.sleep((5 - ((time.time() / 60) % 5)) * 60)
    while True:
        trades_head = "#LOG:CAT=trades_stats:MSG="
        orders_head = "#LOG:CAT=orderbook_stats:MSG="

        trades_body = ''
        orders_body = ''

        for key, val in trades_stats.items():
            trades_body += f'{key}:{str(val)} ' if val > 0 else ''
            orders_body += f'{key}:{str(orderbooks_stats[key])} ' if orderbooks_stats[key] > 0 else ''

            trades_stats[key] = 0
            orderbooks_stats[key] = 0

        print(trades_head + trades_body, flush=True)
        print(orders_head + orders_body, flush=True)

        await asyncio.sleep(5)  # additional delay to prevent second output per one time
        await asyncio.sleep((5 - ((time.time() / 60) % 5)) * 60)


#list of currencies to skip for certain reasons
blacklisted_currencies = ["SBTC"]
def SKIP_CURRENCIES(pair):
	for elem in blacklisted_currencies:
		if elem.upper() in pair or elem.lower() in pair:
			return True
	return False
