import time
import sys

# get time in unix format
def get_unix_time():
	return round(time.time() * 1000)

#trade and orderbook stats output
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


