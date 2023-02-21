# Algohouse-fetchers

Here are fetchers for listed down exchanges to get trades, snapshots and deltas for all available currencies on certain exchange:
 1. WhiteBit(python, websocket)
 2. Huobi(python, websocket)
 3. BitMart(python, websocket)
 4. Poloniex(python, websocket)
 5. BigOne(python, websocket)
 6. BitKub(python, websocket)
 7. Tidex(python, websocket) - fixing
 8. WOO(python, websocket)
 9. EXMO(python, websocket)
10. Bitget(python, websocket) 
 
Rules for writting fetchers:
  1. use python(later maybe go)
  2. use websockets lib(sdk also may be used if it is more productive)
  3. use asyncio(instead of threading to reduce load on servers)
  4. divide code in functions(use main function as a start fucntion)
  5. comment your code and use meaningful names for functions and variables
  6. don`t push in main
  7. create your own branch for your file like '<your exchange name>-exchange'
  8. name your fetcher file in the same way
  9. don`t forget to write that your fetcher is taken here https://docs.google.com/spreadsheets/d/1-H__DoZUpnFQVFgJ8vkQAWFqBcY6ROOTWN-IEE9wwj4/edit#gid=0
 
