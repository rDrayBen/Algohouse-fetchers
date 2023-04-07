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
11. Bitforex(python, websocket)
12. Hitbtc(python, websocket)
13. Changelly-pro(python, websocket)
14. Phemex(python, websocket)
15. Toobit(python, websocket)
16. Trubit-pro(python, websocket)
17. Valr(python, websocket)
18. DeltaExchange(python, websocket)
19. FMFW(python, websocket)
20. LocalTrade(python, websocket)
21. XtCom(python, websocket)
22. Kuna(python, websocket)
23. Bitcointrade(python, websocket)
 
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
 
