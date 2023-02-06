# Algohouse-fetchers

Here are fetchers for listed down exchanges to get trades, snapshots and deltas for all available currencies on certain exchange:
 WhiteBit(python, websocket)
 Huobi(python, websocket)
 BitMart(python, websocket)
 
 
Rules for writting fetchers:
  1. use python(later maybe go)
  2. use websocket(sdk also may be used if it is more productive)
  3. use asyncio(instead of threading to reduce load on servers)
  4. divide code in functions(use main function as a start fucntion)
  5. comment your code and use meaningful names for functions and variables
  6. create your own branch for your file like '<exchange name>-fetcher'
  7. name your fetcher file in the same way
  8. don`t forget to write that your fetcher is taken here https://docs.google.com/spreadsheets/d/1-H__DoZUpnFQVFgJ8vkQAWFqBcY6ROOTWN-IEE9wwj4/edit#gid=0
 
