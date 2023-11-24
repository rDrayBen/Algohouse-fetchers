import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';


// define the websocket and REST URLs
const wsUrl = 'wss://ws.coinstore.com/s/ws';
const restUrl = "https://api.coinstore.com/api/v1/market/tickers";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trade_req = [];
var order_req = [];
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['instrumentId']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item)=>{
        if(item['symbol'].toUpperCase().includes('USDT')){
            trades_count_5min[item['symbol'].toUpperCase()] = 0;
            orders_count_5min[item['symbol'].toUpperCase()] = 0;
            let pair_data = '@MD ' + item['symbol'].toUpperCase() + ' spot ' + item['symbol'].toUpperCase().replace('USDT', '') 
            + ' USDT' + ' -1 1 1 0 0';
            console.log(pair_data);
        }
        
    });
    trades_count_5min['MANDOXETH'] = 0;
    orders_count_5min['MANDOXETH'] = 0;
    trades_count_5min['DUDEUSDC'] = 0;
    orders_count_5min['DUDEUSDC'] = 0;
    trades_count_5min['ERTHBTC'] = 0;
    orders_count_5min['ERTHBTC'] = 0;
    trades_count_5min['JPYCUSDC'] = 0;
    orders_count_5min['JPYCUSDC'] = 0;
    trades_count_5min['ANKRBNBBNB'] = 0;
    orders_count_5min['ANKRBNBBNB'] = 0;
    trades_count_5min['ANKRETHETH'] = 0;
    orders_count_5min['ANKRETHETH'] = 0;
    trades_count_5min['ROYSYFLAG'] = 0;
    orders_count_5min['ROYSYFLAG'] = 0;
    trades_count_5min['P2PSETH'] = 0;
    orders_count_5min['P2PSETH'] = 0;
    console.log('@MD MANDOXETH spot MANDOX ETH -1 1 1 0 0');
    console.log('@MD DUDEUSDC spot DUDE USDC -1 1 1 0 0');
    console.log('@MD ERTHBTC spot ERTH BTC -1 1 1 0 0');
    console.log('@MD JPYCUSDC spot JPYC USDC -1 1 1 0 0');
    console.log('@MD ANKRBNBBNB spot ANKRBNB BNB -1 1 1 0 0');
    console.log('@MD ANKRETHETH spot ANKRETH ETH -1 1 1 0 0');
    console.log('@MD ROYSYFLAG spot ROYSY FLAG -1 1 1 0 0');
    console.log('@MD P2PSETH spot P2PS ETH -1 1 1 0 0');
    console.log('@MDEND')
}

function FormReq(){
    currencies.forEach((number)=>{
        trade_req.push(`${number}@trade`);
        order_req.push(`${number}@depth@100`);
    });
    
}
// "4@trade"
// "4@snapshot_depth@20@0.01"


async function getTrades(message){
    if (message['symbol'] in trades_count_5min){
        trades_count_5min[message['symbol']] += 1;
    }else{
        trades_count_5min[message['symbol']] = 1;
    }
    
    var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + message['symbol'] + ' ' + 
        message['takerSide'][0].toUpperCase() + ' ' + parseFloat(message['price']).noExponents() + 
        ' ' + parseFloat(message['volume']).noExponents();
    console.log(trade_output);
}


async function getOrders(message){
    
    // check if bids array is not Null
    if(message['b']){
        if(message['symbol'] in orders_count_5min){
            orders_count_5min[message['symbol']] += message['b'].length;
        }else{
            orders_count_5min[message['symbol']] = message['b'].length;
        }
        
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['symbol'] + ' B ';
        var pq = '';
        for(let i = 0; i < message['b'].length; i++){
            pq += parseFloat(message['b'][i][1]).noExponents() + '@' + parseFloat(message['b'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }

    // check if asks array is not Null
    if(message['a']){
        if(message['symbol'] in orders_count_5min){
            orders_count_5min[message['symbol']] += message['a'].length;
        }else{
            orders_count_5min[message['symbol']] = message['a'].length;
        }
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['symbol'] + ' S '
        var pq = '';
        for(let i = 0; i < message['a'].length; i++){
            pq += parseFloat(message['a'][i][1]).noExponents() + '@' + parseFloat(message['a'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }
}


async function sendStats(){
    commonFunctions.stats(trades_count_5min, orders_count_5min);
    setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
}


async function ConnectTrades(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "op": "pong",
                    "epochMillis": commonFunctions.getUnixTime()
                }
              ));
              console.log('Ping request sent');
            }
          }, 120000);
        ws.send(JSON.stringify(
            {
                "op":"SUB",
                "channel":trade_req,
                "id":1
            }
        ));
        
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (!('data' in dataJSON) && dataJSON['T'] === 'trade'){
                getTrades(dataJSON);
            }else if('data' in dataJSON){
                // skip trades history
            }else{
                console.log(dataJSON);
            }
        }catch(e){
            // skip confirmation messages cause they can`t be parsed into JSON format without an error
            (async () => {
                await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
                console.log(event.data);
              })();
        }
        
        
    };


    // func to handle closing connection
    ws.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection closed with code ${event.code} and reason ${event}`);
        } else {
            console.log('Connection trade lost');
            setTimeout(async function() {
                ConnectTrades();
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
        (async () => {
            await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
          })();
    };
}

async function ConnectOrders(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "op": "pong",
                    "epochMillis": commonFunctions.getUnixTime()
                }
              ));
              console.log('Ping request sent');
            }
        }, 120000);
        
        ws.send(JSON.stringify(
            {
                "op":"SUB",
                "channel":order_req,
                "id":1
            }
        ));

    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if(dataJSON['T'] === 'depth' && 'a' in dataJSON && 'b' in dataJSON){
                getOrders(dataJSON);
            }else{
                console.log(dataJSON);
            }
        }catch(e){
            // skip confirmation messages cause they can`t be parsed into JSON format without an error
            (async () => {
                await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
                console.log(event.data);
              })();
        }
        
        
    };


    // func to handle closing connection
    ws.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection closed with code ${event.code} and reason ${event}`);
        } else {
            console.log('Connection order lost');
            setTimeout(async function() {
                ConnectOrders();
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
        (async () => {
            await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
          })();
    };
}

Metadata();
setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
FormReq();
ConnectTrades();
if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    ConnectOrders();
}


