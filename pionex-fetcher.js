import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const tradeWsUrl = 'wss://ws.pionex.com/wsPub';
const orderWsUrl = 'wss://stream.pionex.com/stream/v2';
const restUrl = "https://api.pionex.com/api/v1/common/symbols";


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};



// extract symbols from JSON returned information
for(let i = 0; i < myJson['data']['symbols'].length; ++i){
    if(myJson['data']['symbols'][i]['type'] === "SPOT"){
        currencies.push(myJson['data']['symbols'][i]['symbol']);
    }
}


// print metadata about pairs
async function Metadata(){
    myJson['data']['symbols'].forEach((item)=>{
        if(item['type'] === "SPOT"){
            trades_count_5min[item['symbol']] = 0;
            orders_count_5min[item['symbol']] = 0;
            let pair_data = '@MD ' + item['symbol'] + ' spot ' + item['baseCurrency'] + ' ' + item['quoteCurrency'] + ' ' 
            + item['quotePrecision'] + ' 1 1 0 0';
            console.log(pair_data);
        }
    })
    console.log('@MDEND')
}


// func to print trades
async function getTrades(message){
    message['data'].forEach((item)=>{
        trades_count_5min[item['symbol']] += 1;
        var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + 
        item['symbol'] + ' ' + 
        item['side'][0] + ' ' + item['price'] + ' ' + item['size'];
        console.log(trade_output);
    });
}


// func to print orderbooks and deltas
async function getOrders(message){
    // check if bids array is not Null
    if(message['data']['0']['a'].length > 0){
        orders_count_5min[message['data']['0']['b'] + '_' + message['data']['0']['q']] += message['data']['0']['a'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['data']['0']['b'] + '_' + message['data']['0']['q'] + ' B '
        var pq = '';
        for(let i = 0; i < message['data']['0']['a'].length; i++){
            pq += message['data']['0']['a'][i][1] + '@' + message['data']['0']['a'][i][0] + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R')
    }

    // check if asks array is not Null
    if(message['data']['0']['d'].length > 0){
        orders_count_5min[message['data']['0']['b'] + '_' + message['data']['0']['q']] += message['data']['0']['d'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['data']['0']['b'] + '_' + message['data']['0']['q'] + ' S '
        var pq = '';
        for(let i = 0; i < message['data']['0']['d'].length; i++){
            pq += message['data']['0']['d'][i][1] + '@' + message['data']['0']['d'][i][0] + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R')
    }
}


async function sendStats(){
    commonFunctions.stats(trades_count_5min, orders_count_5min);
    setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
}



function Connect1(index){
    var ws1 = new WebSocket(tradeWsUrl);
    // call this func when first opening connection
    ws1.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws1.readyState === WebSocket.OPEN) {
              ws1.send(JSON.stringify(
                {
                    "op": "PONG", 
                    "timestamp": commonFunctions.getUnixTime()
                }
              ));
              console.log('Ping request sent');
            }
          }, 20000);
        // ws1.ping();
        // sub for trades
        ws1.send(JSON.stringify(
            {
                "op": "SUBSCRIBE",
                "topic":  "TRADE", 
                "symbol": currencies[index]
            }
        ))
    };


    // func to handle input messages
    ws1.onmessage = function(event) {
        var dataJSON = JSON.parse(event.data);
        // console.log(dataJSON);
        if (dataJSON['topic'] === 'TRADE' && 'data' in dataJSON){
            getTrades(dataJSON);
        }else{
            console.log(dataJSON);
        }
    };


    // func to handle closing connection
    ws1.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 1 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 1 lost:', event);
            setTimeout(function() {
                Connect1(index);
                }, 500);
        }
    };

    // func to handle errors
    ws1.onerror = function(error) {
        console.log(`Error ${error.message} occurred in ws1`);
        (async () => {
            await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
          })();
    };
}

function Connect2(index){
    // create a new websocket instance
    var ws2 = new WebSocket(orderWsUrl);

    // call this func when first opening connection
    ws2.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws2.readyState === WebSocket.OPEN) {
              ws2.send(JSON.stringify(
                {
                    "op": "PONG", 
                    "timestamp": commonFunctions.getUnixTime()
                }
              ));
              console.log('Ping request sent');
            }
        }, 20000);
        // ws2.ping();
        // sub for orders
        ws2.send(JSON.stringify(
            {
                "action": "subscribe",
                "channel": "order.book",
                "data": [
                    {
                        "base": currencies[index].split('_')[0],
                        "quote": currencies[index].split('_')[1],
                        "exchange": "pionex.v2",
                        "interval": ""
                    }
                ]
            }
        ))       
        
             
    };


    // func to handle input messages
    ws2.onmessage = function(event) {
        var dataJSON = JSON.parse(event.data);
        // console.log(dataJSON);
        if (dataJSON['channel'] === "order.book"){
            getOrders(dataJSON);
        }else{
            console.log(dataJSON);
        }        
    };


    // func to handle closing connection
    ws2.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 2 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 2 lost');
            setTimeout(function() {
                Connect2(index);
                }, 500);
        }
    };

    // func to handle errors
    ws2.onerror = function(error) {
        console.log(`Error ${error.message} occurred in ws2`);
        (async () => {
            await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
          })();
    };
}

// call metadata to execute
Metadata();
setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);

// create an array of websocket connections to get trades for each trading pair
var wsTradeArr = [];
// create an array of websocket connections to get snapshots for each trading pair
var wsOrderArr = [];
for(let i = 0; i < currencies.length; i++){
    wsTradeArr.push(Connect1(i));
    if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
        wsOrderArr.push(Connect2(i));
    }
    
    await commonFunctions.sleep(500);
}

