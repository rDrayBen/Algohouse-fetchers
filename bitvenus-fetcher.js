import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const WsUrl = 'wss://www.bitvenus.me/ws/quote/v1';
const restUrl = "https://www.bitvenus.me/api/v1/basic/all_tokens";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    if(myJson[i]['tokenId'].includes('-SWAP-')){
        currencies.push(myJson[i]['tokenId']);
    }
}

// print metadata about pairs
async function Metadata(){
    myJson.forEach((item)=>{
        if(item['tokenId'].includes('-SWAP-')){
            trades_count_5min[item['tokenId'].split('-')[0] + '-' + item['tokenId'].split('-')[2]] = 0;
            orders_count_5min[item['tokenId'].split('-')[0] + '-' + item['tokenId'].split('-')[2]] = 0;
            let pair_data = '@MD ' + item['tokenId'].split('-')[0] + '-' + item['tokenId'].split('-')[2] + ' spot ' + 
                item['tokenId'].split('-')[0] + ' ' + item['tokenId'].split('-')[2] + ' ' + '-1' +  ' 1 1 0 0';
            console.log(pair_data);
        }
        
    })
    console.log('@MDEND')
}


async function getTrades(message){
    trades_count_5min[message['symbol'].replace('-SWAP', '')] += message['data'].length;
    message['data'].forEach((trade)=>{
        var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + message['symbol'].replace('-SWAP', '') + ' ' + 
            (trade['m'] ? 'B' : 'S') + ' ' + parseFloat(trade['p']).noExponents() + ' ' + parseFloat(trade['q']).noExponents();
        console.log(trade_output);
    })
}


async function getOrders(message, update){
    orders_count_5min[message['symbol'].replace('-SWAP', '')] += message['data'][0]['b'].length + message['data'][0]['a'].length;
    // check if bids array is not Null
    if(message['data'][0]['b'].length > 0){
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['symbol'].replace('-SWAP', '') + ' B '
        var pq = '';
        for(let i = 0; i < message['data'][0]['b'].length; i++){
            pq += parseFloat(message['data'][0]['b'][i][1]).noExponents() + '@' + parseFloat(message['data'][0]['b'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        if(update){
            console.log(order_answer + pq);
        }else{
            console.log(order_answer + pq + ' R');
        }
    }

    // check if asks array is not Null
    if(message['data'][0]['a'].length > 0){
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['symbol'].replace('-SWAP', '') + ' S '
        var pq = '';
        for(let i = 0; i < message['data'][0]['a'].length; i++){
            pq += parseFloat(message['data'][0]['a'][i][1]).noExponents() + '@' + parseFloat(message['data'][0]['a'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        if(update){
            console.log(order_answer + pq);
        }else{
            console.log(order_answer + pq + ' R');
        }
    }
}


async function sendStats(){
    commonFunctions.stats(trades_count_5min, orders_count_5min);
    setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
}


async function ConnectTrades(){
    // create a new websocket instance
    var ws = new WebSocket(WsUrl);
    ws.onopen = function(e) {
        // ping to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(
                { 
                    "ping":commonFunctions.getUnixTime()
                }
            ));
            console.log('Ping request sent');
            }
        }, 10000);
        // subscribe to trades and orders for all instruments
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "event": "sub",
                    "id": `trade301.${pair}`,
                    "limit": 60,
                    "params": {
                        "org": 9001,
                        "binary": false
                    },
                    "symbol": `301.${pair}`,
                    "topic": "trade"
                }
            ));
            
        });
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['topic'] === 'trade' && !dataJSON['f']) {
                getTrades(dataJSON);
            }else if(dataJSON['topic'] === 'trade' && dataJSON['f']){
                // skip trades history
            }
            else {
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
            console.log(`Connection closed with code ${event.code} and message: ${event.reason}`);
            setTimeout(async function() {
                await ConnectTrades();
                }, 500);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                await ConnectTrades();
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
    var ws = new WebSocket(WsUrl);
    ws.onopen = function(e) {
        // ping to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(
                { 
                    "ping": commonFunctions.getUnixTime()
                }
            ));
            console.log('Ping request sent');
            }
        }, 10000);
        // subscribe to trades and orders for all instruments
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "symbol": pair,
                    "topic": "diffDepth",
                    "event": "sub",
                    "params": {
                        "binary": false
                    }
                }
            ));
        });
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['topic'] === 'diffDepth') {
                getOrders(dataJSON, !dataJSON['f']); // "f": true (Whether this is the first entry)
            }
            else {
                console.log(dataJSON);
            }
        }catch(e){
            (async () => {
                await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
                console.log(event.data);
              })();
        }
    };


    // func to handle closing connection
    ws.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection closed with code ${event.code} and message: ${event.reason}`);
            setTimeout(async function() {
                await ConnectOrders();
                }, 500);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                await ConnectOrders();
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
await ConnectTrades();
if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    await ConnectOrders();
}