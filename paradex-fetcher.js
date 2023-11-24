import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const wsUrl = 'wss://ws.api.prod.paradex.trade/v1?cancel-on-disconnect=false';
const restUrl = "https://api.prod.paradex.trade/v1/markets";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};

// extract symbols from JSON returned information
for(let i = 0; i < myJson['results'].length; ++i){
    currencies.push(myJson['results'][i]['symbol']);
}


// print metadata about pairs
async function Metadata(){
    myJson['results'].forEach((item)=>{
        trades_count_5min[item['symbol'].replace('-PERP', '')] = 0;
        orders_count_5min[item['symbol'].replace('-PERP', '')] = 0;
        let pair_data = '@MD ' + item['symbol'].replace('-PERP', '') + ' perpetual ' + 
        item['base_currency'] + ' ' + item['quote_currency'] + ' ' + (item['price_tick_size'].split('0').length - 1) +  ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


async function getTrades(message){
    trades_count_5min[message['params']['data']['market'].replace('-PERP', '')] += 1;
    var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + message['params']['data']['market'].replace('-PERP', '') + ' ' + 
        message['params']['data']['side'][0] + ' ' + parseFloat(message['params']['data']['price']).noExponents() +
         ' ' + parseFloat(message['params']['data']['size']).noExponents();
    console.log(trade_output);
    
}


async function getOrders(message, update){
    // check if bids array is not Null
    var bids_order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['params']['data']['market'].replace('-PERP', '') + ' B '
    var bids_pq = '';
    var asks_order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['params']['data']['market'].replace('-PERP', '') + ' S '
    var asks_pq = '';
    if(message['params']['data']['inserts'].length > 0){

        orders_count_5min[message['params']['data']['market'].replace('-PERP', '')] += message['params']['data']['inserts'].length;

        for(let i = 0; i < message['params']['data']['inserts'].length; i++){

            if(message['params']['data']['inserts'][i]['side'] === 'BUY'){
                bids_pq += parseFloat(message['params']['data']['inserts'][i]['size']).noExponents() +
                    '@' + parseFloat(message['params']['data']['inserts'][i]['price']).noExponents() + '|';
            }else if(message['params']['data']['inserts'][i]['side'] === 'SELL'){
                asks_pq += parseFloat(message['params']['data']['inserts'][i]['size']).noExponents() +
                    '@' + parseFloat(message['params']['data']['inserts'][i]['price']).noExponents() + '|';
            }
            
        }
        bids_pq = bids_pq.slice(0, -1);
        asks_pq = asks_pq.slice(0, -1);
    }

    if(message['params']['data']['updates'].length > 0){

        orders_count_5min[message['params']['data']['market'].replace('-PERP', '')] += message['params']['data']['updates'].length;

        for(let i = 0; i < message['params']['data']['updates'].length; i++){

            if(message['params']['data']['updates'][i]['side'] === 'BUY'){
                bids_pq += parseFloat(message['params']['data']['updates'][i]['size']).noExponents() +
                    '@' + parseFloat(message['params']['data']['updates'][i]['price']).noExponents() + '|';
            }else if(message['params']['data']['updates'][i]['side'] === 'SELL'){
                asks_pq += parseFloat(message['params']['data']['updates'][i]['size']).noExponents() +
                    '@' + parseFloat(message['params']['data']['updates'][i]['price']).noExponents() + '|';
            }
            
        }
        bids_pq = bids_pq.slice(0, -1);
        asks_pq = asks_pq.slice(0, -1);
    }


    if(message['params']['data']['deletes'].length > 0){

        orders_count_5min[message['params']['data']['market'].replace('-PERP', '')] += message['params']['data']['deletes'].length;

        for(let i = 0; i < message['params']['data']['deletes'].length; i++){

            if(message['params']['data']['deletes'][i]['side'] === 'BUY'){
                bids_pq += parseFloat(message['params']['data']['deletes'][i]['size']).noExponents() +
                    '@' + parseFloat(message['params']['data']['deletes'][i]['price']).noExponents() + '|';
            }else if(message['params']['data']['deletes'][i]['side'] === 'SELL'){
                asks_pq += parseFloat(message['params']['data']['deletes'][i]['size']).noExponents() +
                    '@' + parseFloat(message['params']['data']['deletes'][i]['price']).noExponents() + '|';
            }
            
        }
        bids_pq = bids_pq.slice(0, -1);
        asks_pq = asks_pq.slice(0, -1);
    }

    // check if the input data is full order book or just update
    if (update){
        if(bids_pq !== '') console.log(bids_order_answer + bids_pq);
        if(asks_pq !== '') console.log(asks_order_answer + asks_pq);
    }
    else{
        if(bids_pq !== '') console.log(bids_order_answer + bids_pq + ' R');
        if(asks_pq !== '') console.log(asks_order_answer + asks_pq + ' R');
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
        // subscribe to trades for all instruments
        ws.send(JSON.stringify(
            {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": {
                  "channel": "trades.ALL"
                },
                "id": 1
              }
        ));
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['params']['channel'].includes('trades')) {
                getTrades(dataJSON);
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
            console.log(`Connection closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection lost');
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


async function ConnectOrders(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // subscribe to orders for given instrument
        ws.send(JSON.stringify(
            {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": {
                  "channel": `order_book.${pair}.deltas`
                },
                "id": 1
              }
        ));
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if(dataJSON['params']['channel'].includes('order_book') && dataJSON['params']['data']['update_type'] === 's'){
                getOrders(dataJSON, false);
            }
            else if(dataJSON['params']['channel'].includes('order_book') && dataJSON['params']['data']['update_type'] === 'd'){
                getOrders(dataJSON, true);
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
            console.log(`Connection closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                ConnectOrders(pair);
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
ConnectTrades();
if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    var connections = [];
    for(let pair of currencies){
        connections.push(ConnectOrders(pair));
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
}
