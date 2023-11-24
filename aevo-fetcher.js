import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const wsUrl = 'wss://ws.aevo.xyz/';
const restUrl = "https://api.aevo.xyz/markets";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};

// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    if(myJson[i]['is_active'] && myJson[i]['instrument_type'] === 'PERPETUAL'){
        currencies.push(myJson[i]['instrument_name']);
    }
}


// print metadata about pairs
async function Metadata(){
    myJson.forEach((item)=>{
        if(item['is_active'] && item['instrument_type'] === 'PERPETUAL'){
            trades_count_5min[item['underlying_asset'] + '-USD'] = 0;
            orders_count_5min[item['underlying_asset'] + '-USD'] = 0;
            let pair_data = '@MD ' + item['underlying_asset'] + '-USD' + ' perpetual ' + 
            item['underlying_asset'] + ' ' + 'USD' + ' ' + (item['price_step'].split('0').length - 1) +  ' 1 1 0 0';
            console.log(pair_data);
        }
    })
    console.log('@MDEND')
}



async function getTrades(message){
    trades_count_5min[message['data']['instrument_name'].replace('PERP', 'USD')] += 1;
    var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + message['data']['instrument_name'].replace('PERP', 'USD') + ' ' + 
        message['data']['side'][0].toUpperCase() + ' ' + parseFloat(message['data']['price']).noExponents() +
         ' ' + parseFloat(message['data']['amount']).noExponents();
    console.log(trade_output);
}


async function getOrders(message, update){
    
    var bids_order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['data']['instrument_name'].replace('PERP', 'USD') + ' B '
    var bids_pq = '';
    var asks_order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['data']['instrument_name'].replace('PERP', 'USD') + ' S '
    var asks_pq = '';
    // check if bids array is not Null
    if(message['data']['bids'].length > 0){

        orders_count_5min[message['data']['instrument_name'].replace('PERP', 'USD')] += message['data']['bids'].length;

        for(let i = 0; i < message['data']['bids'].length; i++){
            bids_pq += parseFloat(message['data']['bids'][i][1]).noExponents() + 
                '@' + parseFloat(message['data']['bids'][i][0]).noExponents() + '|';
        }
        bids_pq = bids_pq.slice(0, -1);
    }

    if(message['data']['asks'].length > 0){

        orders_count_5min[message['data']['instrument_name'].replace('PERP', 'USD')] += message['data']['asks'].length;

        for(let i = 0; i < message['data']['asks'].length; i++){
            asks_pq += parseFloat(message['data']['asks'][i][1]).noExponents() +
                '@' + parseFloat(message['data']['asks'][i][0]).noExponents() + '|';
        }
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


async function Connect(pair, index){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "op": "ping"
                }
              ));
              console.log('Ping request sent');
            }
          }, 15000);
        ws.send(JSON.stringify(
            {
                "op": "subscribe",
                "data": [
                    `trades:${pair}`
                ],
                "id": index
            }
        ));
        if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
            ws.send(JSON.stringify(
                {
                    "op": "subscribe",
                    "data": [
                        `orderbook:${pair}`
                    ],
                    "id": index
                }
            ));
        }
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['channel'].includes('trades')) {
                getTrades(dataJSON);
            }
            else if(dataJSON['channel'].includes('orderbook') && dataJSON['data']['type'] === 'snapshot'){
                getOrders(dataJSON, false);
            }
            else if(dataJSON['channel'].includes('orderbook') && dataJSON['data']['type'] === 'update'){
                getOrders(dataJSON, true);
            }
            else {
                console.log(dataJSON);
            }
        }catch(e){
            // skip confirmation messages cause they can`t be parsed into JSON format without an error
            (async () => {
                await commonFunctions.sleep(1000); // Sleep for 1000 milliseconds (1 second) 
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
                Connect(pair, index);
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
        (async () => {
            await commonFunctions.sleep(1000); // Sleep for 1000 milliseconds (1 second) 
          })();
    };
}


Metadata();
setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
var connections = [];
for(let [index, pair] of currencies.entries()){
    connections.push(Connect(pair, index));
    await commonFunctions.sleep(150);
}
