import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const wsUrl = 'wss://ws-api.coincheck.com/';
const restUrl = "https://coincheck.com/api/rate/all";
const orderbookUrlBase = "https://coincheck.com/api/order_books?pair=";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};
var temp = [];
temp = Object.keys(myJson['jpy']);


// extract symbols from JSON returned information
for(let i = 0; i < temp.length; ++i){
    currencies.push(temp[i] + '_jpy');
}

temp = Object.keys(myJson['btc']);

// extract symbols from JSON returned information
for(let i = 0; i < temp.length; ++i){
    currencies.push(temp[i] + '_btc');
}


// print metadata about pairs
async function Metadata(){
    currencies.forEach((item)=>{
        trades_count_5min[item.toUpperCase()] = 0;
        orders_count_5min[item.toUpperCase()] = 0;
        let pair_data = '@MD ' + item.toUpperCase() + ' spot ' + item.split('_')[0].toUpperCase() + ' ' + item.split('_')[1].toUpperCase()
         + ' -1 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}



async function getTrades(message){
    message.forEach((trade)=>{
        trades_count_5min[trade[2].toUpperCase()] += 1;
        var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + trade[2].toUpperCase() + ' ' +
            trade[5][0].toUpperCase() + ' ' + parseFloat(trade[3]).noExponents() + ' ' + parseFloat(trade[4]).noExponents();
        console.log(trade_output);
    });
}
// trade ex. 
/*[
    "1690463569",
    "248164679",
    "btc_jpy",
    "4154377.0",
    "0.0264",
    "sell",
    "5706675440",
    "5706675437"
]*/


async function getOrders(message){
    // check if asks array is not Null
    if(message[1]['asks'].length > 0){
        orders_count_5min[message[0].toUpperCase()] += message[1]['asks'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message[0].toUpperCase() + ' S '
        var pq = '';
        for(let i = 0; i < message[1]['asks'].length; i++){
            pq += parseFloat(message[1]['asks'][i][1]).noExponents() + '@' + parseFloat(message[1]['asks'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
    }
    // check if bids array is not Null
    if(message[1]['bids'].length > 0){
        orders_count_5min[message[0].toUpperCase()] += message[1]['bids'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message[0].toUpperCase() + ' B '
        var pq = '';
        for(let i = 0; i < message[1]['bids'].length; i++){
            pq += parseFloat(message[1]['bids'][i][1]).noExponents() + '@' + parseFloat(message[1]['bids'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
    }
    
}

async function getOrderbook(pair){
    const response = await fetch(orderbookUrlBase + pair);
    //extract JSON from the http response
    const responseJSON = await response.json(); 
    try{
        if(responseJSON['asks'].length > 0){
            orders_count_5min[pair.toUpperCase()] += responseJSON['asks'].length;
            var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + pair.toUpperCase() + ' S '
            var pq = '';
            for(let i = 0; i < responseJSON['asks'].length; i++){
                pq += parseFloat(responseJSON['asks'][i][1]).noExponents() + '@' + parseFloat(responseJSON['asks'][i][0]).noExponents() + '|';
            }
            pq = pq.slice(0, -1);
            console.log(order_answer + pq + ' R');
        }

        if(responseJSON['bids'].length > 0){
            orders_count_5min[pair.toUpperCase()] += responseJSON['bids'].length;
            var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + pair.toUpperCase() + ' B '
            var pq = '';
            for(let i = 0; i < responseJSON['bids'].length; i++){
                pq += parseFloat(responseJSON['bids'][i][1]).noExponents() + '@' + parseFloat(responseJSON['bids'][i][0]).noExponents() + '|';
            }
            pq = pq.slice(0, -1);
            console.log(order_answer + pq + ' R');
        }
    }catch(e){

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
        
        // create ping function to keep connection alive
        ws.ping();
        // subscribe to trades and orders for given instrument
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "type": "subscribe",
                    "channel": `${pair}-trades`
                }
            ));
        }); 
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON.length > 0) {
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

async function ConnectDeltas(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        
        // create ping function to keep connection alive
        ws.ping();
        // subscribe to trades and orders for given instrument
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "type": "subscribe",
                    "channel": `${pair}-orderbook`
                }
            ));
        }); 
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON[1]['bids'] && dataJSON[1]['asks']) {
                getOrders(dataJSON);
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
                ConnectDeltas();
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
    for(let pair of currencies){
        getOrderbook(pair);
    }
    ConnectDeltas();
}

 


