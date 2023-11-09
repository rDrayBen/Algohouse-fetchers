import WebSocket from 'ws';
import fetch from 'node-fetch';
import zlib from 'zlib';
import getenv from 'getenv';

// define the websocket and REST URLs
const wsUrl = 'wss://openapi.digifinex.com/ws/v1/';
const restUrl = "https://openapi.digifinex.com/v3/margin/symbols";


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};



// extract symbols from JSON returned information
for(let i = 0; i < myJson['symbol_list'].length; ++i){
    if(myJson['symbol_list'][i]['status'] === 'TRADING'){
        currencies.push(myJson['symbol_list'][i]['symbol']);
    }
}


// print metadata about pairs
async function Metadata(){
    myJson['symbol_list'].forEach((item, index)=>{
        if(item['status'] === 'TRADING'){
            trades_count_5min[item['symbol']] = 0;
            orders_count_5min[item['symbol']] = 0;
            let pair_data = '@MD ' + item['symbol'] + ' spot ' + item['base_asset'] + ' ' + item['quote_asset'] + ' ' 
            + item['price_precision'] + ' 1 1 0 0';
            console.log(pair_data);
        }
        
    })
    console.log('@MDEND')
}


//function to get current time in unix format
function getUnixTime(){
    return Math.floor(Date.now());
}


// func to print trades
async function getTrades(message){
    trades_count_5min[message['params'][2]] += message['params'][1].length;
    message['params'][1].forEach((item)=>{
        var trade_output = '! ' + getUnixTime() + ' ' + 
        message['params'][2] + ' ' + 
        item['type'][0].toUpperCase() + ' ' + item['price'] + ' ' + item['amount'];
        console.log(trade_output);
    });
}


// func to print orderbooks and deltas
async function getOrders(message, update){
    // check if bids array is not Null
    if(message['params'][1]['bids'].length > 0){
        orders_count_5min[message['params'][2]] += message['params'][1]['bids'].length;
        var order_answer = '$ ' + getUnixTime() + ' ' + message['params'][2] + ' B '
        var pq = '';
        for(let i = 0; i < message['params'][1]['bids'].length; i++){
            pq += message['params'][1]['bids'][i][1] + '@' + message['params'][1]['bids'][i][0] + '|';
        }
        pq = pq.slice(0, -1);
        // check if the input data is full order book or just update
        if (update){
            console.log(order_answer + pq)
        }
        else{
            console.log(order_answer + pq + ' R')
        }
    }

    // check if asks array is not Null
    if(message['params'][1]['asks'].length > 0){
        orders_count_5min[message['params'][2]] += message['params'][1]['asks'].length;
        var order_answer = '$ ' + getUnixTime() + ' ' + message['params'][2] + ' S '
        var pq = '';
        for(let i = 0; i < message['params'][1]['asks'].length; i++){
            pq += message['params'][1]['asks'][i][1] + '@' + message['params'][1]['asks'][i][0] + '|';
        }
        pq = pq.slice(0, -1);
        // check if the input data is full order book or just update
        if (update){
            console.log(order_answer + pq)
        }
        else{
            console.log(order_answer + pq + ' R')
        }
    }
}


async function stats(){
    var stat_line = '# LOG:CAT=trades_stats:MSG= ';

    for(var key in trades_count_5min){
        if(trades_count_5min[key] !== 0){
            stat_line += `${key}:${trades_count_5min[key]} `;
        }
        trades_count_5min[key] = 0;
    }
    if (stat_line !== '# LOG:CAT=trades_stats:MSG= '){
        console.log(stat_line);
    }

    stat_line = '# LOG:CAT=orderbook_stats:MSG= ';

    for(var key in orders_count_5min){
        if(orders_count_5min[key] !== 0){
            stat_line += `${key}:${orders_count_5min[key]} `;
        }
        orders_count_5min[key] = 0;
    }
    if (stat_line !== '# LOG:CAT=orderbook_stats:MSG= '){
        console.log(stat_line);
    }
    setTimeout(stats, 300000);
}


function Connect1(){
    var ws1 = new WebSocket(wsUrl);
    // call this func when first opening connection
    ws1.onopen = function(e) {
        // create ping function to keep connection alive
        ws1.ping();
        
        // sub for trades
        ws1.send(JSON.stringify(
            {
                "method":"trades.subscribe",
                "id":1,
                "params":currencies
            }
        ))
            
    };


    // func to handle input messages
    ws1.onmessage = function(event) {
        const compressedData = Buffer.from(event.data, 'base64');
        zlib.inflate(compressedData, (err, uncompressedData) => {
            var dataJSON = JSON.parse(uncompressedData);
            
                if (dataJSON['method'] === 'trades.update' && dataJSON['params'][0] === false){
                    getTrades(dataJSON);
                }else if (dataJSON['method'] === 'trades.update' && dataJSON['params'][0] === true){
                    // to skip trades history
                }else if(dataJSON['method'] === 'depth.update' && dataJSON['params'][0] === true){
                    getOrders(dataJSON, false); 
                }else if(dataJSON['method'] === 'depth.update' && dataJSON['params'][0] === false){
                    getOrders(dataJSON, true); 
                }else{
                    console.log(dataJSON);
                }
        });
    };


    // func to handle closing connection
    ws1.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 1 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 1 lost');
            setTimeout(function() {
                Connect1();
                }, 500);
        }
    };

    // func to handle errors
    ws1.onerror = function(error) {
        console.log(`Error ${error} occurred in ws1`);
    };
}

function Connect2(){
    // create a new websocket instance
    var ws2 = new WebSocket(wsUrl);

    // call this func when first opening connection
    ws2.onopen = function(e) {
        // create ping function to keep connection alive
        ws2.ping();
        
        // sub for orders
        ws2.send(JSON.stringify(
            {
                "method":"depth.subscribe",
                "id":1,
                "params":currencies
            }
        ))
            
    };


    // func to handle input messages
    ws2.onmessage = function(event) {
        const compressedData = Buffer.from(event.data, 'base64');
        zlib.inflate(compressedData, (err, uncompressedData) => {
            var dataJSON = JSON.parse(uncompressedData);
            
                if (dataJSON['method'] === 'trades.update' && dataJSON['params'][0] === false){
                    getTrades(dataJSON);
                }else if (dataJSON['method'] === 'trades.update' && dataJSON['params'][0] === true){
                    // to skip trades history
                }else if(dataJSON['method'] === 'depth.update' && dataJSON['params'][0] === true){
                    getOrders(dataJSON, false); 
                }else if(dataJSON['method'] === 'depth.update' && dataJSON['params'][0] === false){
                    getOrders(dataJSON, true); 
                }else{
                    console.log(dataJSON);
                }
        });
    };


    // func to handle closing connection
    ws2.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 2 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 2 lost');
            setTimeout(function() {
                Connect2();
                }, 500);
        }
    };

    // func to handle errors
    ws2.onerror = function(error) {
        console.log(`Error ${error} occurred in ws2`);
    };
}

// call metadata to execute
Metadata();
setTimeout(stats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
Connect1();
if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    Connect2();
}


