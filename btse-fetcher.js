import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';

// define the websocket and REST URLs
const tradeWsUrl = 'wss://ws.btse.com/ws/spot';
const orderWsUrl = 'wss://ws.btse.com/ws/oss/spot';
const restUrl = "https://api.btse.com/spot/api/v3.2/market_summary";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var precision = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000,
    1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000];
var trades_count_5min = {};
var orders_count_5min = {};

// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    if(myJson[i]['active']){
        currencies.push(myJson[i]['symbol']);
    }
}


// print metadata about pairs
async function Metadata(){
    myJson.forEach((item, index)=>{
        if(item['active']){
            let prec = 11;
            for(let i = 0; i < precision.length; ++i){
                if(item['minPriceIncrement'] * precision[i] >= 1){
                    prec = i
                    break;
                }
            }
            trades_count_5min[item['symbol']] = 0;
            orders_count_5min[item['symbol']] = 0;
            let pair_data = '@MD ' + item['symbol'] + ' spot ' + item['base'] + ' ' + item['quote'] + ' ' 
            + prec + ' 1 1 0 0';
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
    message['data'].forEach((item)=>{
        trades_count_5min[item['symbol']] += message['data'].length;
        var trade_output = '! ' + getUnixTime() + ' ' + 
        item['symbol'] + ' ' + 
        item['side'][0] + ' ' + item['price'] + ' ' + item['size'];
        console.log(trade_output);
    });
}


// func to print orderbooks and deltas
async function getOrders(message, update){
    orders_count_5min[message['data']['symbol']] += message['data']['bids'].length + message['data']['asks'].length;
    // check if bids array is not Null
    if(message['data']['bids'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['data']['symbol'] + ' B '
        var pq = '';
        for(let i = 0; i < message['data']['bids'].length; i++){
            pq += message['data']['bids'][i][1] + '@' + message['data']['bids'][i][0] + '|';
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
    if(message['data']['asks'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['data']['symbol'] + ' S '
        var pq = '';
        for(let i = 0; i < message['data']['asks'].length; i++){
            pq += message['data']['asks'][i][1] + '@' + message['data']['asks'][i][0] + '|';
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
}


function Connect1(){
    var ws1 = new WebSocket(tradeWsUrl);
    // call this func when first opening connection
    ws1.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws1.readyState === WebSocket.OPEN) {
              ws1.send(JSON.stringify(
                "ping"
              ));
              console.log('Ping request sent');
            }
          }, 20000);
        currencies.forEach((item)=>{
            // sub for trades
            ws1.send(JSON.stringify(
                {
                    "op": "subscribe",
                    "args": [
                    `tradeHistoryApi:${item}`
                    ]
                }
            ))
        })
    };


    // func to handle input messages
    ws1.onmessage = function(event) {
        var dataJSON;
        try{
            dataJSON = JSON.parse(event.data);
            if (dataJSON['topic'].split(':')[0] === 'tradeHistoryApi' && dataJSON['data'].length < 5){
                getTrades(dataJSON);
            }else if (dataJSON['topic'].split(':')[0] === 'tradeHistoryApi' && dataJSON['data'].length > 5){
                // skip trades history
            }else{
                console.log(dataJSON);
            }        
        }catch(e) {
            // console.log(e);
            // error may occurr cause some part of incoming data can`t be properly parsed in json format due to inapropriate symbols
            // error only occurrs in messages that confirming subs
            // error caused here is exchanges fault
        }
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
    var ws2 = new WebSocket(orderWsUrl);

    // call this func when first opening connection
    ws2.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws2.readyState === WebSocket.OPEN) {
              ws2.send(JSON.stringify(
                "ping"
              ));
              console.log('Ping request sent');
            }
          }, 20000);
        currencies.forEach((item)=>{
            // sub for orders
            ws2.send(JSON.stringify(
                {
                    "op": "subscribe",
                    "args": [
                      `update:${item}_0`
                    ]
                }
            ))
        })
            
    };


    // func to handle input messages
    ws2.onmessage = function(event) {
        var dataJSON;
        try{
            dataJSON = JSON.parse(event.data);
            if (dataJSON['topic'].split(':')[0] === 'update' && dataJSON['data']['type'] === 'snapshot'){
                getOrders(dataJSON, false);
            }else if (dataJSON['topic'].split(':')[0] === 'update' && dataJSON['data']['type'] === 'delta'){
                getOrders(dataJSON, true);
            }else{
                console.log(dataJSON);
            }        
        }catch(e) {
            // console.log(e);
            // error may occurr cause some part of incoming data can`t be properly parsed in json format due to inapropriate symbols
            // error only occurrs in messages that confirming subs
            // error caused here is exchanges fault
        }
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

stats();
setInterval(stats, 300000);

Connect1();
if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    Connect2();
}

