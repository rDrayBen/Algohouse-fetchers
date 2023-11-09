import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';

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
            let pair_data = '@MD ' + item['underlying_asset'] + '-USD' + ' spot ' + 
            item['underlying_asset'] + ' ' + 'USD' + ' ' + (item['price_step'].split('0').length - 1) +  ' 1 1 0 0';
            console.log(pair_data);
        }
    })
    console.log('@MDEND')
}


//function to get current time in unix format
function getUnixTime(){
    return Math.floor(Date.now());
}


Number.prototype.noExponents = function() {
    var data = String(this).split(/[eE]/);
    if (data.length == 1) return data[0];
  
    var z = '',
      sign = this < 0 ? '-' : '',
      str = data[0].replace('.', ''),
      mag = Number(data[1]) + 1;
  
    if (mag < 0) {
      z = sign + '0.';
      while (mag++) z += '0';
      return z + str.replace(/^\-/, '');
    }
    mag -= str.length;
    while (mag--) z += '0';
    return str + z;
}


async function getTrades(message){
    trades_count_5min[message['data']['instrument_name'].replace('PERP', 'USD')] += 1;
    var trade_output = '! ' + getUnixTime() + ' ' + message['data']['instrument_name'].replace('PERP', 'USD') + ' ' + 
        message['data']['side'][0].toUpperCase() + ' ' + parseFloat(message['data']['price']).noExponents() +
         ' ' + parseFloat(message['data']['amount']).noExponents();
    console.log(trade_output);
}


async function getOrders(message, update){
    
    var bids_order_answer = '$ ' + getUnixTime() + ' ' + message['data']['instrument_name'].replace('PERP', 'USD') + ' B '
    var bids_pq = '';
    var asks_order_answer = '$ ' + getUnixTime() + ' ' + message['data']['instrument_name'].replace('PERP', 'USD') + ' S '
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
        }
        
        
    };


    // func to handle closing connection
    ws.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                Connect(pair);
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}


Metadata();
setTimeout(stats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
var connections = [];
for(let [index, pair] of currencies.entries()){
    connections.push(Connect(pair, index));
    await new Promise((resolve) => setTimeout(resolve, 150));
}
