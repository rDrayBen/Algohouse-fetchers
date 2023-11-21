import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import fs from "fs";

// define the websocket and REST URLs
const wsUrl = 'wss://api.hyperliquid.xyz/ws';
const restUrl = "https://api.hyperliquid.xyz/info";

const response = await fetch(restUrl, {
    method: "POST",
    headers: {
        "Content-Type": "application/json"
    },
    body: JSON.stringify({
        "type": "meta"
    })
});

//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson['universe'].length; ++i){
    currencies.push(myJson['universe'][i]['name']);
}


// print metadata about pairs
async function Metadata(){
    myJson['universe'].forEach((item)=>{
        trades_count_5min[item['name'] + '-USD'] = 0;
        orders_count_5min[item['name'] + '-USD'] = 0;
        let pair_data = '@MD ' + item['name'] + '-USD' + ' perpetual ' + 
        item['name'] + ' ' + 'USD' + ' ' + '-1' +  ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


//function to get current time in unix format
function getUnixTime(){
    return Math.floor(Date.now());
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
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
    message['data'].forEach((trade)=>{
        trades_count_5min[trade['coin'] + '-USD'] += 1;
        var trade_output = '! ' + getUnixTime() + ' ' + trade['coin'] + '-USD' + ' ' + 
            (trade['side'] === 'B' ? 'B' : 'S') + ' ' + parseFloat(trade['px']).noExponents() +
            ' ' + parseFloat(trade['sz']).noExponents();
        console.log(trade_output);
    });
    
}


async function getOrders(message){
    var bids_order_answer = '$ ' + getUnixTime() + ' ' + message['data']['coin'] + '-USD' + ' B '
    var bids_pq = '';
    var asks_order_answer = '$ ' + getUnixTime() + ' ' + message['data']['coin'] + '-USD' + ' S '
    var asks_pq = '';
    // check if bids array is not Null
    if(message['data']['levels'][0].length > 0){
        for(let i = 0; i < message['data']['levels'][0].length; i++){

            orders_count_5min[message['data']['coin'] + '-USD'] += message['data']['levels'][0][i]['n'];

            for(let j = 0; j < message['data']['levels'][0][i]['n']; j++){ // count of orders with the same price&quantity
                bids_pq += parseFloat(message['data']['levels'][0][i]['sz']).noExponents() + 
                '@' + parseFloat(message['data']['levels'][0][i]['px']).noExponents() + '|';
            }
            
        }
        bids_pq = bids_pq.slice(0, -1);
    }

    if(message['data']['levels'][1].length > 0){
        for(let i = 0; i < message['data']['levels'][1].length; i++){

            orders_count_5min[message['data']['coin'] + '-USD'] += message['data']['levels'][1][i]['n'];

            for(let j = 0; j < message['data']['levels'][1][i]['n']; j++){ // count of orders with the same price&quantity
                asks_pq += parseFloat(message['data']['levels'][1][i]['sz']).noExponents() +
                    '@' + parseFloat(message['data']['levels'][1][i]['px']).noExponents() + '|';
            }
        }
        asks_pq = asks_pq.slice(0, -1);
    }

    if(bids_pq !== '') console.log(bids_order_answer + bids_pq + ' R');
    if(asks_pq !== '') console.log(asks_order_answer + asks_pq + ' R');
    
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


async function Connect(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                { 
                    "method": "ping" 
                }
              ));
              console.log('Ping request sent');
            }
          }, 50000);
        currencies.forEach((coin)=>{
            ws.send(JSON.stringify(
                {
                    "method": "subscribe",
                    "subscription": {
                        "type": "trades",
                        "coin": coin
                    }
                }
            ));
            if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                ws.send(JSON.stringify(
                    {
                        "method": "subscribe",
                        "subscription": {
                            "type": "l2Book",
                            "coin": coin
                        }
                    }
                ));
            }
        });
        
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['channel'] === 'trades' && dataJSON['data'].length < 5) {
                getTrades(dataJSON); // recent trades
            }
            else if(dataJSON['channel'] === 'trades' && dataJSON['data'].length >= 5){
                // skip trades snapshot
            }
            else if(dataJSON['channel'] === 'l2Book'){
                getOrders(dataJSON); // exchange only provide shapshots
            }
            else {
                console.log(dataJSON);
            }
        }catch(e){
            // skip confirmation messages cause they can`t be parsed into JSON format without an error
            (async () => {
                await sleep(1000); // Sleep for 1000 milliseconds (1 second) 
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
                Connect();
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
        (async () => {
            await sleep(1000); // Sleep for 1000 milliseconds (1 second) 
          })();
    };
}


Metadata();
setTimeout(stats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
Connect();
