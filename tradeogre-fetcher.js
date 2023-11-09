import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';

// define the websocket and REST URLs
const wsUrl = 'wss://tradeogre.com:8443/';
const restUrl = "https://tradeogre.com/api/v1/markets";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    currencies.push(Object.keys(myJson[i])[0]);
}

// print metadata about pairs
async function Metadata(){
    currencies.forEach((item)=>{
        trades_count_5min[item] = 0;
        orders_count_5min[item] = 0;
        let pair_data = '@MD ' + item + ' spot ' + 
            item.split('-')[0] + ' ' + item.split('-')[1] + ' ' + '-1' +  ' 1 1 0 0';
        console.log(pair_data);
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


async function getTrades(message, pair_name){
    trades_count_5min[pair_name] += 1;
    var trade_output = '! ' + getUnixTime() + ' ' + pair_name + ' ' + 
        (message['d'][1] === 0 ? 'B' : 'S') + ' ' + parseFloat(message['d'][2]).noExponents() + ' ' + parseFloat(message['d'][3]).noExponents();
    console.log(trade_output);
}


async function getOrders(message, update, pair_name){
    if(!update || message['a'] === 'add'){
        orders_count_5min[pair_name] += Object.keys(message['d']).length; 
        var order_answer = '$ ' + getUnixTime() + ' ' + pair_name + ' ' + message['t'][0].toUpperCase() + ' ';
        var pq = '';
        for(var key in message['d']){
            pq += parseFloat(message['d'][key]).noExponents() + '@' + parseFloat(key).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        if(pq != ''){
            // check if the input data is full order book or just update
            if (update){
                console.log(order_answer + pq);
            }
            else{
                console.log(order_answer + pq + ' R');
            }  
        }
        
    }else{
        orders_count_5min[pair_name] += Object.keys(message['d']).length; 
        var order_answer = '$ ' + getUnixTime() + ' ' + pair_name + ' ' + message['t'][0].toUpperCase() + ' ';
        var pq = '';
        for(var key in message['d']){
            pq += '0' + '@' + parseFloat(key).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
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


async function Connect(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        ws.ping();
        // subscribe to trades and orders for all instruments
        ws.send(JSON.stringify(
            {
                "a":"submarket",
                "name":pair
            }
        ));
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON.a === 'newhistory') {
                getTrades(dataJSON, pair);
            }
            else if(dataJSON.a === 'orders'){
                if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                    getOrders(dataJSON, false, pair);
                }
                
            }
            else if(dataJSON.a === 'add' || dataJSON.a === 'sub'){
                if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                    getOrders(dataJSON, true, pair);
                }
                
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
            console.log('Connection lost with pair', pair);
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

for(let pair of currencies){
    connections.push(await Connect(pair));
    await new Promise((resolve) => setTimeout(resolve, 100));
}

