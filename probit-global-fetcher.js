import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';

// define the websocket and REST URLs
const wsUrl = 'wss://www.probit.com/api/exchange/v1/ws';
const restUrl = "https://api.probit.com/api/exchange/v1/market";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['id']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item)=>{
        trades_count_5min[item['id']] = 0;
        orders_count_5min[item['id']] = 0;
        let pair_data = '@MD ' + item['id'] + ' spot ' + item['base_currency_id'] + ' ' + item['quote_currency_id'] + ' ' 
            + item['cost_precision'] + ' 1 1 0 0';
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
    trades_count_5min[message['market_id']] += message['recent_trades'].length;
    message['recent_trades'].forEach((item)=>{
        var trade_output = '! ' + getUnixTime() + ' ' + message['market_id'] + ' ' + 
        item['side'][0].toUpperCase() + ' ' + parseFloat(item['price']).noExponents() + ' ' + parseFloat(item['quantity']).noExponents();
        console.log(trade_output);
    });
}


async function getOrders(message, update){
    // check if bids array is not Null
    if(message['order_books_l0']){
        orders_count_5min[message['market_id']] += message['order_books_l0'].length;
        var order_answer = '$ ' + getUnixTime() + ' ' + message['market_id'] + ' B '
        var pq = '';
        for(let i = 0; i < message['order_books_l0'].length; i++){
            if(message['order_books_l0'][i]['side'] === 'buy'){
                pq += parseFloat(message['order_books_l0'][i]['quantity']).noExponents() + '@' + parseFloat(message['order_books_l0'][i]['price']).noExponents() + '|';
            }
        }
        pq = pq.slice(0, -1);
        // check if the input data is full order book or just update
        if(pq !== ''){
            if (update){
                console.log(order_answer + pq)
            }
            else{
                console.log(order_answer + pq + ' R')
            }
        }

        var order_answer = '$ ' + getUnixTime() + ' ' + message['market_id'] + ' S '
        var pq = '';
        for(let i = 0; i < message['order_books_l0'].length; i++){
            if(message['order_books_l0'][i]['side'] === 'sell'){
                pq += parseFloat(message['order_books_l0'][i]['quantity']).noExponents() + '@' + parseFloat(message['order_books_l0'][i]['price']).noExponents() + '|';
            }
        }
        pq = pq.slice(0, -1);
        // check if the input data is full order book or just update
        if(pq !== ''){
            if (update){
                console.log(order_answer + pq)
            }
            else{
                console.log(order_answer + pq + ' R')
            }
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


async function Connect(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
            ws.send(JSON.stringify(
                {
                    "type":"subscribe",
                    "channel":"marketdata",
                    "market_id":pair,
                    "interval":100,
                    "filter":["recent_trades", "order_books_l0"]
                }
            ));
        }else{
            ws.send(JSON.stringify(
                {
                    "type":"subscribe",
                    "channel":"marketdata",
                    "market_id":pair,
                    "interval":100,
                    "filter":["recent_trades"]
                }
            ));
        }
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['channel'] === 'marketdata' && dataJSON['recent_trades'].length <= 5){
                getTrades(dataJSON);
            }else if(dataJSON['channel'] === 'marketdata' && dataJSON['recent_trades'].length > 5){
                // skip trades history
            }
            if (dataJSON['channel'] === 'marketdata' && dataJSON['order_books_l0'].length >= 5){
                getOrders(dataJSON, false);
            }else if (dataJSON['channel'] === 'marketdata' && dataJSON['order_books_l0'].length < 5){
                getOrders(dataJSON, true);
            }else{
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
            console.log(`Connection closed with code ${event.code} and reason ${event}`);
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
        (async () => {
            await sleep(1000); // Sleep for 1000 milliseconds (1 second) 
          })();
    };
}

Metadata();
setTimeout(stats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
var connections = [];

for(let pair of currencies){
    connections.push(Connect(pair));
    await new Promise((resolve) => setTimeout(resolve, 400));
}



