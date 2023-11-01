import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';

// define the websocket and REST URLs
const wsUrl = 'wss://stream.coincatch.com/spot/v1/stream?compress=false';
const restUrl = "https://api.coincatch.com/api/mix/v1/market/contracts?productType=umcbl";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson1 = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};

// extract symbols from JSON returned information
for(let i = 0; i < myJson1['data'].length; ++i){
    currencies.push(myJson1['data'][i]['symbolName']);
}

// print metadata about pairs
async function Metadata(){
    myJson1['data'].forEach((item)=>{
        trades_count_5min[item['symbolName']] = 0;
        orders_count_5min[item['symbolName']] = 0;
        let pair_data = '@MD ' + item['symbolName'] + ' spot ' + item['baseCoin'] + 
            ' ' + item['quoteCoin'] + ' ' + item['pricePlace'] +  ' 1 1 0 0';
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


async function getTrades(message){
    trades_count_5min[message['arg']['instId']] += message['data'].length;
    message['data'].forEach((trade)=>{
        var trade_output = '! ' + getUnixTime() + ' ' + message['arg']['instId'] + ' ' + 
        (trade[3] === 1 ? 'S' : 'B') + ' ' + parseFloat(trade[1]).noExponents() + ' ' + parseFloat(trade[2]).noExponents();
        console.log(trade_output);
    })
}


async function getOrders(message){
    // check if bids array is not Null
    if(message['data'][0]['bids'] && message['data'][0]['bids'].length > 0){
        orders_count_5min[message['arg']['instId']] += message['data'][0]['bids'].length;
        var order_answer = '$ ' + getUnixTime() + ' ' + message['arg']['instId'] + ' B '
        var pq = '';
        for(let i = 0; i < message['data'][0]['bids'].length; i++){
            pq += parseFloat(message['data'][0]['bids'][i][1]).noExponents() + '@' + parseFloat(message['data'][0]['bids'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
    }

    // check if asks array is not Null
    if(message['data'][0]['asks'] && message['data'][0]['asks'].length > 0){
        orders_count_5min[message['arg']['instId']] += message['data'][0]['asks'].length;
        var order_answer = '$ ' + getUnixTime() + ' ' + message['arg']['instId'] + ' S '
        var pq = '';
        for(let i = 0; i < message['data'][0]['asks'].length; i++){
            pq += parseFloat(message['data'][0]['asks'][i][1]).noExponents() + '@' + parseFloat(message['data'][0]['asks'][i][0]).noExponents() + '|';
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
}


async function Connect(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(
                    "ping"
                );
              console.log('Ping request sent');
            }
          }, 15000);
        // subscribe to trades and orders for all instruments
        ws.send(JSON.stringify(
            {
                "op": "subscribe",
                "args": [
                    {
                        "channel": "trade",
                        "instType": "sp",
                        "instId": pair
                    }
                ]
            }
        ));
        if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
            ws.send(JSON.stringify(
                {
                    "op": "subscribe",
                    "args": [
                        {
                            "channel": "books15",
                            "instType": "sp",
                            "instId": pair
                        }
                    ]
                }
            ));
        }
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);

            if(dataJSON['action'] === 'snapshot' && dataJSON['arg']['channel'] === 'trade'){
                // skip trades history
            }else if(dataJSON['action'] === 'update' && dataJSON['arg']['channel'] === 'trade'){
                getTrades(dataJSON);
            }else if(dataJSON['action'] === 'snapshot' && dataJSON['arg']['channel'] === 'books15'){
                getOrders(dataJSON);
            }
            else{
                console.log(dataJSON);
            }
        }catch(e){
            // skip confirmation messages cause they can`t be parsed into JSON format without an error
        }
        
        
    };


    // func to handle closing connection
    ws.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection closed with code ${event.code} and reason ${event.reason} for pair ${pair}`);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                Connect(pair);
                }, 1000);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}


Metadata();
stats();
setInterval(stats, 300000);

var connection = [];

for(let pair of currencies){
    connection.push(Connect(pair));
}


