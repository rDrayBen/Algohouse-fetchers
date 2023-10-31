import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';

// define the websocket and REST URLs
const wsUrl = 'wss://ws-public.blofin.com/websocket';
const restUrl = "https://api.blofin.com/uapi/v1/basic/symbols";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var precision = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000,
    1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000];


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['symbol']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item)=>{
        let prec = 11;
            for(let i = 0; i < precision.length; ++i){
                if(item['price_precision'] * precision[i] >= 1){
                    prec = i
                    break;
                }
            }
        let pair_data = '@MD ' + item['symbol'] + ' spot ' + 
            item['base_currency'] + ' ' + item['quote_currency'] + ' ' + prec +  ' 1 1 0 0';
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
    var trade_output = '! ' + getUnixTime() + ' ' + message['symbol'] + ' ' + 
        message['side'][0].toUpperCase() + ' ' + parseFloat(message['price']).noExponents() + ' ' + parseFloat(message['quantity']).noExponents();
    console.log(trade_output);
}


async function getOrders(message, update){
    // check if bids array is not Null
    if(message['bids'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['symbol'] + ' B '
        var pq = '';
        for(let i = 0; i < message['bids'].length; i++){
            pq += parseFloat(message['bids'][i][1]).noExponents() + '@' + parseFloat(message['bids'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        if (update){
            console.log(order_answer + pq);
        }
        else{
            console.log(order_answer + pq + ' R');
        }  
    }

    // check if asks array is not Null
    if(message['asks'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['symbol'] + ' S '
        var pq = '';
        for(let i = 0; i < message['asks'].length; i++){
            pq += parseFloat(message['asks'][i][1]).noExponents() + '@' + parseFloat(message['asks'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        if (update){
            console.log(order_answer + pq);
        }
        else{
            console.log(order_answer + pq + ' R');
        }  
    }
}


async function Connect(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // subscribe to trades and orders for all instruments
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "op": "SUBSCRIBE",
                    "args": [
                        {
                            "channel": "TRADE",
                            "symbol": pair
                        }
                    ],
                    "id": 5
                }
            ));
            if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                ws.send(JSON.stringify(
                    {
                        "op": "SUBSCRIBE",
                        "args": [
                            {
                                "channel": "DEPTH",
                                "symbol": pair,
                                "update_speed": "100ms"
                            }
                        ],
                        "id": 1
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
            if (dataJSON['channel'] === 'TRADE') {
                getTrades(dataJSON);
            }
            else if (dataJSON['channel'] === 'DEPTH' && dataJSON['data_type'] === 'SNAPSHOT') {
                getOrders(dataJSON, false);
            }
            else if (dataJSON['channel'] === 'DEPTH' && dataJSON['data_type'] === 'UPDATE') {
                getOrders(dataJSON, true);
            }
            else if('op' in dataJSON){
                ws.send(JSON.stringify(
                    {
                        "op":"PONG"
                    }
                ));
                console.log('Pong sent');
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
                await Connect();
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}

Metadata();
await Connect();
