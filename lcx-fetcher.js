import WebSocket from 'ws';
import fetch from 'node-fetch';


// define the websocket and REST URLs
const wsUrl = 'wss://exchange-api.lcx.com/ws';
const restUrl = "https://exchange-api.lcx.com/market/pairs";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    if(myJson['data'][i]['status'] === true){
        currencies.push(myJson['data'][i]['symbol']);
    }
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item, index)=>{
        if(item['status'] === true){
            let pair_data = '@MD ' + item['symbol'] + ' spot ' + item['base'] + ' ' + item['quote'] + ' ' 
            + item['pricePrecision'] + ' 1 1 0 0';
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

// func to print trades
async function getTrades(message){
    var trade_output = '! ' + getUnixTime() + ' ' + message['pair'] + ' ' + 
    message['data'][2][0] + ' ' + message['data'][0] + ' ' + message['data'][1];
    console.log(trade_output);
}


// func to print orderbooks and deltas
async function getSnapshot(message){
    // check if bids array is not Null
    if(message['data']['buy']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['pair'] + ' B ';
        var pq = '';
        for(let i = 0; i < message['data']['buy'].length; i++){
            pq += (message['data']['buy'][i][1]).noExponents() + '@' + (message['data']['buy'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }

    // check if asks array is not Null
    if(message['data']['sell']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['pair'] + ' S '
        var pq = '';
        for(let i = 0; i < message['data']['sell'].length; i++){
            pq += (message['data']['sell'][i][1]).noExponents() + '@' + (message['data']['sell'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }
}


async function getDelta(message){
    var order_answer = '$ ' + getUnixTime() + ' ' + message['pair'] + ' ' + message['data'][2][0] + ' ';
    var pq = (message['data'][1]).noExponents() + '@' + (message['data'][0]).noExponents();
    console.log(order_answer + pq);
}


async function Connect(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // console.log('opened');
        // create ping function to keep connection alive
        ws.ping();
        // subscribe to trades and orders for all instruments
        currencies.forEach((item) =>{
            // sub for trades
            ws.send(JSON.stringify(
                {
                    "Topic": "subscribe", 
                    "Type": "trade", 
                    "Pair": item
                }
            ));
            // sub for orders/deltas
            ws.send(JSON.stringify(
                {
                    "Topic": "subscribe", 
                    "Type": "orderbook", 
                    "Pair": item
                }
            ));
        })
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            // console.log(dataJSON);
            if (dataJSON['type'] === 'trade' && dataJSON['topic'] === 'update'){
                getTrades(dataJSON);
            }else if(dataJSON['type'] === 'trade' && dataJSON['topic'] === 'snapshot'){
                // skip trades history
            }else if(dataJSON['type'] === 'orderbook' && dataJSON['topic'] === 'snapshot'){
                getSnapshot(dataJSON);
            }else if(dataJSON['type'] === 'orderbook' && dataJSON['topic'] === 'update'){
                getDelta(dataJSON);
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
    };
}

Metadata();
Connect();


