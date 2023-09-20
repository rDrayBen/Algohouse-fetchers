import WebSocket from 'ws';
import fetch from 'node-fetch';


// define the websocket and REST URLs
const wsUrl = 'wss://www.probit.com/api/exchange/v1/ws';
const restUrl = "https://api.probit.com/api/exchange/v1/market";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['id']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item)=>{
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
    message['recent_trades'].forEach((item)=>{
        var trade_output = '! ' + getUnixTime() + ' ' + message['market_id'] + ' ' + 
        item['side'][0].toUpperCase() + ' ' + parseFloat(item['price']).noExponents() + ' ' + parseFloat(item['quantity']).noExponents();
        console.log(trade_output);
    });
}


async function getOrders(message, update){
    // check if bids array is not Null
    if(message['order_books_l0']){
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
    }

    // check if asks array is not Null
    if(message['order_books_l0']){
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


async function Connect(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        ws.send(JSON.stringify(
            {
                "type":"subscribe",
                "channel":"marketdata",
                "market_id":pair,
                "interval":100,
                "filter":["recent_trades", "order_books_l0"]
            }
        ));
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
    };
}

Metadata();
var connections = [];

for(let pair of currencies){
    connections.push(Connect(pair));
    await new Promise((resolve) => setTimeout(resolve, 400));
}



