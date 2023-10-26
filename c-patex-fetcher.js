import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';

// define the websocket and REST URLs
const wsUrl = 'wss://ws.c-patex.com/';
const restUrl = "https://back.c-patex.com/v2/market-list";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];


// extract symbols from JSON returned information
for(let i = 0; i < myJson['response']['marketList']['result'].length; ++i){
    currencies.push(myJson['response']['marketList']['result'][i]['name']);
}


// print metadata about pairs
async function Metadata(){
    myJson['response']['marketList']['result'].forEach((item)=>{
        let pair_data = '@MD ' + item['name'] + ' spot ' + item['stock'] + ' ' + item['money'] + ' ' 
            + item['fee_prec'] + ' 1 1 0 0';
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
    message['params'][1].forEach((item)=>{
        var trade_output = '! ' + getUnixTime() + ' ' + message['params'][0] + ' ' + 
        item['type'][0].toUpperCase() + ' ' + parseFloat(item['price']).noExponents() + ' ' + parseFloat(item['amount']).noExponents();
        console.log(trade_output);
    });
}


async function getOrders(message, update){
    // check if bids array is not Null
    if(message['params'][1]['bids']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['params'][2] + ' B '
        var pq = '';
        for(let i = 0; i < message['params'][1]['bids'].length; i++){
            pq += parseFloat(message['params'][1]['bids'][i][1]).noExponents() + '@' + parseFloat(message['params'][1]['bids'][i][0]).noExponents() + '|';
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
    if(message['params'][1]['asks']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['params'][2] + ' S '
        var pq = '';
        for(let i = 0; i < message['params'][1]['asks'].length; i++){
            pq += parseFloat(message['params'][1]['asks'][i][1]).noExponents() + '@' + parseFloat(message['params'][1]['asks'][i][0]).noExponents() + '|';
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


async function ConnectTrades(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "method": "server.ping",
                    "params": [],
                    "id": 10
                }
              ));
              console.log('Ping request sent');
            }
          }, 20000);
        // subscribe to trades and orders for all instruments
        ws.send(JSON.stringify(
            {
                "method": "deals.subscribe",
                "params": currencies,
                "id": 2
            }
        ));
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['method'] === 'deals.update' && dataJSON['params'][1].length <= 10){
                getTrades(dataJSON);
            }else if(dataJSON['method'] === 'deals.update' && dataJSON['params'][1].length > 10){
                // skip trades history
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
            console.log(`Connection closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                ConnectTrades();
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}

async function ConnectOrders(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "method": "server.ping",
                    "params": [],
                    "id": 5
                }
              ));
              console.log('Ping request sent');
            }
          }, 20000);
        // subscribe to orders for all instruments
        ws.send(JSON.stringify(
            {
                "method": "depth.subscribe",
                "params": [
                    pair,
                    100,
                    "0"
                ],
                "id": 3
            }
        ));
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['method'] === 'depth.update'){
                getOrders(dataJSON, !dataJSON['params'][0]);
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
            console.log(`Connection closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                ConnectOrders(pair);
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}

Metadata();
ConnectTrades();
if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    var connections = [];

    for(let pair of currencies){
        connections.push(ConnectOrders(pair));
        await new Promise((resolve) => setTimeout(resolve, 100));
    }
}




