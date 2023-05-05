import WebSocket from 'ws';
import fetch from 'node-fetch';


// define the websocket and REST URLs
const wsUrl = 'wss://fapi.bkex.com/fapi/v2/ws';
const restUrl = "https://api.bkex.com/v2/common/symbols";


// create a new websocket instance
var ws = new WebSocket(wsUrl);


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['symbol']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item, index)=>{
        let pair_data = '@MD ' + item['symbol'] + ' spot ' + item['symbol'].split('_')[0] + ' ' + item['symbol'].split('_')[1] + ' ' 
        + item['pricePrecision'] + ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


//function to get current time in unix format
function getUnixTime(){
    return Math.floor(Date.now());
}


// func to print trades
async function getTrades(message){
    var trade_output = '! ' + getUnixTime() + ' ' + 
    message['type'].split('.')[0].toUpperCase() + ' ' + 
    (message['data'][1] === '1' ? 'B ' : 'S ') + message['data'][0] + ' ' + message['data'][2];
    console.log(trade_output);
}


// func to print orderbooks and deltas
async function getOrders(message, update){
    // check if bids array is not Null
    if(message['data']['bids']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['data']['symbol'].toUpperCase() + ' B '
        var pq = '';
        for(let i = 0; i < message['data']['bids'].length; i+=2){
            pq += message['data']['bids'][i+1] + '@' + message['data']['bids'][i] + '|';
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
    if(message['data']['asks']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['data']['symbol'].toUpperCase() + ' S '
        var pq = '';
        for(let i = 0; i < message['data']['asks'].length; i+=2){
            pq += message['data']['asks'][i+1] + '@' + message['data']['asks'][i] + '|';
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


// call this func when first opening connection
ws.onopen = function(e) {
    // call metadata to execute
    Metadata();
    // create ping function to keep connection alive
    ws.ping();
    // subscribe to trades and orders for all instruments
    currencies.forEach((item) =>{
        // sub for trades
        ws.send(JSON.stringify(
            {
                "event":"sub",
                "topic":`${item.toLowerCase()}.trade`
            }
        ))
        //sub for orderbooks with depth 20
        ws.send(JSON.stringify(
            {
                "event":"sub",
                "topic":`${item.toLowerCase()}.20deep`
            }
        ))
        // sub for deltas in 5 top positions of orderbook
        ws.send(JSON.stringify(
            {
                "event":"sub",
                "topic":`${item.toLowerCase()}.updateDepth`
            }
        ))
    })
};


// func to handle input messages
ws.onmessage = function(event) {
    // parse input data to JSON format
    var dataJSON = JSON.parse(event.data);
    if (dataJSON['type'].split('.')[1] === 'trade'){
        getTrades(dataJSON);
    }else if(dataJSON['type'].split('.')[1] === '20deep'){
        getOrders(dataJSON, false);
    }else if(dataJSON['type'].split('.')[1] === 'updateDepth'){
        getOrders(dataJSON, true);
    }else{
        console.log(dataJSON);
    }
};


// func to handle closing connection
ws.onclose = function(event) {
    if (event.wasClean) {
        console.log(`Connection closed with code ${event.code} and reason ${event.reason}`);
    } else {
        console.log('Connection lost');
    }
};

// func to handle errors
ws.onerror = function(error) {
    console.log(`Error ${error} occurred`);
};

