import WebSocket from 'ws';
import fetch from 'node-fetch';

// define the websocket and REST URLs
const wsUrl = 'wss://btc-alpha.com/alp-ws';
const restUrl = "https://btc-alpha.com/api/v1/pairs/";


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];


// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    currencies.push(myJson[i]['name']);
}


// print metadata about pairs
async function Metadata(){
    myJson.forEach((item, index)=>{
        let pair_data = '@MD ' + item['name'] + ' spot ' + item['currency1'] + ' ' + item['currency2'] + ' ' 
        + item['price_precision'] + ' 1 1 0 0';
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
    message[3] + ' ' + 
    message[6][0].toUpperCase() + ' ' + message[4] + ' ' + message[5];
    console.log(trade_output);
    // example of message ["t",1683703622,274430761,"TRX_USDT","3921.43400516","0.06930900","sell"]
}


// func to print orderbooks and deltas
async function getOrders(message, update){
    // check if bids array is not Null
    if(message[3].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message[2] + ' B '
        var pq = '';
        message[3].forEach((element)=>{
            pq += (element[1][0] === '-' ? '0' : element[1]) + '@' + element[0] + '|';
        })
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
    if(message[4].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message[2] + ' S '
        var pq = '';
        message[4].forEach((element)=>{
            pq += (element[1][0] === '-' ? '0' : element[1]) + '@' + element[0] + '|';
        })
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

  

async function Connect(){
    // create a new websocket instance
    var wsConnection = new WebSocket(wsUrl);
    
    // call this func when first opening connection
    wsConnection.onopen = function(e) {
        // create ping function to keep connection alive
        wsConnection.ping();
        
        // sub for trades for all currency pairs in 1 message
        wsConnection.send(JSON.stringify(
                ["subscribe", "trade.*"]
        ))  
    
    }

    // func to handle input messages
    wsConnection.onmessage = function(event) {
        var dataJSON = JSON.parse(event.data);
        if(dataJSON[0] === 't'){
            getTrades(dataJSON);
        }else if(dataJSON[0] === 'd'){
            getOrders(dataJSON, true);
        }else{
            console.log(dataJSON);
        }
    };


    // func to handle closing connection
    wsConnection.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 1 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 1 lost');
            setTimeout(async function() {
                Connect();
                }, 500);
        }
    };

    // func to handle errors
    wsConnection.onerror = function(error) {
        // console.log(`Error ${error} occurred`);
    };
}
    

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
  

async function ConnectDepth1(/*index*/){
    var wsDepth = new WebSocket(wsUrl);
    // call this func when first opening connection
    wsDepth.onopen = function(e) {
        // create ping function to keep connection alive
        wsDepth.ping();
        for(let i = 0; i < 20; i++){ 
            wsDepth.send(JSON.stringify(
                ["subscribe", `diff.${currencies[i]}`]
            )) 
            sleep(500);
        }
              
    }
    // func to handle input messages
    wsDepth.onmessage = function(event) {
        // parse input data to JSON format
        var dataJSON = JSON.parse(event.data);
        if(dataJSON[0] === 'd'){
            getOrders(dataJSON, true);
        }else{
            console.log(dataJSON);
        }
    };


    // func to handle closing connection
    wsDepth.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection depth closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection depth lost with event', event);
            setTimeout(async function() {
                ConnectDepth1();
                }, 500);
        }
    };

    // func to handle errors
    wsDepth.onerror = function(error) {
        
    };

}

async function ConnectDepth2(/*index*/){
    var wsDepth = new WebSocket(wsUrl);
    // call this func when first opening connection
    wsDepth.onopen = function(e) {
        // create ping function to keep connection alive
        wsDepth.ping();
        for(let i = 20; i < currencies.length; i++){   
            wsDepth.send(JSON.stringify(
                ["subscribe", `diff.${currencies[i]}`]
            )) 
            sleep(500);
        }
              
    }
    // func to handle input messages
    wsDepth.onmessage = function(event) {
        // parse input data to JSON format
        var dataJSON = JSON.parse(event.data);
        if(dataJSON[0] === 'd'){
            getOrders(dataJSON, true);
        }else{
            console.log(dataJSON);
        }
    };


    // func to handle closing connection
    wsDepth.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection depth closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection depth lost with event', event);
            setTimeout(async function() {
                ConnectDepth2();
                }, 500);
        }
    };

    // func to handle errors
    wsDepth.onerror = function(error) {
        
    };

}


Metadata();
Connect();
ConnectDepth1();
ConnectDepth2();


// as exchange has 38 spot pairs, out of which 37 are working, each websocket connection can handle up to 20 subs. 
// so in order to sub for all trading pairs without creating a websocket connection for each trading pair 
// I created 2 separate websocket connections which handle 1-19 and 20-38 index of pairs respectfully
// There are no snapshots in this fetcher