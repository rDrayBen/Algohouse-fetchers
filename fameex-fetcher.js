import WebSocket from 'ws';
import fetch from 'node-fetch';
import zlib from 'zlib';


// define the websocket and REST URLs
const wsUrl = 'wss://www.fameex.com/spot';
const restUrl = "https://api.fameex.com/v1/common/symbols";


// create a new websocket instance
var ws = new WebSocket(wsUrl);


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['pair']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item, index)=>{
        let pair_data = '@MD ' + item['pair'] + ' spot ' + item['base'] + ' ' + item['quote'] + ' ' 
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
    message['data'].forEach((item)=>{
        var trade_output = '! ' + getUnixTime() + ' ' + 
        item['symbol'] + ' ' + 
        (item['side'] === 1 ? 'B' : 'S') + ' ' + item['price'] + ' ' + item['amount'];
        console.log(trade_output);
    });
}


// func to print orderbooks and deltas
async function getOrders(message, update){
    // check if bids array is not Null
    if(message['data']['bids']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['data']['symbol'] + ' B '
        var pq = '';
        message['data']['bids'].forEach((element)=>{
            pq += element[1] + '@' + element[0] + '|';
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
    if(message['data']['asks']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['data']['symbol'] + ' S '
        var pq = '';
        message['data']['asks'].forEach((element)=>{
            pq += element[1] + '@' + element[0] + '|';
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

var wsTradesArray = [];
for(let i = 0; i < currencies.length; i++){
    wsTradesArray.push(ConnectTrades(i));
}
  

async function ConnectTrades(index){
    // create a new websocket instance
    var wsTrade = new WebSocket(wsUrl);
    
    // call this func when first opening connection
    wsTrade.onopen = function(e) {
        // create ping function to keep connection alive
        wsTrade.ping();
        
        // sub for orders
        wsTrade.send(JSON.stringify(
            {
                "op": "sub",
                "topic": "spot.market.last_trade",
                "params": {
                "symbol": currencies[index].split('/')[0] + '-' + currencies[index].split('/')[1]
                }
            }
        ))        
    }

    // func to handle input messages
    wsTrade.onmessage = function(event) {
        // parse input data to JSON format
        const compressedData = Buffer.from(event.data, 'base64');
        try{
            zlib.gunzip(compressedData, (err, uncompressedData) => {
                // console.log(uncompressedData);
                try{
                    var dataJSON = JSON.parse(uncompressedData);
                    // console.log(dataJSON);
                    if (dataJSON['topic'] === 'spot.market.last_trade' && 'data' in dataJSON){
                        getTrades(dataJSON);
                    }else{
                        console.log(dataJSON);
                    }
                }catch(e){
    
                }   
            });  
            
        }catch(e){

        }
    };


    // func to handle closing connection
    wsTrade.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 1 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 1 lost');
            setTimeout(async function() {
                ConnectTrades(index);
                }, 500);
        }
    };

    // func to handle errors
    wsTrade.onerror = function(error) {

    };
}
    


var wsDepthArray = [];
for(let i = 0; i < currencies.length; i++){
    wsDepthArray.push(ConnectDepth(i));
}
  

async function ConnectDepth(index){
    var wsDepth = new WebSocket(wsUrl);
    // call this func when first opening connection
    wsDepth.onopen = function(e) {
        // create ping function to keep connection alive
        wsDepth.ping();
        
        // sub for orders
        wsDepth.send(JSON.stringify(
            {
                "op": "sub",
                "topic": "spot.market.depth",
                "params": {
                    "symbol": currencies[index].split('/')[0] + '-' + currencies[index].split('/')[1],
                    "step": "step0"
                }
            }
        ))        
    }
    // func to handle input messages
    wsDepth.onmessage = function(event) {
        // parse input data to JSON format
        const compressedData = Buffer.from(event.data, 'base64');
        try{
            zlib.gunzip(compressedData, (err, uncompressedData) => {
                try{
                    //uncompressedData = uncompressedData.trim();
                    var dataJSON = JSON.parse(uncompressedData);
                    // console.log(dataJSON);
                    if(dataJSON['topic'] === 'spot.market.depth' && 'data' in dataJSON){
                        getOrders(dataJSON, false); 
                    }else{
                        console.log(dataJSON);
                    }
                }catch(e){
    
                }   
            });  
            
        }catch(e){

        }
    };


    // func to handle closing connection
    wsDepth.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection depth closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection depth lost');
            setTimeout(async function() {
                ConnectDepth(index);
                }, 500);
        }
    };

    // func to handle errors
    wsDepth.onerror = function(error) {

    };

}

Metadata();
// ConnectTrades();
// ConnectDepth();