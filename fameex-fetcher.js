import WebSocket from 'ws';
import fetch from 'node-fetch';
import zlib from 'zlib';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const wsUrl = 'wss://www.fameex.com/spot';
const restUrl = "https://api.fameex.com/v1/common/symbols";


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};



// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['pair']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item, index)=>{
        trades_count_5min[item['base'] + '-' + item['quote']] = 0;
        orders_count_5min[item['base'] + '-' + item['quote']] = 0;
        let pair_data = '@MD ' + item['base'] + '-' + item['quote'] + ' spot ' + item['base'] + ' ' + item['quote'] + ' ' 
        + item['pricePrecision'] + ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


// func to print trades
async function getTrades(message){
    message['data'].forEach((item)=>{
        trades_count_5min[item['symbol']] += 1;
        var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + 
        item['symbol'] + ' ' + 
        (item['side'] === 1 ? 'B' : 'S') + ' ' + item['price'] + ' ' + item['amount'];
        console.log(trade_output);
    });
}


// func to print orderbooks and deltas
async function getOrders(message, update){
    // check if bids array is not Null
    if(message['data']['bids']){
        orders_count_5min[message['data']['symbol']] += message['data']['bids'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['data']['symbol'] + ' B '
        var pq = '';
        message['data']['bids'].forEach((element)=>{
            pq += element[1] + '@' + element[0] + '|';
        })
        pq = pq.slice(0, -1);
        // check if the input data is full order book or just update
        if (update){
            console.log(order_answer + pq);
        }
        else{
            console.log(order_answer + pq + ' R');
        }
    }

    // check if asks array is not Null
    if(message['data']['asks']){
        orders_count_5min[message['data']['symbol']] += message['data']['asks'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['data']['symbol'] + ' S '
        var pq = '';
        message['data']['asks'].forEach((element)=>{
            pq += element[1] + '@' + element[0] + '|';
        })
        pq = pq.slice(0, -1);
        // check if the input data is full order book or just update
        if (update){
            console.log(order_answer + pq);
        }
        else{
            console.log(order_answer + pq + ' R');
        }
    }
}

async function sendStats(){
    commonFunctions.stats(trades_count_5min, orders_count_5min);
    setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
}
  

async function ConnectTrades(index){
    // create a new websocket instance
    var wsTrade = new WebSocket(wsUrl);
    
    // call this func when first opening connection
    wsTrade.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (wsTrade.readyState === WebSocket.OPEN) {
              wsTrade.send(JSON.stringify(
                {
                    "op": "ping"
                }
              ));
              console.log('Ping request sent');
            }
          }, 10000);
        
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
        (async () => {
            await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
            console.log(error);
          })();
    };
}
    

async function ConnectDepth(index){
    var wsDepth = new WebSocket(wsUrl);
    // call this func when first opening connection
    wsDepth.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (wsDepth.readyState === WebSocket.OPEN) {
              wsDepth.send(JSON.stringify(
                {
                    "op": "ping"
                }
              ));
              console.log('Ping request sent');
            }
          }, 10000);
        
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
        (async () => {
            await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
            console.log(error);
          })();
    };

}

Metadata();
setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);

var wsTradesArray = [];
for(let i = 0; i < currencies.length; i++){
    wsTradesArray.push(ConnectTrades(i));
    await commonFunctions.sleep(100);
}


if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    var wsDepthArray = [];
    for(let i = 0; i < currencies.length; i++){
        wsDepthArray.push(ConnectDepth(i));
        await commonFunctions.sleep(100);
    }
} 