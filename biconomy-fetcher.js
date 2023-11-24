import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const wsUrl = 'wss://www.biconomy.com/ws';
const restUrl = "https://www.biconomy.com/api/v1/exchangeInfo";


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    if(myJson[i]['status'] === 'trading'){
        currencies.push(myJson[i]['symbol']);
    }
}


// print metadata about pairs
async function Metadata(){
    myJson.forEach((item, index)=>{
        if(item['status'] === 'trading'){
            trades_count_5min[item['symbol']] = 0;
            orders_count_5min[item['symbol']] = 0;
            let pair_data = '@MD ' + item['symbol'] + ' spot ' + item['baseAsset'] + ' ' + item['quoteAsset'] + ' ' 
            + item['quoteAssetPrecision'] + ' 1 1 0 0';
            console.log(pair_data);
        }
    })
    console.log('@MDEND')
}


// func to print trades
async function getTrades(message){
    trades_count_5min[message['params'][0]] += message['params'][1].length;
    message['params'][1].forEach((item)=>{
        var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + 
        message['params'][0] + ' ' + 
        item['type'][0].toUpperCase() + ' ' + parseFloat(item['price']).noExponents() + ' ' + parseFloat(item['amount']).noExponents();
        console.log(trade_output);
    });
}


// func to print orderbooks and deltas
async function getOrders(message, update){
    // check if bids array is not Null
    if(message['params'][1]['bids']){
        orders_count_5min[message['params'][2]] += message['params'][1]['bids'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['params'][2] + ' B '
        var pq = '';
        for(let i = 0; i < message['params'][1]['bids'].length; i++){
            pq += (parseFloat(message['params'][1]['bids'][i][1])).noExponents() + '@' + (parseFloat(message['params'][1]['bids'][i][0])).noExponents() + '|';
        }
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
    if(message['params'][1]['asks']){
        orders_count_5min[message['params'][2]] += message['params'][1]['asks'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['params'][2] + ' S '
        var pq = '';
        for(let i = 0; i < message['params'][1]['asks'].length; i++){
            pq += (parseFloat(message['params'][1]['asks'][i][1])).noExponents() + '@' + (parseFloat(message['params'][1]['asks'][i][0])).noExponents() + '|';
        }
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





function Connect1(){
    var ws1 = new WebSocket(wsUrl);
    
    // call this func when first opening connection
    ws1.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws1.readyState === WebSocket.OPEN) {
              ws1.send(JSON.stringify(
                {
                    "method":"server.ping",
                    "params":[],
                    "id":5160
                }
              ));
              console.log('Ping request sent');
            }
          }, 60000);
        // sub for trades
        ws1.send(JSON.stringify(
            {
                "method":"deals.subscribe",
                "params":currencies,
                "id":20
            }
        ))
    };


    // func to handle input messages
    ws1.onmessage = function(event) {
        try{
            var dataJSON = JSON.parse(event.data);
            // console.log(dataJSON);
            if (dataJSON['method'] === 'deals.update' && dataJSON['params'][1].length < 5){
                getTrades(dataJSON);
            }else if (dataJSON['method'] === 'deals.update' && dataJSON['params'][1].length > 5){
                // skip trades history
            }else{
                console.log(dataJSON);
            }        
        }catch(e){
            // possible errors while parsing data to json format
            (async () => {
                await commonFunctions.sleep(1000); // Sleep for 1000 milliseconds (1 second) 
                console.log(event.data);
              })();
        }
    };


    // func to handle closing connection
    ws1.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 1 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 1 lost');
            setTimeout(function() {
                Connect1();
                }, 500);
        }
    };

    // func to handle errors
    ws1.onerror = function(error) {
        console.log(error);
        (async () => {
            await commonFunctions.sleep(1000); // Sleep for 1000 milliseconds (1 second) 
          })();
    };
}


async function sendStats(){
    commonFunctions.stats(trades_count_5min, orders_count_5min);
    setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
}
  

async function Connect2(index){
    // create a new websocket instance
    var ws2 = new WebSocket(wsUrl);
    
    ws2.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws2.readyState === WebSocket.OPEN) {
              ws2.send(JSON.stringify(
                {
                    "method":"server.ping",
                    "params":[],
                    "id":5160
                }
              ));
              console.log('Ping request sent');
            }
          }, 60000);
        // sub for orders
        ws2.send(JSON.stringify(
            {
                "method":"depth.subscribe",
                "params":[currencies[index],100,"0.00000001"],
                "id":2000+index
            }
        ))        
    }


    // func to handle input messages
    ws2.onmessage = function(event) {
        var dataJSON;
        try{
            dataJSON = JSON.parse(event.data);
            if (dataJSON['method'] === 'depth.update' && dataJSON['params'][0] === true){
                getOrders(dataJSON, false);
            }else if (dataJSON['method'] === 'depth.update' && dataJSON['params'][0] === false){
                getOrders(dataJSON, true);
            }else{
                console.log(dataJSON);
            }        
        }catch(e) {
            // console.log(e);
            // error may occurr cause some part of incoming data can`t be properly parsed in json format due to inapropriate symbols
            // error only occurrs in messages that confirming subs
            // error caused here is exchanges fault
            (async () => {
                await commonFunctions.sleep(1000); // Sleep for 1000 milliseconds (1 second) 
                console.log(event.data);
              })();
        }
    };


    // func to handle closing connection
    ws2.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 2 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 2 lost');
            setTimeout(async function() {
                Connect2(index);
                }, 500);
        }
    };

    // func to handle errors
    ws2.onerror = function(error) {
        console.log(error);
        (async () => {
            await commonFunctions.sleep(1000); // Sleep for 1000 milliseconds (1 second) 
          })();
    };
    
}

// call metadata to execute
Metadata();
setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);

Connect1();

if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    var wsArr = [];
    for(let i = 0; i < currencies.length; i++){
        wsArr.push(Connect2(i));
        await commonFunctions.sleep(500);
    }
}

