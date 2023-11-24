import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const wsUrl = 'wss://tradeogre.com:8443/';
const restUrl = "https://tradeogre.com/api/v1/markets";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    currencies.push(Object.keys(myJson[i])[0]);
}

// print metadata about pairs
async function Metadata(){
    currencies.forEach((item)=>{
        trades_count_5min[item] = 0;
        orders_count_5min[item] = 0;
        let pair_data = '@MD ' + item + ' spot ' + item.split('-')[0] + ' ' + item.split('-')[1] + ' ' + '-1' +  ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


async function getTrades(message, pair_name){
    trades_count_5min[pair_name] += 1;
    var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + pair_name + ' ' + 
        (message['d'][1] === 0 ? 'B' : 'S') + ' ' + parseFloat(message['d'][2]).noExponents() + ' ' + parseFloat(message['d'][3]).noExponents();
    console.log(trade_output);
}


async function getOrders(message, update, pair_name){
    if(!update || message['a'] === 'add'){
        orders_count_5min[pair_name] += Object.keys(message['d']).length; 
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + pair_name + ' ' + message['t'][0].toUpperCase() + ' ';
        var pq = '';
        for(var key in message['d']){
            pq += parseFloat(message['d'][key]).noExponents() + '@' + parseFloat(key).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        if(pq != ''){
            // check if the input data is full order book or just update
            if (update){
                console.log(order_answer + pq);
            }
            else{
                console.log(order_answer + pq + ' R');
            }  
        }
        
    }else{
        orders_count_5min[pair_name] += Object.keys(message['d']).length; 
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + pair_name + ' ' + message['t'][0].toUpperCase() + ' ';
        var pq = '';
        for(var key in message['d']){
            pq += '0' + '@' + parseFloat(key).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
    }
    
}

async function sendStats(){
    commonFunctions.stats(trades_count_5min, orders_count_5min);
    setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
}


async function Connect(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        ws.ping();
        // subscribe to trades and orders for all instruments
        ws.send(JSON.stringify(
            {
                "a":"submarket",
                "name":pair
            }
        ));
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON.a === 'newhistory') {
                getTrades(dataJSON, pair);
            }
            else if(dataJSON.a === 'orders'){
                if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                    getOrders(dataJSON, false, pair);
                }
                
            }
            else if(dataJSON.a === 'add' || dataJSON.a === 'sub'){
                if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                    getOrders(dataJSON, true, pair);
                }
                
            }
            else {
                console.log(dataJSON);
            }
        }catch(e){
            // skip confirmation messages cause they can`t be parsed into JSON format without an error
            (async () => {
                await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
                console.log(event.data);
              })();
        }
    };


    // func to handle closing connection
    ws.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection lost with pair', pair);
            setTimeout(async function() {
                Connect(pair);
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
        (async () => {
            await commonFunctions.sleep(1000); // commonFunctions.sleep for 1000 milliseconds (1 second) 
          })();
    };
}

Metadata();
setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
var connections = [];

for(let pair of currencies){
    connections.push(await Connect(pair));
    await commonFunctions.sleep(100);
}

