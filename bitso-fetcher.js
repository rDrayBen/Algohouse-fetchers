import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const wsUrl = 'wss://ws.bitso.com';
const restUrl = "https://bitso.com/api/v3/ticker?books=all";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};

// extract symbols from JSON returned information
for(let i = 0; i < myJson['payload'].length; ++i){
    currencies.push(myJson['payload'][i]['book']);
}


// print metadata about pairs
async function Metadata(){
    myJson['payload'].forEach((item)=>{
        trades_count_5min[item['book'].toUpperCase()] = 0;
        orders_count_5min[item['book'].toUpperCase()] = 0;
        let pair_data = '@MD ' + item['book'].toUpperCase() + ' spot ' + item['book'].split('_')[0].toUpperCase() + 
            ' ' + item['book'].split('_')[1].toUpperCase() + ' ' 
            + '-1 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


async function getTrades(message){
    trades_count_5min[message['book'].toUpperCase()] += message['payload'].length;
    message['payload'].forEach((trade)=>{
        var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + message['book'].toUpperCase() + ' ' + 
        (trade['t'] == 0 ? 'B' : 'S') + ' ' + parseFloat(trade['r']).noExponents() + ' ' + parseFloat(trade['a']).noExponents();
        console.log(trade_output);
    });
}


async function getOrders(message, update){
    if(update){
        orders_count_5min[message['book'].toUpperCase()] += 1;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['book'].toUpperCase() + ' ' + (message['payload'][0]['t'] === 1 ? 'B ' : 'S ');
        var pq = '';
        if(message['payload'][0]['s'] === 'open'){
            pq += parseFloat(message['payload'][0]['a']).noExponents() + '@' + parseFloat(message['payload'][0]['r']).noExponents();
        }else if(message['payload'][0]['s'] === 'cancelled'){
            pq += '0@' + parseFloat(message['payload'][0]['r']).noExponents();
        }
        console.log(order_answer + pq);
        
    }else{
        orders_count_5min[message['book'].toUpperCase()] += message['payload']['bids'].length + message['payload']['asks'].length;
        // check if bids array is not Null
        if(message['payload']['bids']){
            var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['book'].toUpperCase() + ' B '
            var pq = '';
            for(let i = 0; i < message['payload']['bids'].length; i++){
                pq += parseFloat(message['payload']['bids'][i]['a']).noExponents() + '@' + parseFloat(message['payload']['bids'][i]['r']).noExponents() + '|';
            }
            pq = pq.slice(0, -1);
            console.log(order_answer + pq + ' R');
        }

        // check if asks array is not Null
        if(message['payload']['asks']){
            var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['book'].toUpperCase() + ' S '
            var pq = '';
            for(let i = 0; i < message['payload']['asks'].length; i++){
                pq += parseFloat(message['payload']['asks'][i]['a']).noExponents() + '@' + parseFloat(message['payload']['asks'][i]['r']).noExponents() + '|';
            }
            pq = pq.slice(0, -1);
            console.log(order_answer + pq + ' R');
        }
    }
}


async function sendStats(){
    commonFunctions.stats(trades_count_5min, orders_count_5min);
    setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
}


async function Connect(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        ws.ping();
        // subscribe to trades and orders for all instruments
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "action": 'subscribe',
                    "book": pair,
                    "type": 'trades'
                }
            ));
            if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                ws.send(JSON.stringify(
                    {
                        "action": "subscribe",
                        "book": pair,
                        "type": "orders"
                    }
                ));
                ws.send(JSON.stringify(
                    {
                        "action": "subscribe",
                        "book": pair,
                        "type": "diff-orders"
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
            if (dataJSON.type == 'trades' && dataJSON.payload) {
                getTrades(dataJSON);
            }
            else if (dataJSON.type === 'diff-orders' && dataJSON.payload) {
                getOrders(dataJSON, true);
            }
            else if (dataJSON.type === 'orders' && dataJSON.payload) {
                getOrders(dataJSON, false);
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
            console.log('Connection lost');
            setTimeout(async function() {
                Connect();
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
Connect();



