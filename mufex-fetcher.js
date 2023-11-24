import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const wsUrl = 'wss://ws2.mufex.finance/realtime_public';
const restUrl = "https://www.mufex.finance/mapi/trade/private/v1/position/lp-list-all";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};

// extract symbols from JSON returned information
for(let i = 0; i < myJson['data']['list'].length; ++i){
    currencies.push(myJson['data']['list'][i]['data']['symbol']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data']['list'].forEach((item)=>{
        trades_count_5min[item['data']['symbol']] = 0;
        orders_count_5min[item['data']['symbol']] = 0;
        let pair_data = '@MD ' + item['data']['symbol'] + ' perpetual ' + 
        item['data']['symbol'].replace(item['data']['coin'], '') + ' ' + item['data']['coin'] + ' ' + '-1' +  ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


async function getTrades(message){
    trades_count_5min[message['data']['s']] += message['data']['d'].length;
    message['data']['d'].forEach((trade)=>{
        var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + message['data']['s'] + ' ' + 
            (trade[4] === 'b' ? 'B' : 'S') + ' ' + parseFloat(trade[1]).noExponents() + ' ' + parseFloat(trade[2]).noExponents();
        console.log(trade_output);
    })
}


async function getOrders(message, update){
    // check if bids array is not Null
    if(message['data']['b'].length > 0){
        orders_count_5min[message['data']['s']] += message['data']['b'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['data']['s'] + ' B '
        var pq = '';
        for(let i = 0; i < message['data']['b'].length; i++){
            pq += parseFloat(message['data']['b'][i][1]).noExponents() + '@' + parseFloat(message['data']['b'][i][0]).noExponents() + '|';
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
    if(message['data']['a'].length > 0){
        orders_count_5min[message['data']['s']] += message['data']['a'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['data']['s'] + ' S '
        var pq = '';
        for(let i = 0; i < message['data']['a'].length; i++){
            pq += parseFloat(message['data']['a'][i][1]).noExponents() + '@' + parseFloat(message['data']['a'][i][0]).noExponents() + '|';
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

async function sendStats(){
    commonFunctions.stats(trades_count_5min, orders_count_5min);
    setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
}


async function Connect(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "op":"ping",
                    "args":[commonFunctions.getUnixTime()]
                }
              ));
              console.log('Ping request sent');
            }
          }, 10000);
        if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
            // subscribe to trades and orders for given instrument
            ws.send(JSON.stringify(
                {
                    "args": [
                        `books-25.${pair}`,
                        `trades-100.${pair}`
                    ],
                    "op": "subscribe"
                }
            ));
        }else{
            // subscribe to trades for given instrument
            ws.send(JSON.stringify(
                {
                    "args": [
                        `trades-100.${pair}`
                    ],
                    "op": "subscribe"
                }
            ));
        }
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            // console.log(dataJSON);
            if (dataJSON['topic'].includes('trades') && dataJSON['type'] === 'delta') {
                getTrades(dataJSON);
            }
            else if (dataJSON['topic'].includes('trades') && dataJSON['type'] === 'snapshot') {
                // skip trading history
            }
            else if(dataJSON['topic'].includes('books') && dataJSON['type'] === 'snapshot'){
                getOrders(dataJSON, false);
            }
            else if(dataJSON['topic'].includes('books') && dataJSON['type'] === 'delta'){
                getOrders(dataJSON, true);
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
    connections.push(Connect(pair));
    await commonFunctions.sleep(100);
}
