import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';

// define the websocket and REST URLs
const wsUrl = 'wss://perp-api.openocean.finance/arbitrum/ws/market';
const restUrl = "https://perp-api.openocean.finance/arbitrum/api/v1/public/instruments";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var precision = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000,
    1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000];
var trades_count_5min = {};
var orders_count_5min = {};
    


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['symbol']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item)=>{
        let prec = 11;
        for(let i = 0; i < precision.length; ++i){
            if(item['priceTick'] * precision[i] >= 1){
                prec = i
                break;
            }
        }
        trades_count_5min[item['symbol']] = 0;
        orders_count_5min[item['symbol']] = 0;
        let pair_data = '@MD ' + item['symbol'] + ' perpetual ' + 
            item['symbol'].split('-')[0] + ' ' + item['symbol'].split('-')[1] + ' ' + prec +  ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


async function getTrades(message){
    trades_count_5min[message['symbol']] += 1;
    var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + message['symbol'] + ' ' + 
        message['data']['side'][0] + ' ' + parseFloat(message['data']['price']).noExponents() + ' ' + parseFloat(message['data']['qty']).noExponents();
    console.log(trade_output);
}


async function getOrders(message){
    // check if bids array is not Null
    var messageJSON = JSON.parse(message['data']);
    if(messageJSON['bids'].length > 0){
        orders_count_5min[message['symbol']] += messageJSON['bids'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['symbol'] + ' B '
        var pq = '';
        for(let i = 0; i < messageJSON['bids'].length; i++){
            pq += parseFloat(messageJSON['bids'][i]["1"]).noExponents() + '@' + parseFloat(messageJSON['bids'][i]["0"]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }

    // check if asks array is not Null
    if(messageJSON['asks'].length > 0){
        orders_count_5min[message['symbol']] += messageJSON['asks'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['symbol'] + ' S '
        var pq = '';
        for(let i = 0; i < messageJSON['asks'].length; i++){
            pq += parseFloat(messageJSON['asks'][i]["1"]).noExponents() + '@' + parseFloat(messageJSON['asks'][i]["0"]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
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
        // subscribe to trades and orders for all instruments
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "op":"subscribe",
                    "args":[
                        {
                            "channel":"trade",
                            "symbol":pair
                        }
                    ]
                }
            ));
            if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                ws.send(JSON.stringify(
                    {
                        "op":"subscribe",
                        "args":[
                            {
                                "channel":"depth20",
                                "symbol":pair
                            }
                        ]
                    }
                ));
            }
            
        });
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            if(event.data === 'ping'){// ping to keep connection alive
                console.log(event.data);
                ws.send(
                    "pong"	
                );
                console.log('Ping request sent');
            }
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['channel'] === 'trade') {
                getTrades(dataJSON);
            }
            else if (dataJSON['channel'] === 'depth20') {
                getOrders(dataJSON);
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
            console.log(`Connection closed with code ${event.code} and message: ${event.reason}`);
            setTimeout(async function() {
                await Connect();
                }, 500);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                await Connect();
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
await Connect();
