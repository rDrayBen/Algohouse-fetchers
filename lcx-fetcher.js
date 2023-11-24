import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';


// define the websocket and REST URLs
const wsUrl = 'wss://exchange-api.lcx.com/ws';
const restUrl = "https://exchange-api.lcx.com/market/pairs";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var check_activity = {};
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    if(myJson['data'][i]['status'] === true){
        currencies.push(myJson['data'][i]['symbol']);
        check_activity[myJson['data'][i]['symbol']] = false;
    }
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item, index)=>{
        if(item['status'] === true){
            trades_count_5min[item['symbol']] = 0;
            orders_count_5min[item['symbol']] = 0;
            let pair_data = '@MD ' + item['symbol'] + ' spot ' + item['base'] + ' ' + item['quote'] + ' ' 
            + item['pricePrecision'] + ' 1 1 0 0';
            console.log(pair_data);
        }
    })
    console.log('@MDEND')
}


// func to print trades
async function getTrades(message){
    check_activity[message['pair']] = true;
    trades_count_5min[message['pair']] += 1;
    var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + message['pair'] + ' ' + 
    message['data'][2][0] + ' ' + message['data'][0] + ' ' + message['data'][1];
    console.log(trade_output);
}


// func to print orderbooks and deltas
async function getSnapshot(message){
    check_activity[message['pair']] = true;
    // check if bids array is not Null
    if(message['data']['buy'] && message['data']['buy'].length > 0){
        orders_count_5min[message['pair']] += message['data']['buy'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['pair'] + ' B ';
        var pq = '';
        for(let i = 0; i < message['data']['buy'].length; i++){
            pq += (message['data']['buy'][i][1]).noExponents() + '@' + (message['data']['buy'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }

    // check if asks array is not Null
    if(message['data']['sell'] && message['data']['sell'].length > 0){
        orders_count_5min[message['pair']] += message['data']['sell'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['pair'] + ' S '
        var pq = '';
        for(let i = 0; i < message['data']['sell'].length; i++){
            pq += (message['data']['sell'][i][1]).noExponents() + '@' + (message['data']['sell'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }
}


async function getDelta(message){
    check_activity[message['pair']] = true;
    orders_count_5min[message['pair']] += 1;
    var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['pair'] + ' ' + message['data'][2][0] + ' ';
    var pq = (message['data'][1]).noExponents() + '@' + (message['data'][0]).noExponents();
    console.log(order_answer + pq);
}

async function sendStats(){
    commonFunctions.stats(trades_count_5min, orders_count_5min);
    setTimeout(sendStats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
}



async function Connect(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        ws.ping();
        async function subscribe(){
            // subscribe to trades and orders for all instruments
            for (const [key, value] of Object.entries(check_activity)) {
                if(value === false){
                    // sub for trades
                    ws.send(JSON.stringify(
                        {
                            "Topic": "subscribe", 
                            "Type": "trade", 
                            "Pair": key
                        }
                    ));
                    // console.log('subbed for', key);
                    if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                        // sub for orders/deltas
                        ws.send(JSON.stringify(
                            {
                                "Topic": "subscribe", 
                                "Type": "orderbook", 
                                "Pair": key
                            }
                        )); 
                    }
                    
                }
                
            }

            // console.log(check_activity);
            for (var key in check_activity) {
                check_activity[key] = false;
            }
            // console.log(check_activity);
        }
        subscribe();
        setInterval(subscribe, 1800000); // resub every 30 min
        
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['type'] === 'trade' && dataJSON['topic'] === 'update'){
                getTrades(dataJSON);
            }else if(dataJSON['type'] === 'trade' && dataJSON['topic'] === 'snapshot'){
                // skip trades history
            }else if(dataJSON['type'] === 'orderbook' && dataJSON['topic'] === 'snapshot'){
                getSnapshot(dataJSON);
            }else if(dataJSON['type'] === 'orderbook' && dataJSON['topic'] === 'update'){
                getDelta(dataJSON);
            }
            else{
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


