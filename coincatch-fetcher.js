import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';
import * as commonFunctions from './CommonFunctions/CommonFunctions.js';


var CURRENT_MODE = 'SPOT';

// Extract command-line arguments starting from index 2
const args = process.argv.slice(2);

// Check if there are any arguments
if (args.length > 0) {
    // Iterate through the arguments
    for(let i = 0; i < args.length; i++){
        // Check if the argument starts with '-'
        if (args[i].startsWith('-') && args[i].substring(1) === 'perpetual') {
            // FUTURES();
            CURRENT_MODE = 'FUTURES';
            break;
        }
    }
} else {
    // SPOT();
    CURRENT_MODE = 'SPOT';
}


const SpotWsUrl = 'wss://stream.coincatch.com/spot/v1/stream?compress=false';
const SpotRestUrl = "https://api.coincatch.com/api/mix/v1/market/contracts?productType=umcbl";

const FuturesWsUrl = 'wss://stream.coincatch.com/mix/v1/stream?compress=false&terminalType=1';
const FuturesRestUrl = "https://www.coincatch.com/v1/mcp/contract/list2";

var response;
var myJson;
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};

if(CURRENT_MODE === 'SPOT'){
    response = await fetch(SpotRestUrl);
    //extract JSON from the http response
    myJson = await response.json(); 

    // extract symbols from JSON returned information
    for(let i = 0; i < myJson['data'].length; ++i){
        currencies.push(myJson['data'][i]['symbolName']);
    }
}else if(CURRENT_MODE === 'FUTURES'){
    response = await fetch(FuturesRestUrl, {
        method:"POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            "businessLine": 10,
            "secondBusinessLine": "N/A",
            "languageType": 0
        }),
    });
    //extract JSON from the http response
    myJson = await response.json(); 

    // extract symbols from JSON returned information
    for(let i = 0; i < myJson['data']['listDTOS'][0]['symbolDTOList'].length; ++i){
        currencies.push(myJson['data']['listDTOS'][0]['symbolDTOList'][i]['symbolDisplayName']);
    }
}

// print metadata about pairs
async function Metadata(){
    if(CURRENT_MODE === 'SPOT'){
        myJson['data'].forEach((item)=>{
            trades_count_5min[item['symbolName']] = 0;
            orders_count_5min[item['symbolName']] = 0;
            let pair_data = '@MD ' + item['symbolName'] + ' spot ' + item['baseCoin'] + 
                ' ' + item['quoteCoin'] + ' ' + item['pricePlace'] +  ' 1 1 0 0';
            console.log(pair_data);
        })
        console.log('@MDEND');
    }else if(CURRENT_MODE === 'FUTURES'){
        myJson['data']['listDTOS'][0]['symbolDTOList'].forEach((item)=>{
            trades_count_5min[item['symbolDisplayName']] = 0;
            orders_count_5min[item['symbolDisplayName']] = 0;
            let pair_data = '@MD ' + item['symbolDisplayName'] + ' perpetual ' + item['baseSymbol'] + 
                ' ' + item['pricedSymbol'] + ' ' + (item['buyLimitPriceRatio'].split('0').length - 1) +  ' 1 1 0 0';
            console.log(pair_data);
        })
        console.log('@MDEND');
    }   
}


async function getTrades(message){
    trades_count_5min[message['arg']['instId']] += message['data'].length;
    message['data'].forEach((trade)=>{
        var trade_output = '! ' + commonFunctions.getUnixTime() + ' ' + message['arg']['instId'] + ' ' + 
        (trade[3] === 1 ? 'S' : 'B') + ' ' + parseFloat(trade[1]).noExponents() + ' ' + parseFloat(trade[2]).noExponents();
        console.log(trade_output);
    })
}


async function getOrders(message){
    // check if bids array is not Null
    if(message['data'][0]['bids'] && message['data'][0]['bids'].length > 0){
        orders_count_5min[message['arg']['instId']] += message['data'][0]['bids'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['arg']['instId'] + ' B '
        var pq = '';
        for(let i = 0; i < message['data'][0]['bids'].length; i++){
            pq += parseFloat(message['data'][0]['bids'][i][1]).noExponents() + '@' + parseFloat(message['data'][0]['bids'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
    }

    // check if asks array is not Null
    if(message['data'][0]['asks'] && message['data'][0]['asks'].length > 0){
        orders_count_5min[message['arg']['instId']] += message['data'][0]['asks'].length;
        var order_answer = '$ ' + commonFunctions.getUnixTime() + ' ' + message['arg']['instId'] + ' S '
        var pq = '';
        for(let i = 0; i < message['data'][0]['asks'].length; i++){
            pq += parseFloat(message['data'][0]['asks'][i][1]).noExponents() + '@' + parseFloat(message['data'][0]['asks'][i][0]).noExponents() + '|';
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
    var ws;
    if(CURRENT_MODE === 'SPOT'){
        ws = new WebSocket(SpotWsUrl);
    }else if(CURRENT_MODE === 'FUTURES'){
        ws = new WebSocket(FuturesWsUrl);
    }
    
    ws.onopen = function(e) {
        
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(
                    "ping"
                );
            console.log('Ping request sent');
            }
        }, 19000);
        if(CURRENT_MODE === 'SPOT'){
            // subscribe to trades and orders for all instruments
            ws.send(JSON.stringify(
                {
                    "op": "subscribe",
                    "args": [
                        {
                            "channel": "trade",
                            "instType": "sp",
                            "instId": pair
                        }
                    ]
                }
            ));
            if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                ws.send(JSON.stringify(
                    {
                        "op": "subscribe",
                        "args": [
                            {
                                "channel": "books15",
                                "instType": "sp",
                                "instId": pair
                            }
                        ]
                    }
                ));
            }
        }else if(CURRENT_MODE === 'FUTURES'){
            // subscribe to trades and orders for all instruments
            ws.send(JSON.stringify(
                {
                    "op": "subscribe",
                    "args": [
                        {
                            "channel": "trade",
                            "instType": "mc",
                            "instId": pair
                        }
                    ]
                }
            ));
            if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                ws.send(JSON.stringify(
                    {
                        "op": "subscribe",
                        "args": [
                            {
                                "channel": "depth5",
                                "instType": "mc",
                                "instId": pair
                            }
                        ]
                    }
                ));
            }
        }
        
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            if(CURRENT_MODE === "SPOT"){
                // parse input data to JSON format
                let dataJSON = JSON.parse(event.data);

                if(dataJSON['action'] === 'snapshot' && dataJSON['arg']['channel'] === 'trade'){
                    // skip trades history
                }else if(dataJSON['action'] === 'update' && dataJSON['arg']['channel'] === 'trade'){
                    getTrades(dataJSON);
                }else if(dataJSON['action'] === 'snapshot' && dataJSON['arg']['channel'] === 'books15'){
                    getOrders(dataJSON);
                }
                else{
                    console.log(dataJSON);
                }
            }else if(CURRENT_MODE === 'FUTURES'){
                // parse input data to JSON format
                let dataJSON = JSON.parse(event.data);

                if(dataJSON['action'] === 'snapshot' && dataJSON['arg']['channel'] === 'trade'){
                    // skip trades history
                }else if(dataJSON['action'] === 'update' && dataJSON['arg']['channel'] === 'trade'){
                    getTrades(dataJSON);
                }else if(dataJSON['action'] === 'snapshot' && dataJSON['arg']['channel'] === 'depth5'){
                    getOrders(dataJSON);
                }
                else{
                    console.log(dataJSON);
                }
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
            console.log(`Connection closed with code ${event.code} and reason ${event.reason} for pair ${pair}`);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                Connect(pair);
                }, 1000);
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

var connection = [];

for(let pair of currencies){
    connection.push(Connect(pair));
}
