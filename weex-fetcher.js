import WebSocket from 'ws';
import fetch from 'node-fetch';


// define the websocket and REST URLs
const wsUrl = 'wss://stream-rest.qqkcs.com/v1/stream?compress=false';
const restUrl = "http://www.weex.com/v1/spot/public/coinChainList";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];

// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    if(myJson['data'][i]['coinName'] === "USDT") continue;
    currencies.push(myJson['data'][i]['coinName'].toLowerCase() + '_usdt');
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item)=>{
        if(item['coinName'] !== "USDT"){
            let pair_data = '@MD ' + item['coinName'] + '-USDT spot ' + item['coinName'] + ' ' + 'USDT' + ' ' 
                + '-1' + ' 1 1 0 0';
            console.log(pair_data);
        }
            
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
        var name = message['id'].replace('histroyInit|', '');
        name = name.split('_')[0].toUpperCase() + '-' + name.split('_')[1].toUpperCase();
        var trade_output = '! ' + getUnixTime() + ' ' + name + ' ' + 
        (item['type'] === 1 ? 'S' : 'B') + ' ' + item['prize'] + ' ' + item['count'];
        console.log(trade_output);
    });
}


// func to print orderbooks and deltas
async function getSnapshot(message){
    var name = message['id'].replace('depthInit|', '');
    name = name.split('_')[0].toUpperCase() + '-' + name.split('_')[1].toUpperCase();
    // check if bids array is not Null
    if(message['data']['bids']){
        var order_answer = '$ ' + getUnixTime() + ' ' + name + ' B ';
        var pq = '';
        for(let i = 0; i < message['data']['bids'].length; i++){
            pq += message['data']['bids'][i]['count'] + '@' + message['data']['bids'][i]['prize'] + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }

    // check if asks array is not Null
    if(message['data']['asks']){
        var order_answer = '$ ' + getUnixTime() + ' ' + name + ' S '
        var pq = '';
        for(let i = 0; i < message['data']['asks'].length; i++){
            pq += message['data']['asks'][i]['count'] + '@' + message['data']['asks'][i]['prize'] + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }
}



async function ConnectTrades(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        // ws.ping();
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "action": "requestTheheartbeat",
                    "id": `requestTheheartbeat|ping|${getUnixTime()}`
                }
              ));
              console.log('Ping request sent');
            }
          }, 15000);
        // subscribe to trades and orders for all instruments
        ws.send(JSON.stringify(
            {
                "id": `histroyInit|${pair}`,
                "action": "histroyInit",
                "count": 10,
                "exchangeTypeCode": pair
            }
        ));
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['action'] === 'histroyInit' && dataJSON['data'].length > 5){
                // skip trades history
            }else if(dataJSON['action'] === 'histroyInit' && dataJSON['data'].length <= 5){
                getTrades(dataJSON);
            }else{
                console.log(dataJSON);
            }
        }catch(e){
            // skip confirmation messages cause they can`t be parsed into JSON format without an error
        }
        
        
    };


    // func to handle closing connection
    ws.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                ConnectTrades(pair);
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}

async function ConnectSnapshots(pair){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "action": "requestTheheartbeat",
                    "id": `requestTheheartbeat|ping|${getUnixTime()}`
                }
              ));
              console.log('Ping request sent');
            }
          }, 15000);
        // subscribe to trades and orders for all instruments
        ws.send(JSON.stringify(
            {
                "action": "depthInit",
                "depthAccuracy": "0.00000001",
                "exchangeTypeCode": pair,
                "id": `depthInit|${pair}`
            }
        ));
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['action'] === 'depthInit'){
                getSnapshot(dataJSON);
            }else{
                console.log(dataJSON);
            }
        }catch(e){
            // skip confirmation messages cause they can`t be parsed into JSON format without an error
        }
        
        
    };


    // func to handle closing connection
    ws.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection lost');
            setTimeout(async function() {
                ConnectSnapshots(pair);
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}

Metadata();
var connections = [];

for(let pair of currencies){
    connections.push(ConnectTrades(pair));
    connections.push(ConnectSnapshots(pair));
}

