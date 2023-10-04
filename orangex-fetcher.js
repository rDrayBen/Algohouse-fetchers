import WebSocket from 'ws';
import fetch from 'node-fetch';

// define the websocket and REST URLs
const wsUrl = 'wss://api.orangex.com/ws/api/v1';
const restUrl = "https://api.orangex.com/api/v1/public/get_instruments?kind=spot";
const snapshotUrl = 'https://api.orangex.com/api/v1/public/get_order_book?depth=100&instrument_name=';

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson1 = await response.json(); 
var currencies = [];
var precision = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000,
    1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000];
var request = [];

// extract symbols from JSON returned information
for(let i = 0; i < myJson1['result'].length; ++i){
    if(myJson1['result'][i]['is_active']){
        currencies.push(myJson1['result'][i]['instrument_name'].replace('-SPOT', ''));
    }
}

// print metadata about pairs
async function Metadata(){
    myJson1['result'].forEach((item)=>{
        if(item['is_active']){
            let prec = 11;
            for(let i = 0; i < precision.length; ++i){
                if(item['tick_size'] * precision[i] >= 1){
                    prec = i
                    break;
                }
            }
            let pair_data = '@MD ' + item['show_name'] + ' spot ' + item['quote_currency'] + 
                ' ' + item['base_currency'] + ' ' + prec +  ' 1 1 0 0';
            console.log(pair_data);
        }
    })
    console.log('@MDEND')
}


//function to get current time in unix format
function getUnixTime(){
    return Math.floor(Date.now());
}


Number.prototype.noExponents = function() {
    var data = String(this).split(/[eE]/);
    if (data.length == 1) return data[0];
  
    var z = '',
      sign = this < 0 ? '-' : '',
      str = data[0].replace('.', ''),
      mag = Number(data[1]) + 1;
  
    if (mag < 0) {
      z = sign + '0.';
      while (mag++) z += '0';
      return z + str.replace(/^\-/, '');
    }
    mag -= str.length;
    while (mag--) z += '0';
    return str + z;
}

function formRequest(){
    currencies.forEach((pair)=>{
        request.push(`trades.${pair}.raw`);
        request.push(`book.${pair}.raw`);
    })
}

async function manageOrderbook(pair){
    const response1 = await fetch(snapshotUrl + pair);
    //extract JSON from the http response
    const myJson = await response1.json(); 
    if(myJson['result']['bids'] && myJson['result']['bids'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + myJson['result']['instrument_name'] + ' B ';
        var pq = '';
        for(let i = 0; i < myJson['result']['bids'].length; i++){
            pq += parseFloat(myJson['result']['bids'][i][1]).noExponents() + '@' + parseFloat(myJson['result']['bids'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }

    // check if asks array is not Null
    if(myJson['result']['asks'] && myJson['result']['asks'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + myJson['result']['instrument_name'] + ' S ';
        var pq = '';
        for(let i = 0; i < myJson['result']['asks'].length; i++){
            pq += parseFloat(myJson['result']['asks'][i][1]).noExponents() + '@' + parseFloat(myJson['result']['asks'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }
}


async function getTrades(message){
    message['params']['data'].forEach((trade)=>{
        var trade_output = '! ' + getUnixTime() + ' ' + trade['instrument_name'] + ' ' + 
        trade['direction'][0].toUpperCase() + ' ' + parseFloat(trade['price']).noExponents() + ' ' + parseFloat(trade['amount']).noExponents();
        console.log(trade_output);
    });
}


async function getOrders(message){
    // check if bids array is not Null
    if(message['params']['data']['bids'] && message['params']['data']['bids'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['params']['data']['instrument_name'] + ' B '
        var pq = '';
        for(let i = 0; i < message['params']['data']['bids'].length; i++){
            pq += parseFloat(message['params']['data']['bids'][i][2]).noExponents() + '@' + parseFloat(message['params']['data']['bids'][i][1]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
    }

    // check if asks array is not Null
    if(message['params']['data']['asks'] && message['params']['data']['asks'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['params']['data']['instrument_name'] + ' S '
        var pq = '';
        for(let i = 0; i < message['params']['data']['asks'].length; i++){
            pq += parseFloat(message['params']['data']['asks'][i][2]).noExponents() + '@' + parseFloat(message['params']['data']['asks'][i][1]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
    }
}


async function Connect(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        
        // create ping function to keep connection alive
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(
                    { 
                        "jsonrpc":"2.0",
                        "id": 1,
                        "method": "/public/ping",
                        "params":{}
                    }
                ));
              console.log('Ping request sent');
            }
          }, 5000);
        // subscribe to trades and orders for all instruments
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "jsonrpc" : "2.0",
                    "id" : 1,
                    "method" : "/public/subscribe",
                    "params" : {
                        "channels":request
                    }
                }
            ));
        })
        
        
            
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            
            if(dataJSON['params']['channel'].split('.')[0] === 'trades'){
                getTrades(dataJSON);
            }else if(dataJSON['params']['channel'].split('.')[0] === 'book'){
                getOrders(dataJSON);
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
                Connect();
                }, 1000);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}


Metadata();
formRequest();
for(let pair of currencies){
    manageOrderbook(pair);
    await new Promise((resolve) => setTimeout(resolve, 50));
}
Connect();

