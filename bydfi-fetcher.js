import WebSocket from 'ws';
import fetch from 'node-fetch';

// define the websocket and REST URLs
const wsUrl = 'wss://quote.bydfi.in/wsquote';
const restUrl = "https://www.bydfi.com/swap/public/common/exchangeInfo";
const restOrderbookBaseUrl = "https://quote.bydfi.in/mkpai/depth-v2?dType=0&size=100&businessType=";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson1 = await response.json(); 
var currencies = [];


// extract symbols from JSON returned information
for(let i = 0; i < myJson1['data'].length; ++i){
    currencies.push(myJson1['data'][i]['baseSymbol'] + '_' + myJson1['data'][i]['priceSymbol']);
}

// print metadata about pairs
async function Metadata(){
    myJson1['data'].forEach((item)=>{
        let pair_data = '@MD ' + item['baseSymbol'] + '_' + item['priceSymbol'] + ' spot ' + item['baseSymbol'] + 
            ' ' + item['priceSymbol'] + ' ' + item['basePrecision'] +  ' 1 1 0 0';
        console.log(pair_data);
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


async function getTrades(message){
    message = message['data'];
    var trade_output = '! ' + getUnixTime() + ' ' + message.split(',')[4] + ' ' + 
    (message.split(',')[0] === '1' ? 'B' : 'S') + ' ' + parseFloat(message.split(',')[1]).noExponents() + ' ' + parseFloat(message.split(',')[2]).noExponents();
    console.log(trade_output);
}


async function getOrders(message){
    // check if bids array is not Null
    if(message['bids'] && message['bids'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['s'] + ' B '
        var pq = '';
        for(let i = 0; i < message['bids'].length; i++){
            pq += parseFloat(message['bids'][i][1]).noExponents() + '@' + parseFloat(message['bids'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
    }

    // check if asks array is not Null
    if(message['asks'] && message['asks'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['s'] + ' S '
        var pq = '';
        for(let i = 0; i < message['asks'].length; i++){
            pq += parseFloat(message['asks'][i][1]).noExponents() + '@' + parseFloat(message['asks'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);
    }
}

async function manageOrderbook(pair){
    const response1 = await fetch(restOrderbookBaseUrl + pair);
    //extract JSON from the http response
    const myJson = await response1.json(); 
    if(myJson['data']['bids'] && myJson['data']['bids'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + pair + ' B ';
        var pq = '';
        for(let i = 0; i < myJson['data']['bids'].length; i++){
            pq += parseFloat(myJson['data']['bids'][i]['amount']).noExponents() + '@' + parseFloat(myJson['data']['bids'][i]['price']).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }

    // check if asks array is not Null
    if(myJson['data']['asks'] && myJson['data']['asks'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + pair + ' S ';
        var pq = '';
        for(let i = 0; i < myJson['data']['asks'].length; i++){
            pq += parseFloat(myJson['data']['asks'][i]['amount']).noExponents() + '@' + parseFloat(myJson['data']['asks'][i]['price']).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }
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
                    "cmid":"3000"
                }
              ));
              console.log('Ping request sent');
            }
          }, 3000);
        // subscribe to trades and orders for all instruments
        ws.send(JSON.stringify(
            {
                "cmid": "4001",
                "symbols": pair,
                "r": 1
            }
        ));
            
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if(dataJSON['data'][0] === '0' || dataJSON['data'][0] === '1' && dataJSON['data'].length > 3){
                getTrades(dataJSON);
            }else{
                dataJSON = JSON.parse(dataJSON['data']);
                if('bids' in dataJSON && 'asks' in dataJSON){
                    getOrders(dataJSON, true);
                }
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
                Connect(pair);
                }, 1000);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}



Metadata();
for(let pair of currencies){
    manageOrderbook(pair);
}

// Connect();
var connection = [];

for(let pair of currencies){
    connection.push(Connect(pair));
    await new Promise((resolve) => setTimeout(resolve, 1000));
}


