import WebSocket from 'ws';
import fetch from 'node-fetch';

// define the websocket and REST URLs
const wsUrl = 'wss://api.vitex.net/v2/ws';
const restUrl = "https://api.vitex.net/api/v2/markets";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];

// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    if(myJson['data'][i]['s'] !== 0){
        currencies.push(myJson['data'][i]['symbol']);
    }
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item)=>{
        let pair_data = '@MD ' + item['tradeTokenSymbol'].split('-')[0] + '-' + item['quoteTokenSymbol'].split('-')[0] + ' spot ' + 
            item['tradeTokenSymbol'].split('-')[0] + ' ' + item['quoteTokenSymbol'].split('-')[0] + ' ' + item['pricePrecision'] +  ' 1 1 0 0';
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
    let pair_name = message['topic'].replace('market.', '').replace('.trade', '');
    pair_name = pair_name.split('_')[0].split('-')[0] + '-' + pair_name.split('_')[1].split('-')[0];
    message['data'].forEach((trade)=>{
        var trade_output = '! ' + getUnixTime() + ' ' + pair_name + ' ' + 
            (trade['side'] === 0 ? 'B' : 'S') + ' ' + parseFloat(trade['p']).noExponents() + ' ' + parseFloat(trade['q']).noExponents();
        console.log(trade_output);
    })
}


async function getOrders(message){
    let pair_name = message['topic'].replace('market.', '').replace('.depth', '');
    pair_name = pair_name.split('_')[0].split('-')[0] + '-' + pair_name.split('_')[1].split('-')[0];
    // check if bids array is not Null
    if(message['data']['bids'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + pair_name + ' B '
        var pq = '';
        for(let i = 0; i < message['data']['bids'].length; i++){
            pq += parseFloat(message['data']['bids'][i][1]).noExponents() + '@' + parseFloat(message['data']['bids'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');  
    }

    // check if asks array is not Null
    if(message['data']['asks'].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + pair_name + ' S '
        var pq = '';
        for(let i = 0; i < message['data']['asks'].length; i++){
            pq += parseFloat(message['data']['asks'][i][1]).noExponents() + '@' + parseFloat(message['data']['asks'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
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
                    "command":"ping"
                }
              ));
              console.log('Ping request sent');
            }
          }, 10000);
        // subscribe to trades and orders for all instruments
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "command": "sub",
                    "params": [`market.${pair}.trade`]
                }
            ));
            ws.send(JSON.stringify(
                {
                    "command": "sub", 
                    "params": [`market.${pair}.depth`]
                }
            ));
            
        });
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            // console.log(dataJSON);
            if (dataJSON.event === 'push' && dataJSON['topic'].includes('trade')) {
                getTrades(dataJSON);
            }
            else if(dataJSON.event === 'push' && dataJSON['topic'].includes('depth')){
                getOrders(dataJSON);
            }
            else {
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
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}

Metadata();
Connect();
