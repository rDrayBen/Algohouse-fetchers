import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';


// define the websocket and REST URLs
const wsUrl = 'wss://ws.coinstore.com/s/ws';
const restUrl = "https://api.coinstore.com/api/v1/market/tickers";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trade_req = [];
var order_req = [];


// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['instrumentId']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item)=>{
        if(item['symbol'].toUpperCase().includes('USDT')){
            let pair_data = '@MD ' + item['symbol'].toUpperCase() + ' spot ' + item['symbol'].toUpperCase().replace('USDT', '') 
            + ' USDT' + ' -1 1 1 0 0';
            console.log(pair_data);
        }
        
    });
    console.log('@MD MANDOXETH spot MANDOX ETH -1 1 1 0 0');
    console.log('@MD DUDEUSDC spot DUDE USDC -1 1 1 0 0');
    console.log('@MD ERTHBTC spot ERTH BTC -1 1 1 0 0');
    console.log('@MD JPYCUSDC spot JPYC USDC -1 1 1 0 0');
    console.log('@MD ANKRBNBBNB spot ANKRBNB BNB -1 1 1 0 0');
    console.log('@MD ANKRETHETH spot ANKRETH ETH -1 1 1 0 0');
    console.log('@MD ROYSYFLAG spot ROYSY FLAG -1 1 1 0 0');
    console.log('@MD P2PSETH spot P2PS ETH -1 1 1 0 0');
    console.log('@MDEND')
}

function FormReq(){
    currencies.forEach((number)=>{
        trade_req.push(`${number}@trade`);
        order_req.push(`${number}@depth@100`);
    });
    
}

// "4@trade"
// "4@snapshot_depth@20@0.01"
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
    var trade_output = '! ' + getUnixTime() + ' ' + message['symbol'] + ' ' + 
        message['takerSide'][0].toUpperCase() + ' ' + parseFloat(message['price']).noExponents() + 
        ' ' + parseFloat(message['volume']).noExponents();
    console.log(trade_output);
}


async function getOrders(message){
    
    // check if bids array is not Null
    if(message['b']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['symbol'] + ' B ';
        var pq = '';
        for(let i = 0; i < message['b'].length; i++){
            pq += parseFloat(message['b'][i][1]).noExponents() + '@' + parseFloat(message['b'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }

    // check if asks array is not Null
    if(message['a']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['symbol'] + ' S '
        var pq = '';
        for(let i = 0; i < message['a'].length; i++){
            pq += parseFloat(message['a'][i][1]).noExponents() + '@' + parseFloat(message['a'][i][0]).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }
}


async function ConnectTrades(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "op": "pong",
                    "epochMillis": getUnixTime()
                }
              ));
              console.log('Ping request sent');
            }
          }, 120000);
        ws.send(JSON.stringify(
            {
                "op":"SUB",
                "channel":trade_req,
                "id":1
            }
        ));
        
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (!('data' in dataJSON) && dataJSON['T'] === 'trade'){
                getTrades(dataJSON);
            }else if('data' in dataJSON){
                // skip trades history
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
            console.log(`Connection closed with code ${event.code} and reason ${event}`);
        } else {
            console.log('Connection trade lost');
            setTimeout(async function() {
                ConnectTrades();
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}

async function ConnectOrders(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        setInterval(function() {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(
                {
                    "op": "pong",
                    "epochMillis": getUnixTime()
                }
              ));
              console.log('Ping request sent');
            }
        }, 120000);
        
        ws.send(JSON.stringify(
            {
                "op":"SUB",
                "channel":order_req,
                "id":1
            }
        ));

    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if(dataJSON['T'] === 'depth' && 'a' in dataJSON && 'b' in dataJSON){
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
            console.log(`Connection closed with code ${event.code} and reason ${event}`);
        } else {
            console.log('Connection order lost');
            setTimeout(async function() {
                ConnectOrders();
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
    };
}

Metadata();
FormReq();
ConnectTrades();
if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    ConnectOrders();
}


