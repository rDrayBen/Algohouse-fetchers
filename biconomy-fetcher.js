import WebSocket from 'ws';
import fetch from 'node-fetch';


// define the websocket and REST URLs
const wsUrl = 'wss://www.biconomy.com/ws';
const restUrl = "https://www.biconomy.com/api/v1/exchangeInfo";


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];


// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    if(myJson[i]['status'] === 'trading'){
        currencies.push(myJson[i]['symbol']);
    }
}


// print metadata about pairs
async function Metadata(){
    myJson.forEach((item, index)=>{
        if(item['status'] === 'trading'){
            let pair_data = '@MD ' + item['symbol'] + ' spot ' + item['baseAsset'] + ' ' + item['quoteAsset'] + ' ' 
            + item['quoteAssetPrecision'] + ' 1 1 0 0';
            console.log(pair_data);
        }
    })
    console.log('@MDEND')
}

//function to get current time in unix format
function getUnixTime(){
    return Math.floor(Date.now() / 1000);
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


// func to print trades
async function getTrades(message){
    message['params'][1].forEach((item)=>{
        var trade_output = '! ' + getUnixTime() + ' ' + 
        message['params'][0] + ' ' + 
        item['type'][0].toUpperCase() + ' ' + item['price'] + ' ' + item['amount'];
        console.log(trade_output);
    });
}


// func to print orderbooks and deltas
async function getOrders(message, update){
    // check if bids array is not Null
    if(message['params'][1]['bids']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['params'][2] + ' B '
        var pq = '';
        for(let i = 0; i < message['params'][1]['bids'].length; i++){
            pq += (parseInt(message['params'][1]['bids'][i][1])).noExponents() + '@' + (parseInt(message['params'][1]['bids'][i][0])).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        // check if the input data is full order book or just update
        if (update){
            console.log(order_answer + pq)
        }
        else{
            console.log(order_answer + pq + ' R')
        }
    }

    // check if asks array is not Null
    if(message['params'][1]['asks']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['params'][2] + ' S '
        var pq = '';
        for(let i = 0; i < message['params'][1]['asks'].length; i++){
            pq += (parseInt(message['params'][1]['asks'][i][1])).noExponents() + '@' + (parseInt(message['params'][1]['asks'][i][0])).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        // check if the input data is full order book or just update
        if (update){
            console.log(order_answer + pq)
        }
        else{
            console.log(order_answer + pq + ' R')
        }
    }
}


function Connect1(){
    var ws1 = new WebSocket(wsUrl);
    
    
    // call this func when first opening connection
    ws1.onopen = function(e) {
        // create ping function to keep connection alive
        ws1.ping();
        // sub for trades
        ws1.send(JSON.stringify(
            {
                "method":"deals.subscribe",
                "params":currencies,
                "id":2070
            }
        ))
    };


    // func to handle input messages
    ws1.onmessage = function(event) {
        try{
            var dataJSON = JSON.parse(event.data);
            // console.log(dataJSON);
            if (dataJSON['method'] === 'deals.update' && dataJSON['params'][1].length < 5){
                getTrades(dataJSON);
            }else if (dataJSON['method'] === 'deals.update' && dataJSON['params'][1].length > 5){
                // skip trades history
            }else{
                console.log(dataJSON);
            }        
        }catch(e){
            // possible errors while parsing data to json format
        }
    };


    // func to handle closing connection
    ws1.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 1 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 1 lost');
            setTimeout(function() {
                Connect1();
                }, 500);
        }
    };

    // func to handle errors
    ws1.onerror = function(error) {
        console.log(`Error ${error} occurred in ws1`);
    };
}

var wsArr = [];
for(let i = 0; i < currencies.length; i++){
    var wsTemp = new WebSocket(wsUrl);
    wsArr.push(wsTemp);
}
  

async function Connect2(){
    // create a new websocket instance
    
    
    for(let i = 0; i < wsArr.length; i++){
        // call this func when first opening connection
        wsArr[i].onopen = function(e) {
            // create ping function to keep connection alive
            wsArr[i].ping();
            
            // sub for orders
            wsArr[i].send(JSON.stringify(
                {
                    "method":"depth.subscribe",
                    "params":[currencies[i],100,"0.00000001"],
                    "id":2066
                }
            ))        
        };


        // func to handle input messages
        wsArr[i].onmessage = function(event) {
            var dataJSON;
            try{
                dataJSON = JSON.parse(event.data);
                //console.log(dataJSON);
                if (dataJSON['method'] === 'depth.update' && dataJSON['params'][0] === true){
                    getOrders(dataJSON, false);
                }else if (dataJSON['method'] === 'depth.update' && dataJSON['params'][0] === false){
                    getOrders(dataJSON, true);
                }else{
                    console.log(dataJSON);
                }        
            }catch(e) {
                // console.log(e);
                // error may occurr cause some part of incoming data can`t be properly parsed in json format due to inapropriate symbols
                // error only occurrs in messages that confirming subs
                // error caused here is exchanges fault
            }
        };


        // func to handle closing connection
        wsArr[i].onclose = function(event) {
            if (event.wasClean) {
                console.log(`Connection 2 closed with code ${event.code} and reason ${event.reason}`);
            } else {
                console.log('Connection 2 lost');
                setTimeout(async function() {
                    Connect2();
                    }, 500);
            }
        };

        // func to handle errors
        wsArr[i].onerror = function(error) {
            console.log(`Error ${error} occurred in ws2`);
        };
    }
    
}

// call metadata to execute
Metadata();

Connect1();
Connect2();
