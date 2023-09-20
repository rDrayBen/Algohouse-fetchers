import WebSocket from 'ws';
import fetch from 'node-fetch';
import fs from "fs";

// define the websocket and REST URLs
const wsUrl = 'wss://nominex.io/api/ws/v1';
const restUrl = "https://nominex.io/api/rest/v1/pairs";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var precision = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000,
    1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000];


// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    if(myJson[i]['active']){
        currencies.push(myJson[i]['name']);
    }
}


// print metadata about pairs
async function Metadata(){
    myJson.forEach((item)=>{
        if(item['active']){
            let prec = 11;
            for(let i = 0; i < precision.length; ++i){
                if(item['baseStep'] * precision[i] >= 1){
                    prec = i
                    break;
                }
            }
            let pair_data = '@MD ' + item['name'] + ' spot ' + item['name'].split('/')[0] + ' ' + item['name'].split('/')[1] + ' ' 
                + prec + ' 1 1 0 0';
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


async function getTrades(message){
    let pair_name = message['endpoint'].replace('/system/trades@50/', '');
    var trade_output = '! ' + getUnixTime() + ' ' + pair_name + ' ' + 
    message['payload']['side'][0].toUpperCase() + ' ' + parseFloat(message['payload']['price']).noExponents() + 
    ' ' + parseFloat(message['payload']['amount']).noExponents();
    console.log(trade_output);
}


async function getOrders(message, update){
    let pair_name = message['endpoint'].replace('/orderBook/', '');
    pair_name = pair_name.replace('/A0/100', '');
    
    var order_answer = '$ ' + getUnixTime() + ' ' + pair_name + ' B '
    var pq = '';
    if(update){
        for(let i = 0; i < message['payload'].length; i++){
            if(message['payload'][i]['side'] === 'BUY'){
                pq += parseFloat(message['payload'][i]['amount']).noExponents() + '@' + parseFloat(message['payload'][i]['price']).noExponents() + '|';
            }
        }
        pq = pq.slice(0, -1);
    }else{
        for(let i = 0; i < message['snapshot'].length; i++){
            if(message['snapshot'][i]['side'] === 'BUY'){
                pq += parseFloat(message['snapshot'][i]['amount']).noExponents() + '@' + parseFloat(message['snapshot'][i]['price']).noExponents() + '|';
            }
        }
        pq = pq.slice(0, -1);
    }
    
    // check if the input data is full order book or just update
    if(pq !== ''){
        if (update){
            console.log(order_answer + pq)
        }
        else{
            console.log(order_answer + pq + ' R')
        }
    }
    

    var order_answer = '$ ' + getUnixTime() + ' ' + pair_name + ' S '
    var pq = '';
    if(update){
        for(let i = 0; i < message['payload'].length; i++){
            if(message['payload'][i]['side'] === 'SELL'){
                pq += parseFloat(message['payload'][i]['amount']).noExponents() + '@' + parseFloat(message['payload'][i]['price']).noExponents() + '|';
            }
        }
        pq = pq.slice(0, -1);
    }else{
        for(let i = 0; i < message['snapshot'].length; i++){
            if(message['snapshot'][i]['side'] === 'SELL'){
                pq += parseFloat(message['snapshot'][i]['amount']).noExponents() + '@' + parseFloat(message['snapshot'][i]['price']).noExponents() + '|';
            }
        }
        pq = pq.slice(0, -1);
    }
    
    // check if the input data is full order book or just update
    if(pq !== ''){
        if (update){
            console.log(order_answer + pq)
        }
        else{
            console.log(order_answer + pq + ' R')
        }
    }
        
    
}


async function ConnectTrades(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "event":"subscribe",
                    "endpoint":`/system/trades@50/${pair.split('/')[0]}/${pair.split('/')[1]}`
                }
            ));
        })
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if ('payload' in dataJSON){
                getTrades(dataJSON);
            }else if('snapshot' in dataJSON){
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
            console.log('Connection lost');
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
        currencies.forEach((pair)=>{
            ws.send(JSON.stringify(
                {
                    "event":"subscribe",
                    "endpoint":`/orderBook/${pair.split('/')[0]}/${pair.split('/')[1]}/A0/100`
                }
            ));
        })
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if ('payload' in dataJSON){
                getOrders(dataJSON, true);
            }else if('snapshot' in dataJSON){
                getOrders(dataJSON, false);
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
            console.log('Connection lost');
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
ConnectTrades();
ConnectOrders();
