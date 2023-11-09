import WebSocket from 'ws';
import fetch from 'node-fetch';
import zlib from 'zlib';
import getenv from 'getenv';

// define the websocket and REST URLs
const wsUrl = 'wss://ws.dcoinpro.com/kline-api/ws';
const restUrl = "https://openapi.dcoin.com/open/api/common/symbols";


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};



// extract symbols from JSON returned information
for(let i = 0; i < myJson['data'].length; ++i){
    currencies.push(myJson['data'][i]['symbol']);
}


// print metadata about pairs
async function Metadata(){
    myJson['data'].forEach((item, index)=>{
        trades_count_5min[item['symbol'].toUpperCase()] = 0;
        orders_count_5min[item['symbol'].toUpperCase()] = 0;
        let pair_data = '@MD ' + item['symbol'].toUpperCase() + ' spot ' + item['count_coin'].toUpperCase() + ' ' 
        + item['base_coin'].toUpperCase() + ' ' 
        + (item['price_precision']*-1) + ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


//function to get current time in unix format
function getUnixTime(){
    return Math.floor(Date.now());
}


// func to print trades
async function getTrades(message){
    message['tick']['data'].forEach((item)=>{
        let pair_name = message['channel'];
        pair_name = pair_name.replace('market_', '');
        pair_name = pair_name.replace('_trade_ticker', '');

        trades_count_5min[pair_name.toUpperCase()] += 1;

        var trade_output = '! ' + getUnixTime() + ' ' + 
        pair_name.toUpperCase() + ' ' + 
        item['side'][0] + ' ' + item['price'] + ' ' + item['vol'];
        console.log(trade_output);
    });
}


// func to print orderbooks and deltas
async function getOrders(message, update){
    let pair_name = message['channel'];
    pair_name = pair_name.replace('market_', '');
    pair_name = pair_name.replace('_depth_step', '');
    pair_name = pair_name.slice(0, -1);
    // check if bids array is not Null
    if(message['tick']['buys'].length > 0){
        orders_count_5min[pair_name.toUpperCase()] += message['tick']['buys'].length;
        var order_answer = '$ ' + getUnixTime() + ' ' + pair_name.toUpperCase() + ' B '
        var pq = '';
        for(let i = 0; i < message['tick']['buys'].length; i++){
            pq += message['tick']['buys'][i][1] + '@' + message['tick']['buys'][i][0] + '|';
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
    if(message['tick']['asks'].length > 0){
        orders_count_5min[pair_name.toUpperCase()] += message['tick']['asks'].length;
        var order_answer = '$ ' + getUnixTime() + ' ' + pair_name.toUpperCase() + ' S '
        var pq = '';
        for(let i = 0; i < message['tick']['asks'].length; i++){
            pq += message['tick']['asks'][i][1] + '@' + message['tick']['asks'][i][0] + '|';
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

async function stats(){
    var stat_line = '# LOG:CAT=trades_stats:MSG= ';

    for(var key in trades_count_5min){
        if(trades_count_5min[key] !== 0){
            stat_line += `${key}:${trades_count_5min[key]} `;
        }
        trades_count_5min[key] = 0;
    }
    if (stat_line !== '# LOG:CAT=trades_stats:MSG= '){
        console.log(stat_line);
    }

    stat_line = '# LOG:CAT=orderbook_stats:MSG= ';

    for(var key in orders_count_5min){
        if(orders_count_5min[key] !== 0){
            stat_line += `${key}:${orders_count_5min[key]} `;
        }
        orders_count_5min[key] = 0;
    }
    if (stat_line !== '# LOG:CAT=orderbook_stats:MSG= '){
        console.log(stat_line);
    }
    setTimeout(stats, 300000);
}


function Connect1(){
    var ws1 = new WebSocket(wsUrl);
    // call this func when first opening connection
    ws1.onopen = function(e) {
        // create ping function to keep connection alive
        ws1.ping();
        currencies.forEach((item)=>{
            // sub for trades
            ws1.send(JSON.stringify(
                {
                    "event":"sub",
                    "params":{
                        "channel":`market_${item}_trade_ticker`,
                        "cb_id":""
                    }
                }
            ))
            if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                // sub for snapshot
                ws1.send(JSON.stringify(
                    {
                        "event":"sub",
                        "params":{
                            "channel":`market_${item}_depth_step0`,
                            "cb_id":"",
                            "asks":150,
                            "bids":150
                        }
                    }
                ))
                // sub for delta
                ws1.send(JSON.stringify(
                    {
                        "event":"sub",
                        "params":{
                            "channel":`market_${item}_depth_step2`,
                            "cb_id":"",
                            "asks":10,
                            "bids":10
                        }
                    }
                    
                ))
            }
            
        })
        
            
    };


    // func to handle input messages
    ws1.onmessage = function(event) {
        const compressedData = Buffer.from(event.data, 'base64');
        zlib.gunzip(compressedData, (err, uncompressedData) => {
            // console.log(uncompressedData);
            try{
                //uncompressedData = uncompressedData.trim();
                var dataJSON = JSON.parse(uncompressedData);
                // console.log(dataJSON);
                if (dataJSON['channel'].slice(-6) === 'ticker' && dataJSON['tick']['data'].length <= 3){
                    getTrades(dataJSON);
                }else if (dataJSON['channel'].slice(-6) === 'ticker' && dataJSON['tick']['data'].length > 3){
                    // to skip trades history
                }else if(dataJSON['channel'].slice(-5) === 'step0' && 'tick' in dataJSON){
                    getOrders(dataJSON, false); 
                }else if(dataJSON['channel'].slice(-5) === 'step2' && 'tick' in dataJSON){
                    getOrders(dataJSON, true); 
                }else{
                    console.log(dataJSON);
                }
            }catch(e){

            }   
        });  
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


// call metadata to execute
Metadata();
setTimeout(stats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);

Connect1();

