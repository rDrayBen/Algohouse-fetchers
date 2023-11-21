import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';

// define the websocket and REST URLs
const wsUrl = 'wss://ws.coinfalcon.com/';
const restUrl = "https://coinfalcon.com/advanced-view/BTC-EUR/initial";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};

// extract symbols from JSON returned information
for(let i = 0; i < myJson['markets'].length; ++i){
    currencies.push(myJson['markets'][i]['name']);
}


// print metadata about pairs
async function Metadata(){
    myJson['markets'].forEach((item)=>{
        trades_count_5min[item['name']] = 0;
        orders_count_5min[item['name']] = 0;
        let pair_data = '@MD ' + item['name'] + ' spot ' + 
            item['name'].split('-')[0] + ' ' + item['name'].split('-')[1] + ' ' + item['price_precision'] +  ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}


//function to get current time in unix format
function getUnixTime(){
    return Math.floor(Date.now());
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
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
    let pair_name = JSON.parse(message['identifier']);
    pair_name = pair_name['market'];
    trades_count_5min[pair_name] += 1;
    var trade_output = '! ' + getUnixTime() + ' ' + pair_name + ' ' + 
        message.message.trade['side'][0].toUpperCase() + ' ' + parseFloat(message.message.trade['price']).noExponents() + ' ' + parseFloat(message.message.trade['size']).noExponents();
    console.log(trade_output);
}


async function getOrders(message, update){
    let pair_name = JSON.parse(message['identifier']);
    pair_name = pair_name['market'];
    if(update){
        orders_count_5min[pair_name] += message['message']['update']['value'].length;
        var order_answer = '$ ' + getUnixTime() + ' ' + pair_name + (message['message']['update']['key'] === 'asks' ? ' S ' : ' B ');
        var pq = '';
        for(let i = 0; i < message['message']['update']['value'].length; i++){
            pq += parseFloat(message['message']['update']['value'][i]['size']).noExponents() + '@' + parseFloat(message['message']['update']['value'][i]['price']).noExponents() + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq);  
    }else{
        // check if bids array is not Null
        if(message['message']['init']['bids'].length > 0){
            orders_count_5min[pair_name] += message['message']['init']['bids'].length;
            var order_answer = '$ ' + getUnixTime() + ' ' + pair_name + ' B '
            var pq = '';
            for(let i = 0; i < message['message']['init']['bids'].length; i++){
                pq += parseFloat(message['message']['init']['bids'][i]['size']).noExponents() + '@' + parseFloat(message['message']['init']['bids'][i]['price']).noExponents() + '|';
            }
            pq = pq.slice(0, -1);
            console.log(order_answer + pq + ' R');  
        }

        // check if asks array is not Null
        if(message['message']['init']['asks'].length > 0){
            orders_count_5min[pair_name] += message['message']['init']['asks'].length;
            var order_answer = '$ ' + getUnixTime() + ' ' + pair_name + ' S '
            var pq = '';
            for(let i = 0; i < message['message']['init']['asks'].length; i++){
                pq += parseFloat(message['message']['init']['asks'][i]['size']).noExponents() + '@' + parseFloat(message['message']['init']['asks'][i]['price']).noExponents() + '|';
            }
            pq = pq.slice(0, -1);
            console.log(order_answer + pq + ' R');
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


async function Connect(){
    // create a new websocket instance
    var ws = new WebSocket(wsUrl);
    ws.onopen = function(e) {
        // subscribe to trades and orders for all instruments
        currencies.forEach((pair)=>{
            if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                ws.send(JSON.stringify(
                    {
                        "command": "subscribe",
                        "identifier": `{\"channel\":\"OrderbookChannel\",\"market\":\"${pair}\"}`
                    }
                ));
            }
            ws.send(JSON.stringify(
                {
                    "command": "subscribe",
                    "identifier": `{\"channel\":\"TradesChannel\",\"market\":\"${pair}\"}`
                }
            ));
            
        });
    };


    // func to handle input messages
    ws.onmessage = function(event) {
        try{
            // parse input data to JSON format
            let dataJSON = JSON.parse(event.data);
            if (dataJSON['identifier'].includes('TradesChannel') && 'init' in dataJSON['message']) {
                // skip trades history
            }
            else if (dataJSON['identifier'].includes('TradesChannel') && 'update' in dataJSON['message']) {
                getTrades(dataJSON);
            }
            else if (dataJSON['identifier'].includes('OrderbookChannel') && 'init' in dataJSON['message']) {
                getOrders(dataJSON, false);
            }
            else if (dataJSON['identifier'].includes('OrderbookChannel') && 'update' in dataJSON['message']) {
                getOrders(dataJSON, true);
            }
            else {
                console.log(dataJSON);
            }
        }catch(e){
            // skip confirmation messages cause they can`t be parsed into JSON format without an error
            (async () => {
                await sleep(1000); // Sleep for 1000 milliseconds (1 second) 
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
                await Connect();
                }, 500);
        }
    };

    // func to handle errors
    ws.onerror = function(error) {
        console.log(`Error ${error} occurred`);
        (async () => {
            await sleep(1000); // Sleep for 1000 milliseconds (1 second) 
          })();
    };
}

Metadata();
setTimeout(stats, parseFloat(5 - ((Date.now() / 60000) % 5)) * 60000);
await Connect();
