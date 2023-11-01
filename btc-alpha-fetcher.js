import WebSocket from 'ws';
import fetch from 'node-fetch';
import getenv from 'getenv';


// define the websocket and REST URLs
const wsUrl = 'wss://btc-alpha.com/alp-ws';
const restUrl = "https://btc-alpha.com/api/v1/pairs/";


const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var orderbooks = {};
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson.length; ++i){
    currencies.push(myJson[i]['name']);
}


// print metadata about pairs
async function Metadata(){
    myJson.forEach((item)=>{
        trades_count_5min[item['name']] = 0;
        orders_count_5min[item['name']] = 0;
        let pair_data = '@MD ' + item['name'] + ' spot ' + item['currency1'] + ' ' + item['currency2'] + ' ' 
        + item['price_precision'] + ' 1 1 0 0';
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
    trades_count_5min[message[3]] += 1;
    var trade_output = '! ' + getUnixTime() + ' ' + 
    message[3] + ' ' + 
    message[6][0].toUpperCase() + ' ' + message[5] + ' ' + message[4];
    console.log(trade_output);
    // example of message ["t",1683703622,274430761,"TRX_USDT","3921.43400516","0.06930900","sell"]
}


// func to print orderbooks and deltas
async function getOrders(message){
    orders_count_5min[message[2]] += message[3].length + message[4].length;
    // check if bids array is not Null
    if(message[3].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message[2] + ' B '
        var pq = '';
        message[3].forEach((element)=>{
            // if pair [price, quantity] is like this ["27715.26000000", "-0.17600000"]
            if(element[1][0] === '-'){
                // check whether is incoming price value is in orderbook
                if(element[0] in orderbooks[message[2]] && parseFloat(orderbooks[message[2]][element[0]]) > 0){
                    if(orderbooks[message[2]][element[0]] - parseFloat(element[1].slice(1)) >= 0){
                        // change quantity in orderbook
                        orderbooks[message[2]][element[0]] = parseFloat(orderbooks[message[2]][element[0]]) - parseFloat(element[1].slice(1));
                        // add pair to delta output
                        pq += parseFloat(orderbooks[message[2]][element[0]]).noExponents() + '@' + element[0] + '|';
                        // check if now quantity is 0, and delete pair [price] = quantity from orderbook
                        if(parseFloat(orderbooks[message[2]][element[0]]) === 0){
                            delete orderbooks[message[2]][element[0]];
                        }
                    }
                }
                // if pair [price, quantity] is like this ["27879.20000000", "0.13600000"]
            }else{
                // check whether is incoming price value is in orderbook
                if(element[0] in orderbooks[message[2]] && parseFloat(orderbooks[message[2]][element[0]]) > 0){
                    // change quantity in orderbook
                    orderbooks[message[2]][element[0]] = parseFloat(orderbooks[message[2]][element[0]]) + parseFloat(element[1]);
                    // add pair to delta output
                    pq += parseFloat(orderbooks[message[2]][element[0]]).noExponents() + '@' + element[0] + '|';
                }else{
                    orderbooks[message[2]][element[0]] = parseFloat(element[1]).noExponents();
                    pq += parseFloat(orderbooks[message[2]][element[0]]).noExponents() + '@' + element[0] + '|';
                }
            }
            
        })
        pq = pq.slice(0, -1);
        if (pq !== ''){
            console.log(order_answer + pq);
        }
    }

    // check if asks array is not Null
    if(message[4].length > 0){
        var order_answer = '$ ' + getUnixTime() + ' ' + message[2] + ' S '
        var pq = '';
        message[4].forEach((element)=>{
            // if pair [price, quantity] is like this ["27715.26000000", "-0.17600000"]
            if(element[1][0] === '-'){
                // check whether is incoming price value is in orderbook
                if(element[0] in orderbooks[message[2]] && parseFloat(orderbooks[message[2]][element[0]]) > 0){
                    if(orderbooks[message[2]][element[0]] - parseFloat(element[1].slice(1)) >= 0){
                        // change quantity in orderbook
                        orderbooks[message[2]][element[0]] = parseFloat(orderbooks[message[2]][element[0]]) - parseFloat(element[1].slice(1));
                        // add pair to delta output
                        pq += parseFloat(orderbooks[message[2]][element[0]]).noExponents() + '@' + element[0] + '|';
                        // check if now quantity is 0, and delete pair [price] = quantity from orderbook
                        if(parseFloat(orderbooks[message[2]][element[0]]) === 0){
                            delete orderbooks[message[2]][element[0]];
                        }
                    }
                }
            // if pair [price, quantity] is like this ["27879.20000000", "0.13600000"]
            }else{
                try{
                    // check whether is incoming price value is in orderbook
                    if(element[0] in orderbooks[message[2]] && parseFloat(orderbooks[message[2]][element[0]]) > 0){
                        // change quantity in orderbook
                        orderbooks[message[2]][element[0]] = parseFloat(orderbooks[message[2]][element[0]]) + parseFloat(element[1]);
                        // add pair to delta output
                        pq += parseFloat(orderbooks[message[2]][element[0]]).noExponents() + '@' + element[0] + '|';
                    }else{
                        orderbooks[message[2]][element[0]] = parseFloat(element[1]).noExponents();
                        pq += parseFloat(orderbooks[message[2]][element[0]]).noExponents() + '@' + element[0] + '|';
                    }
                }catch(e){

                }
                
            }
            
        })
        pq = pq.slice(0, -1);
        if (pq !== ''){
            console.log(order_answer + pq);
        }
    }
}

  

async function Connect(){
    // create a new websocket instance
    var wsConnection = new WebSocket(wsUrl);
    
    // call this func when first opening connection
    wsConnection.onopen = function(e) {
        // create ping function to keep connection alive
        wsConnection.ping();
        
        // sub for trades for all currency pairs in 1 message
        wsConnection.send(JSON.stringify(
                ["subscribe", "trade.*"]
        ))  
    
    }

    // func to handle input messages
    wsConnection.onmessage = function(event) {
        var dataJSON = JSON.parse(event.data);
        if(dataJSON[0] === 't'){
            getTrades(dataJSON);
        }else if(dataJSON[0] === 'd'){
            getOrders(dataJSON, true);
        }else{
            console.log(dataJSON);
        }
    };


    // func to handle closing connection
    wsConnection.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection 1 closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection 1 lost');
            setTimeout(async function() {
                Connect();
                }, 500);
        }
    };

    // func to handle errors
    wsConnection.onerror = function(error) {
        
    };
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

async function manageOrderBook(){
    while(true){
        currencies.forEach((pair)=>{
            subForSnapshot(pair);
        })
        await new Promise((resolve) => setTimeout(resolve, 1800000)); // Delay 30 min between requesting orderbooks
    }
    
}

async function subForSnapshot(curr){
    //delete previous orderbook for this trading pair
    delete orderbooks[curr];
    // url to get orderbook for each tradong pair
    let url = `https://btc-alpha.com/api/web/finance/exchange?pair=${curr}&depth=true`;
    const response = await fetch(url);

    // convert response to JSON
    try{
        const responseJSON = await response.json();
        try{
            orders_count_5min[responseJSON['depth']['symbol']] += responseJSON['depth']['bids'].length + responseJSON['depth']['asks'].length;
            // creating orderbook for certain trading pair in general orderbook
            orderbooks[responseJSON['depth']['symbol']] = {};
            // check whether bids array is not NULL
            if(responseJSON['depth']['bids'].length > 0){
                var order_answer = '$ ' + getUnixTime() + ' ' + responseJSON['depth']['symbol'] + ' B ';
                var pq = '';
                responseJSON['depth']['bids'].forEach((element)=>{
                    var decimals_len;
                    var last_dec = '';
                    var dot = '';
                    if(String(parseFloat(element[0]).noExponents()).includes('.')){
                        decimals_len = 8 - String(parseFloat(element[0]).noExponents()).split('.')[1].length;
                    }else{
                        decimals_len = 8;
                        dot = '.';
                    }
                    for(let i = 0; i < decimals_len; i++){
                        last_dec += '0';
                    }
                    // add new element to orderbook
                    orderbooks[responseJSON['depth']['symbol']][String(parseFloat(element[0]).noExponents()) + dot + last_dec] = parseFloat(element[1]).noExponents();
                    // add this pair to orderbook output
                    pq += parseFloat(element[1]).noExponents() + "@" + String(parseFloat(element[0]).noExponents()) + dot + last_dec + '|';
                });
                pq = pq.slice(0, -1);
                console.log(order_answer + pq + ' R');
            }
            // check whether asks array is not NULL
            if(responseJSON['depth']['asks'].length > 0){
                var order_answer = '$ ' + getUnixTime() + ' ' + responseJSON['depth']['symbol'] + ' S ';
                var pq = ''
                responseJSON['depth']['asks'].forEach((element)=>{
                    var decimals_len;
                    var last_dec = '';
                    var dot = '';
                    if(String(parseFloat(element[0]).noExponents()).includes('.')){
                        decimals_len = 8 - String(parseFloat(element[0]).noExponents()).split('.')[1].length;
                    }else{
                        decimals_len = 8;
                        dot = '.';
                    }
                    for(let i = 0; i < decimals_len; i++){
                        last_dec += '0';
                    }
                    // add new element to orderbook
                    orderbooks[responseJSON['depth']['symbol']][String(parseFloat(element[0]).noExponents()) + dot + last_dec] = parseFloat(element[1]).noExponents();
                    // add this pair to orderbook output
                    pq += parseFloat(element[1]).noExponents() + "@" + String(parseFloat(element[0]).noExponents()) + dot + last_dec + '|';
                });
                pq = pq.slice(0, -1);
                console.log(order_answer + pq + ' R');
            }
        }catch(e){
            // if trading pair listed in metadata doesn`t have orderbook this catches an error
        }
    }catch(e){
        
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
}
  

async function ConnectDepth1(){
    var wsDepth = new WebSocket(wsUrl);
    // call this func when first opening connection
    wsDepth.onopen = function(e) {
        // create ping function to keep connection alive
        wsDepth.ping();
        for(let i = 0; i < 20; i++){ 
            wsDepth.send(JSON.stringify(
                ["subscribe", `diff.${currencies[i]}`]
            ));
            sleep(10);
        } 
    }
    // func to handle input messages
    wsDepth.onmessage = function(event) {
        // parse input data to JSON format
        var dataJSON = JSON.parse(event.data);
        if(dataJSON[0] === 'd'){
            getOrders(dataJSON);
        }else{
            console.log(dataJSON);
        }
    };


    // func to handle closing connection
    wsDepth.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection depth closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection depth lost with event', event);
            setTimeout(async function() {
                ConnectDepth1();
                }, 500);
        }
    };

    // func to handle errors
    wsDepth.onerror = function(error) {
        
    };

}

async function ConnectDepth2(){
    var wsDepth = new WebSocket(wsUrl);
    // call this func when first opening connection
    wsDepth.onopen = function(e) {
        // create ping function to keep connection alive
        wsDepth.ping();
        for(let i = 20; i < currencies.length; i++){   
            wsDepth.send(JSON.stringify(
                ["subscribe", `diff.${currencies[i]}`]
            )) 
            sleep(10);
        }
              
    }
    // func to handle input messages
    wsDepth.onmessage = function(event) {
        // parse input data to JSON format
        var dataJSON = JSON.parse(event.data);
        if(dataJSON[0] === 'd'){
            getOrders(dataJSON);
        }else{
            console.log(dataJSON);
        }
    };


    // func to handle closing connection
    wsDepth.onclose = function(event) {
        if (event.wasClean) {
            console.log(`Connection depth closed with code ${event.code} and reason ${event.reason}`);
        } else {
            console.log('Connection depth lost with event', event);
            setTimeout(async function() {
                ConnectDepth2();
                }, 500);
        }
    };

    // func to handle errors
    wsDepth.onerror = function(error) {
        
    };

}


Metadata();
stats();
setInterval(stats, 300000);
if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    manageOrderBook();
}

Connect();
if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
    ConnectDepth1();
    ConnectDepth2();
}



// as exchange has 38 spot pairs, out of which 37 are working, each websocket connection can handle up to 20 subs. 
// so in order to sub for all trading pairs without creating a websocket connection for each trading pair 
// I created 2 separate websocket connections which handle 1-19 and 20-38 index of pairs respectfully
