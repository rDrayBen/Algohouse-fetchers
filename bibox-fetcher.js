import WebSocket from 'ws';
import fetch from 'node-fetch';
import zlib from 'zlib';
import getenv from 'getenv';

// define the websocket and REST URLs
// const wsUrl = 'wss://npush.bibox360.com';
const restUrl = "https://api.bibox.com/v3/mdata/pairList";

const response = await fetch(restUrl);
//extract JSON from the http response
const myJson = await response.json(); 
var currencies = [];
var trades_count_5min = {};
var orders_count_5min = {};


// extract symbols from JSON returned information
for(let i = 0; i < myJson['result'].length; ++i){
    currencies.push(myJson['result'][i]['pair']);
}


// print metadata about pairs
async function Metadata(){
    myJson['result'].forEach((item, index)=>{
        trades_count_5min[item['pair'].toUpperCase()] = 0;
        orders_count_5min[item['pair'].toUpperCase()] = 0;
        let pair_data = '@MD ' + item['pair'].toUpperCase() + ' spot ' + item['pair'].split('_')[0].toUpperCase() + ' ' + item['pair'].split('_')[1].toUpperCase() + ' ' 
        + item['decimal'] + ' 1 1 0 0';
        console.log(pair_data);
    })
    console.log('@MDEND')
}

Metadata();


//function to get current time in unix format
function getUnixTime(){
    return Math.floor(Date.now());
}


// func to print trades
async function getTrades(message){
    trades_count_5min[message['d'][0].toUpperCase()] += 1;
    var trade_output = '! ' + getUnixTime() + ' ' + 
    message['d'][0].toUpperCase() + ' ' + 
    (message['d'][3] === '2' ? 'S ' : 'B ') + message['d'][1] + ' ' + message['d'][2];
    console.log(trade_output);
}


// func to print orderbooks and deltas
async function getSnaphots(message){
    orders_count_5min[message['d']['pair'].toUpperCase()] += message['d']['bids'].length + message['d']['asks'].length;
    // check if bids array is not Null
    if(message['d']['bids']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['d']['pair'].toUpperCase() + ' B '
        var pq = '';
        for(let i = 0; i < message['d']['bids'].length; i++){
            pq += message['d']['bids'][i][1] + '@' + message['d']['bids'][i][0] + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }

    // check if asks array is not Null
    if(message['d']['asks']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['d']['pair'].toUpperCase() + ' S '
        var pq = '';
        for(let i = 0; i < message['d']['asks'].length; i++){
            pq += message['d']['asks'][i][1] + '@' + message['d']['asks'][i][0] + '|';
        }
        pq = pq.slice(0, -1);
        console.log(order_answer + pq + ' R');
    }
}

// func to print orderbooks and deltas
async function getDeltas(message){

    // check if bids array is not Null
    if(message['d']['add']){
        
        var order_answer = '$ ' + getUnixTime() + ' ' + message['d']['pair'].toUpperCase() + ' B '
        var pq = '';
        if(message['d']['add']['bids']){
            orders_count_5min[message['d']['pair'].toUpperCase()] += message['d']['add']['bids'].length;
            for(let i = 0; i < message['d']['add']['bids'].length; i++){
                pq += message['d']['add']['bids'][i][1] + '@' + message['d']['add']['bids'][i][0] + '|';
            }
            pq = pq.slice(0, -1);
        }
        if(message['d']['del']){
            if(message['d']['del']['bids']){
                orders_count_5min[message['d']['pair'].toUpperCase()] += message['d']['del']['bids'].length;
                for(let i = 0; i < message['d']['del']['bids'].length; i++){
                    pq += '0@' + message['d']['del']['bids'][i][0] + '|';
                }
                pq = pq.slice(0, -1);
            }
        }
        if(pq !== ''){
            console.log(order_answer + pq);
        }
    }

    // check if asks array is not Null
    if(message['d']['add']){
        var order_answer = '$ ' + getUnixTime() + ' ' + message['d']['pair'].toUpperCase() + ' S '
        var pq = '';
        if(message['d']['add']['asks']){
            orders_count_5min[message['d']['pair'].toUpperCase()] += message['d']['add']['asks'].length;
            for(let i = 0; i < message['d']['add']['asks'].length; i++){
                pq += message['d']['add']['asks'][i][1] + '@' + message['d']['add']['asks'][i][0] + '|';
            }
            pq = pq.slice(0, -1);
        }
        if(message['d']['del']){
            if(message['d']['del']['asks']){
                orders_count_5min[message['d']['pair'].toUpperCase()] += message['d']['del']['asks'].length;
                for(let i = 0; i < message['d']['del']['asks'].length; i++){
                    pq += '0@' + message['d']['del']['asks'][i][0] + '|';
                }
                pq = pq.slice(0, -1);
            }
        }
        if(pq !== ''){
            console.log(order_answer + pq);
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
}


const biboxws = 'wss://npush.bibox360.com';

let wsClass = function () {

};

wsClass.prototype._decodeMsg = function (data) {
    let data1 = data.slice(1, data.length);
    zlib.unzip(data1, (err, buffer) => {
        if (err) {
            console.log(err);
        } else {
            try {
                let res = JSON.parse(buffer.toString());
                if(res['topic'].slice(-5, res['topic'].length) === 'depth' && res['t'] === 0){
                    getSnaphots(res);
                }else if(res['topic'].slice(-5, res['topic'].length) === 'deals' && res['t'] === 0){
                    // skip trade history
                }else if(res['topic'].slice(-5, res['topic'].length) === 'depth' && res['t'] === 1){
                    getDeltas(res);
                }
            } catch (e) {
                console.log(e);
            }
        }
    });
};

wsClass.prototype._initWs = async function () {
    let that = this;
    let ws = new WebSocket(biboxws);
    that.ws = ws;

    ws.on('open', function open() {
        currencies.forEach((item)=>{
            {
                ws.send(JSON.stringify({
                    event: 'addChannel',
                    sub: `${item}_deals`,
                }));
                if(getenv.string("SKIP_ORDERBOOKS", '') === '' || getenv.string("SKIP_ORDERBOOKS") === null){
                    ws.send(JSON.stringify({
                        sub: `${item}_depth`,
                    }));  
                }
                
            }
        });
        
    });

    ws.on('close', err => {
        console.log('close, ', err);
        setTimeout(async function() {
            CreateInstance();
            }, 500);
    });

    ws.on('error', err => {
        console.log('error', err);
    });

    ws.on('ping', err => {
        console.log('ping ', err.toString('utf8'));
    });

    ws.on('pong', err => {
        console.log('pong ', err.toString('utf8'));
    });

    ws.on('message', data => {
        if (data[0] == '1') {
            that._decodeMsg(data);
            // skip trades history
        } else if (data[0] == '0') {
            let dataJSON = JSON.parse(data.slice(1));
            // console.log(dataJSON);
            if(dataJSON['topic'].slice(-5, dataJSON['topic'].length) === 'deals' && dataJSON['t'] === 0){
                // skip trade history
            }else if(dataJSON['topic'].slice(-5, dataJSON['topic'].length) === 'deals' && dataJSON['t'] === 1){
                getTrades(dataJSON);
            }
            // else if(dataJSON['topic'].slice(-5) === 'depth' && dataJSON['t'] === false){
            //     getSnaphots(dataJSON);
            // }else if(dataJSON['topic'].slice(-5) === 'depth' && dataJSON['t'] === true){
            //     getDeltas(dataJSON);
            // }
            else{
                console.log(dataJSON);
            }
        } else {
            //console.log(that._decodeMsg(data));
        }

    });
};

function CreateInstance(){
    let instance = new wsClass();

    instance._initWs().catch(err => {
        console.log(err);
    });
}

CreateInstance();

stats();
setInterval(stats, 300000);
