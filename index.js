var fs = require('fs');
var Crawler = require('crawler');
var json2csv = require('json2csv');
var moment = require('moment');

const dirName = './data';

function scrapper () {

    if (!fs.existsSync(dirName)){
        fs.mkdirSync(dirName);
    }
    
    // set field names
    var currencyFields = ['Name', 'Symbol', 'Circulating Supply', 'Dollar Volume'];
    var historyFileds = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap'];
    
    // crawler for currency list
    var currencyCrawler = new Crawler({
        maxConnections: 10,
    })

    currencyCrawler.direct({
        uri: 'https://coinmarketcap.com/all/views/all/',
        callback: function (err, res) {
            if (err) {
                console.log(err);
            } else {
                var $ = res.$;
                var allCurrencies = [];

                // parse currency list
                $('#currencies-all tbody tr').each(function () {
                    var currency = {};
                    currency['Name'] = $(this).find('td a.currency-name-container').text();
                    currency['Link'] = $(this).find('td a.currency-name-container').attr('href');
                    currency['Symbol'] = $(this).find('td.col-symbol').text();
                    currency['Circulating Supply'] = $(this).find('td.circulating-supply a').attr('data-supply');
                    currency['Dollar Volume'] = $(this).find('td a.volume').attr('data-usd');
                    allCurrencies.push(currency);
                });

                // write currency list information
                var currencyCSV = json2csv({data: allCurrencies, fields: currencyFields});
                fs.writeFileSync(dirName + '/coinlist.csv', currencyCSV);

                console.log('Wrote all currency list into "data/coinlist.csv" file. Total Count: ', allCurrencies.length);

                // crawler for currency histories
                var historyCrawler = new Crawler ({
                    maxConnections: 10,
                    rateLimit: 300
                });

                var allHistories = [];

                // if queue is empty, write grabed history to csv file
                historyCrawler.on('drain', function(){
                    console.log("-------End----------");
                    console.log('Total entries: ', allHistories.length);
                    var historyCSV = json2csv({data: allHistories, fields: historyFileds});
                    fs.writeFileSync(dirName + '/coinpricehistory.csv', historyCSV);
                });

                allCurrencies.forEach(function(currency) {
                    // make url from currency link and current date
                    var url = 'https://coinmarketcap.com' + currency.Link + 'historical-data?start=20160101&end=' + moment().format('YYYYMMDD');
                    
                    // add to queue of history crawler
                    historyCrawler.queue({
                        uri: url,
                        callback: function(err, res, done) {
                            if (err) {
                                console.log(err);
                            } else {
                                var $ = res.$;
                                var histories = [];

                                $('#historical-data table tbody tr').each(function() {
                                    var tds = $(this).find('td');
                                    var history = {};
                                    history['Ticker'] = currency['Symbol'];
                                    history['Date'] = moment($(tds[0]).text(), 'll').format('YYYY-MM-DD');
                                    history['Open'] = $(tds[1]).text();
                                    history['High'] = $(tds[2]).text();
                                    history['Low'] = $(tds[3]).text();
                                    history['Close'] = $(tds[4]).text();
                                    history['Volume'] = $(tds[5]).text().replace(/\,/g,"");
                                    history['Market Cap'] = $(tds[6]).text().replace(/\,/g,"");

                                    histories.push(history);
                                });

                                console.log(currency.Name + ' : ' + histories.length);
                                allHistories = allHistories.concat(histories);
                            }
                            done();
                        }
                    });
                });
            }
        }
    });
}

scrapper();