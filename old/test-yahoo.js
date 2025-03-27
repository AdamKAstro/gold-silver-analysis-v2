// test-yahoo.js
const yahooFinance = require('yahoo-finance2').default;

const tickers = ['AAB.TO', 'IRV.CN', 'NVO']; // Add a known good one like NVO

async function test() {
    console.log('Testing yahoo-finance2 directly...');
    for (const ticker of tickers) {
        console.log(`\nFetching ${ticker}...`);
        try {
            const result = await yahooFinance.quote(ticker, {
                 fields: ['regularMarketPrice', 'currency', 'marketCap']
            });
            console.log(`Success for ${ticker}:`, result);
        } catch (error) {
            console.error(`Error fetching ${ticker}:`, error.message);
             if(error.stack) console.error(error.stack); // Show stack trace
        }
    }
}

test();