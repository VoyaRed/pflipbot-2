// bot.cjs - UpsideDownCake 24/7 Engine 🍰
const { ethers } = require('ethers');
const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
const ccxt = require('ccxt'); 
const WebSocket = require('ws');

// --- CONFIG ---
const SUPABASE_URL = 'https://tggqamigkruvhoqkyxrq.supabase.co';
const SUPABASE_KEY = 'sb_publishable_HVa5hO_AyTxmsI_iIgrDBA_jSenZuSD';
const supabaseClient = createClient(SUPABASE_URL, SUPABASE_KEY);

const PREDICT_ADDR = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA";
const ABI = [
    "function currentEpoch() view returns (uint256)", 
    "function rounds(uint256) view returns (uint256 epoch, uint256 startTimestamp, uint256 lockTimestamp, uint256 closeTimestamp, int256 lockPrice, int256 closePrice, uint256 lockOracleId, uint256 closeOracleId, uint256 totalAmount, uint256 bullAmount, uint256 bearAmount, uint256 rewardBaseCalAmount, uint256 rewardAmount, bool oracleCalled)"
];

// Initialize Binance exchange with automatic rate limiting
const exchange = new ccxt.binance({
    enableRateLimit: true, 
    options: { defaultType: 'spot' }
});

// --- STATE VARIABLES ---
let provider, contract;
let lastEpochChecked = 0;
let memoryStore = {};
let lastScrapeTime = 0;
const SCRAPE_INTERVAL = 22000; 

let localCandles = [];

function startCandleStream() {
    const wsUrl = 'wss://stream.binance.com:9443/ws/bnbusdt@kline_5m';
    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
        console.log("🔌 Live WebSocket connected to Binance (BNB/USDT 5m)");
    });

    ws.on('message', (data) => {
        const message = JSON.parse(data);
        const kline = message.k; 

        if (Math.random() < 0.01) { 
            console.log(`✅ Heartbeat: Received tick for BNB/USDT. Current Close: ${kline.c}`);
        }
        
        const candle = [
            kline.t,
            parseFloat(kline.o),
            parseFloat(kline.h),
            parseFloat(kline.l),
            parseFloat(kline.c),
            parseFloat(kline.v)
        ];

        if (localCandles.length > 0 && localCandles[localCandles.length - 1][0] === candle[0]) {
            localCandles[localCandles.length - 1] = candle; 
        } else {
            localCandles.push(candle);
            if (localCandles.length > 1000) localCandles.shift(); 
        }
    });

    ws.on('error', (err) => console.error("❌ WebSocket Error:", err));
    ws.on('close', () => {
        console.log("🔌 WebSocket disconnected. Reconnecting in 5 seconds...");
        setTimeout(startCandleStream, 5000);
    });
}

async function findFastestRPC() {
    const nodes = [
        "https://bsc-rpc.publicnode.com",
        "https://binance.llamarpc.com",
        "https://bsc-dataseed.binance.org"
    ];
    for (let url of nodes) {
        try {
            const p = new ethers.providers.JsonRpcProvider(url);
            const c = new ethers.Contract(PREDICT_ADDR, ABI, p);
            await c.currentEpoch(); 
            return { provider: p, contract: c };
        } catch (e) {
            console.warn(`Node ${url} failed, trying next...`);
        }
    }
    throw new Error("All RPC nodes failed.");
}

let isInitialFetchDone = false;
let binanceSleepUntil = 0; 

async function startBot() {
    console.log("🍰 UpsideDownCake 24/7 Engine Starting...");
    if (Date.now() < binanceSleepUntil) {
        const remainingTime = Math.ceil((binanceSleepUntil - Date.now()) / 1000);
        console.log(`💤 Bot is in Sleep Mode. Skipping initialization. Waking up in ${remainingTime}s...`);
        setTimeout(startBot, 10000);
        return;
    }

    try {
        console.log("Loading Binance markets to prevent rate limits...");
        await exchange.loadMarkets();
        
        console.log("Fetching initial 1000 candles...");
        localCandles = await exchange.fetchOHLCV('BNB/USDT', '5m', undefined, 1000);
        startCandleStream(); 
        isInitialFetchDone = true;

        const fastest = await findFastestRPC();
        provider = fastest.provider;
        contract = fastest.contract;

        console.log("✅ Connected to BSC successfully.");
        runLoop();
    } catch (error) {
        const errorMsg = error.message || "";
        const headers = error.response?.headers || error.responseHeaders;

        if (errorMsg.includes('418')) {
            // 🛑 418: IP AUTO-BANNED
            console.error(`🛑 [HTTP 418] IP Auto-Banned: Binance has temporarily banned your IP.`);
            
            let baseBanDurationMs = 5 * 60 * 1000; // Default fallback to 5 minutes
            let headerCode = "None provided";
            
            if (headers) {
                // Check standard retry headers or Binance's specific banned-until header
                const rawHeader = typeof headers.get === 'function' 
                    ? headers.get('retry-after') || headers.get('Retry-After') || headers.get('x-mbx-banned-until')
                    : headers['retry-after'] || headers['Retry-After'] || headers['x-mbx-banned-until'];
                    
                if (rawHeader) {
                    headerCode = rawHeader;
                    const parsedVal = parseInt(rawHeader, 10);
                    
                    if (!isNaN(parsedVal)) {
                        // If it's a huge number (13 digits), it's a Unix timestamp in milliseconds
                        if (parsedVal > 1000000000000) { 
                            baseBanDurationMs = Math.max(0, parsedVal - Date.now());
                        } 
                        // If it's a 10-digit number, it's a Unix timestamp in seconds
                        else if (parsedVal > 1000000000) {
                            baseBanDurationMs = Math.max(0, (parsedVal * 1000) - Date.now());
                        } 
                        // Otherwise, it's just a standard "Retry-After" in seconds
                        else {
                            baseBanDurationMs = parsedVal * 1000;
                        }
                    }
                }
            }

            // Add the 5 minutes (300,000 ms) safety padding on top of the ban
            const paddingMs = 5 * 60 * 1000; 
            const totalSleepMs = baseBanDurationMs + paddingMs;
            binanceSleepUntil = Date.now() + totalSleepMs;
            
            console.error(`   -> 🚨 MODERATOR ALERT 🚨`);
            console.error(`   -> Received Header/UNIX Code: ${headerCode}`);
            console.error(`   -> Translated Ban Duration: ${(baseBanDurationMs / 1000 / 60).toFixed(2)} minutes`);
            console.error(`   -> Adding 5-minute safety buffer...`);
            console.error(`   -> Total Sleep Mode Duration: ${(totalSleepMs / 1000 / 60).toFixed(2)} minutes`);
            
            setTimeout(startBot, totalSleepMs);

        } else if (errorMsg.includes('429')) {
            // ⚠️ 429: TOO MANY REQUESTS (Rate Limited)
            console.error(`⚠️ [HTTP 429] Rate Limit Hit: Too Many Requests.`);
            
            let sleepDurationMs = 60 * 1000; // Default fallback to 1 minute
            
            if (headers) {
                const rawHeader = typeof headers.get === 'function' 
                    ? headers.get('retry-after') || headers.get('Retry-After')
                    : headers['retry-after'] || headers['Retry-After'];
                    
                if (rawHeader) {
                    const retrySeconds = parseInt(rawHeader, 10);
                    // Ensure it's not a Unix timestamp before treating it as seconds
                    if (!isNaN(retrySeconds) && retrySeconds < 1000000000) { 
                        sleepDurationMs = retrySeconds * 1000;
                    }
                }
            }

            binanceSleepUntil = Date.now() + sleepDurationMs;
            console.error(`   -> Cooling down for ${(sleepDurationMs / 1000 / 60).toFixed(2)} minutes.`);
            setTimeout(startBot, sleepDurationMs);

        } else {
            // ❌ ALL OTHER ERRORS
            console.error(`❌ Initialization failed (Error: ${error.message}). Retrying in 10s...`);
            setTimeout(startBot, 10000);
        }
    }
}

async function runLoop() {
    try {
        await checkRound();
    } catch (error) {
        console.warn("Loop error:", error.message);
    }
    setTimeout(runLoop, 2000);
}

// --- NEW HELPER: Fetch dynamic EV threshold ---
async function getDynamicThreshold() {
    try {
        const { data, error } = await supabaseClient
            .from('bot_settings')
            .select('ev_threshold')
            .eq('id', 1)
            .single();
        if (error) throw error;
        return data ? data.ev_threshold : 1.5;
    } catch (e) {
        console.warn("Could not fetch EV threshold, defaulting to 1.5", e.message);
        return 1.5;
    }
}

async function checkRound() {
    const currentEpoch = (await contract.currentEpoch()).toNumber();
    
    // Garbage Collection
    const staleEpoch = currentEpoch - 10;
    Object.keys(memoryStore).forEach(key => {
        if (key.includes(`_${staleEpoch}`)) delete memoryStore[key];
    });

    const nextRoundData = await contract.rounds(currentEpoch);
    const lockTimestamp = nextRoundData.lockTimestamp.toNumber();
    const now = Math.floor(Date.now() / 1000);
    const secondsLeft = lockTimestamp - now;

    if (secondsLeft > 102) {
        if (!memoryStore[`cleared_${currentEpoch}`]) {
            console.log(`⏳ Epoch #${currentEpoch} just started. Sleeping until 102s mark...`);
            let lastAnalysis = "";
            const lastData = memoryStore[`best_${currentEpoch - 1}`];
            if (lastData && lastData.thought_process) {
                lastAnalysis = `\n\n--- LAST MARKET ANALYSIS ---\n${lastData.thought_process}`;
            }

            await supabaseClient
                .from('market_stats')
                .update({ 
                    current_pred: 'NONE', 
                    current_conf: 'Calculating...',
                    thought_process: `Just uhhh... waiting for the next epoch.. and then we can go from there! Yk what I mean!! ||  Waiting for initial 3-minute market settling...${lastAnalysis}`
                })
                .eq('id', 1);
            memoryStore[`cleared_${currentEpoch}`] = true;
        }
    }

    if (secondsLeft > 0 && secondsLeft <= 102 && !memoryStore[`locked_${currentEpoch}`]) {
        if (Date.now() - lastScrapeTime > SCRAPE_INTERVAL) {
            console.log(`📡 Scanning... Epoch #${currentEpoch} locks in ${secondsLeft}s`);
            await generatePrediction(currentEpoch);
            lastScrapeTime = Date.now();
        }
    }

    if (secondsLeft <= 33 && secondsLeft > 0 && !memoryStore[`locked_${currentEpoch}`]) {
        
        // 🔥 FORCE A FINAL SCAN RIGHT BEFORE LOCK-IN
        console.log(`⚡ Executing absolute final scan right before lock-in...`);
        await generatePrediction(currentEpoch);

        if (!memoryStore[`best_${currentEpoch}`]) {
            console.warn(`⚠️ Failsafe triggered: No prediction generated for #${currentEpoch}. Forcing fallback direction prediction.`);
            memoryStore[`best_${currentEpoch}`] = {
                current_pred: "NONE", 
                current_conf: "binance trippin 1sec",
                numeric: 50,
                later_pred: "NONE",
                later_conf: "0%",
                rsi: 0, macd: 0, price: 0,
                thought_process: "Emergency Fallback: Binance data retrieval timed out before lock."
            };
        }
        console.log(`⏱️ Locking in Epoch #${currentEpoch}`);
        await lockInPrediction(currentEpoch);
    }

    if (currentEpoch > 1) await verifyResult(currentEpoch - 1);
    
    try {
        const { data: pendingLogs } = await supabaseClient
            .from('prediction_logs')
            .select('epoch_id')
            .eq('result', 'PENDING');
        if (pendingLogs && pendingLogs.length > 0) {
            for (let log of pendingLogs) {
                if (log.epoch_id <= currentEpoch - 2) {
                    await verifyResult(log.epoch_id);
                }
            }
        }
    } catch (e) {
        console.error("Error checking pending rounds:", e);
    }
}

async function updateMarketStats(rsi, currentMACD, currentClose, currentPred = "NONE", currentConf = "0%", laterPred = "NONE", laterConf = "0%", thoughtProcess = "") {
    const { error } = await supabaseClient
        .from('market_stats')
        .upsert([{ 
            id: 1, 
            rsi: rsi, 
            macd: currentMACD, 
            price: currentClose,
            current_pred: currentPred,
            current_conf: currentConf,
            later_pred: laterPred,
            later_conf: laterConf,
            thought_process: thoughtProcess,
            updated_at: new Date().toISOString() 
        }]);
    if (error) console.error("Error updating stats:", error);
}

async function generatePrediction(targetEpoch) {
    try {
        memoryStore[`pred_${targetEpoch}`] = "PENDING";
        let candles = localCandles;
        
        if (!candles || candles.length < 50) {
            console.log("⚠️ Live stream not ready, falling back to REST API...");
            try {
                candles = await exchange.fetchOHLCV('BNB/USDT', '5m', undefined, 1000);
            } catch (e) {
                console.error("Direct connection failed:", e.message);
                throw e; 
            }
        }

        if (Array.isArray(candles) && candles.length >= 50) {
            console.log("✅ Binance market data verified.");
        } else {
            throw new Error("Insufficient candles returned from Binance.");
        }
        
        const opens = candles.map(c => parseFloat(c[1]));
        const highs = candles.map(c => parseFloat(c[2]));
        const lows = candles.map(c => parseFloat(c[3]));
        const closes = candles.map(c => parseFloat(c[4]));
        const volumes = candles.map(c => parseFloat(c[5])); 
        const currentClose = closes[closes.length - 1];

        // RSI Array Calculations
        let gains = [], losses = [];
        for (let i = 1; i < closes.length; i++) {
            let diff = closes[i] - closes[i - 1];
            gains.push(diff > 0 ? diff : 0);
            losses.push(diff < 0 ? Math.abs(diff) : 0);
        }
        
        let rsiHistory = [];
        let avgGain = gains.slice(0, 14).reduce((a, b) => a + b, 0) / 14;
        let avgLoss = losses.slice(0, 14).reduce((a, b) => a + b, 0) / 14;
        rsiHistory.push(avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss))));
        
        for (let i = 14; i < gains.length; i++) {
            avgGain = ((avgGain * 13) + gains[i]) / 14;
            avgLoss = ((avgLoss * 13) + losses[i]) / 14;
            let currentRsi = 100;
            if (avgLoss !== 0) currentRsi = 100 - (100 / (1 + (avgGain / avgLoss)));
            else if (avgGain === 0) currentRsi = 0;
            rsiHistory.push(currentRsi);
        }

        let rsi = rsiHistory[rsiHistory.length - 1];
        let previousRSI_3_candles_ago = rsiHistory[rsiHistory.length - 4] || rsi;
        let rsiSlope = (rsi - previousRSI_3_candles_ago) / 3;
        let previousRSI_1_candle_ago = rsiHistory[rsiHistory.length - 2] || rsi;
        let previousRSI_4_candles_ago = rsiHistory[rsiHistory.length - 5] || previousRSI_3_candles_ago;
        let previousRSISlope = (previousRSI_1_candle_ago - previousRSI_4_candles_ago) / 3;
        let rsiAcceleration = rsiSlope - previousRSISlope;

        // Bollinger Bands
        const bbPeriod = 20;
        const bbSlice = closes.slice(-bbPeriod);
        const sma = bbSlice.reduce((a, b) => a + b, 0) / bbPeriod;
        const variance = bbSlice.reduce((acc, val) => acc + Math.pow(val - sma, 2), 0) / bbPeriod;
        const stdDev = Math.sqrt(variance);
        const upperBB = sma + (stdDev * 2);
        const lowerBB = sma - (stdDev * 2);

        // EMA Helper & MACD
        const calculateEMAArray = (data, period) => {
            const k = 2 / (period + 1);
            let emaArray = [data[0]]; 
            for (let i = 1; i < data.length; i++) {
                emaArray.push((data[i] * k) + (emaArray[i - 1] * (1 - k)));
            }
            return emaArray;
        };
        const ema9 = calculateEMAArray(closes, 9)[closes.length - 1];
        const ema21 = calculateEMAArray(closes, 21)[closes.length - 1];
        const ema12Array = calculateEMAArray(closes, 12);
        const ema26Array = calculateEMAArray(closes, 26);
        const macdLineArray = ema12Array.map((v, i) => v - ema26Array[i]);
        const signalLineArray = calculateEMAArray(macdLineArray, 9);
        const currentMACD = macdLineArray[macdLineArray.length - 1];
        const currentSignal = signalLineArray[signalLineArray.length - 1];
        const currentHist = currentMACD - currentSignal;
        const prevHist = (macdLineArray[macdLineArray.length - 2] - signalLineArray[signalLineArray.length - 2]);

        // Volume
        const volSMA20 = volumes.slice(-20).reduce((a,b)=>a+b,0) / 20;
        const currentVol = volumes[volumes.length - 1];

        // Historical Trends
        let recentUps = 0, recentDowns = 0;
        const roundPromises = [];
        for(let i=1; i<=5; i++) {
            roundPromises.push(contract.rounds(targetEpoch - i).catch(() => null));
        }
        const pastRounds = await Promise.all(roundPromises);
        pastRounds.forEach(r => {
            if (r && r.oracleCalled) {
                const lp = parseFloat(ethers.utils.formatUnits(r.lockPrice, 8));
                const cp = parseFloat(ethers.utils.formatUnits(r.closePrice, 8));
                if (cp > lp) recentUps++;
                else if (cp < lp) recentDowns++;
            }
        });

        let upScore = 0, downScore = 0;
        let brainText = []; 
        let historyScore = { up: 0, down: 0 };
        let trendScore = { up: 0, down: 0 };
        let volScore = { up: 0, down: 0 };
        let patternScore = { up: 0, down: 0 };

        // --- 1. CALCULATE ATR (Moved up for Dynamic RSI) ---
        let trSum = 0;
        for (let i = closes.length - 14; i < closes.length; i++) {
            const highLow = highs[i] - lows[i];
            const highClose = Math.abs(highs[i] - closes[i-1]);
            const lowClose = Math.abs(lows[i] - closes[i-1]);
            trSum += Math.max(highLow, highClose, lowClose);
        }
        const atrPercentage = ((trSum / 14) / currentClose) * 100;

        // --- 2. TWEAK 1: DYNAMIC RSI THRESHOLDS ---
        let dynamicCeiling = Math.min(80, Math.max(65, 60 + (atrPercentage * 50)));
        let dynamicFloor = Math.max(20, Math.min(35, 40 - (atrPercentage * 50)));

        if (rsiSlope > 0.5) {
            brainText.push("RSI is aggressively rising; momentum is strong.");
            if (rsi > dynamicCeiling) {
                volScore.down += 4.0;
                brainText.push(`Warning: RSI (${rsi.toFixed(1)}) exceeded dynamic ceiling (${dynamicCeiling.toFixed(1)}). Anticipating a bearish exhaustion reversal.`);
            } else {
                trendScore.up += 1.0;
            }
        }
        if (rsiSlope < -0.5) {
            brainText.push("RSI is skyrocketing downward; bearish momentum is accelerating.");
            if (rsi < dynamicFloor) {
                volScore.up += 4.0;
                brainText.push(`Warning: RSI (${rsi.toFixed(1)}) breached dynamic floor (${dynamicFloor.toFixed(1)}). Anticipating a bullish exhaustion reversal.`);
            } else {
                trendScore.down += 1.0;
            }
        }
        if (rsiAcceleration < 0 && rsi > 60) brainText.push("Warning: RSI rise is slowing down; potential overbought reversal.");

        // CATEGORY 1: Round History
        if (recentUps >= 3) { historyScore.up += 1.0; brainText.push("Recent historical rounds lean bullish."); }
        if (recentUps === 5) historyScore.up += 1.5;
        if (recentDowns >= 3) { historyScore.down += 1.0; brainText.push("Recent historical rounds lean bearish."); }
        if (recentDowns === 5) historyScore.down += 1.5;

        const prevOpen = opens[opens.length - 2];
        const prevClose = closes[closes.length - 2];
        const prevHigh = highs[highs.length - 2];
        const prevLow = lows[lows.length - 2];
        
        const upperWick = prevHigh - Math.max(prevOpen, prevClose);
        const lowerWick = Math.min(prevOpen, prevClose) - prevLow;
        const bodySize = Math.max(Math.abs(prevClose - prevOpen), 0.0001);
        const roc3 = ((currentClose - closes[closes.length - 4]) / closes[closes.length - 4]) * 100;

        let bbWidth = (upperBB - lowerBB) / sma;
        let isChoppy = bbWidth < 0.0015;

        // Choppiness & Trend logic
        if ((atrPercentage < 0.08 || isChoppy) && ema9 > ema21) { 
            volScore.up += 2.5;
            brainText.push("Volatility is extremely low, executing a Mean-Reversion selection.");
            if (currentClose < sma) {
                volScore.up += 2.5;
                brainText.push("Price is lagging beneath the Moving Average, forcing a counter-structural upcall.");
                if (rsi < 40) { volScore.up += 1.5; brainText.push(`RSI is low at ${rsi.toFixed(1)}, optimizing safety threshold.`); }
            } else if (currentClose > sma) {
                volScore.down += 2.5;
                brainText.push("Price is floating above the Moving Average, forcing a counter-structural downcall.");
                if (rsi > 60) { volScore.down += 1.5; brainText.push(`RSI is elevated at ${rsi.toFixed(1)}, optimizing resistance threshold.`); }
            }
        } else if ((atrPercentage < 0.08 || isChoppy) && ema9 < ema21) {
            volScore.down += 2.5;
            brainText.push("Volatility is extremely low, executing a Mean-Reversion selection.");
            if (currentClose > sma) {
                volScore.down += 2.5;
                brainText.push("Price is floating above the Moving Average, forcing a counter-structural downcall.");
            }
        } else {
            brainText.push("Market is showing structural momentum, engaging Trend analysis.");
            if (ema9 > ema21) { trendScore.up += 1.0; brainText.push("Fast EMA(9) leads Slow EMA(21) (Bullish configuration)."); }
            if (ema9 < ema21) { trendScore.down += 1.0; brainText.push("Fast EMA(9) trails Slow EMA(21) (Bearish configuration)."); }
            
            if (currentMACD > currentSignal && currentHist > prevHist) { 
                trendScore.up += 3.0;
                brainText.push("MACD histogram is expanding upward, displaying strong structural expansion.");
            }
            if (currentMACD < currentSignal && currentHist < prevHist) { 
                trendScore.down += 3.0;
                brainText.push("MACD histogram is expanding downward, displaying strong structural compression.");
            }
            
            if (roc3 > 0.15) trendScore.up += 3.0;
            if (roc3 < -0.15) trendScore.down += 3.0; 

            if (upperWick > bodySize * 2) { patternScore.down += 3.5; brainText.push("Spotted a long upper wick on the previous candle, predicting supply overhead."); }
            if (lowerWick > bodySize * 2) { patternScore.up += 3.5; brainText.push("Spotted a long lower wick on the previous candle, predicting clear demand protection."); }

            if (currentClose > upperBB && rsi > 72) { volScore.down += 4.5; brainText.push("Price pierced the Upper Bollinger Band with overextended RSI. Overrides forced a downward bias."); }
            if (currentClose < lowerBB && rsi < 28) { volScore.up += 4.5; brainText.push("Price pierced the Lower Bollinger Band with crushed RSI. Overrides forced an upward bias."); }
        }

        // --- TWEAK 2: ADJUSTED CATEGORICAL CAPS ---
        historyScore.up = Math.min(historyScore.up, 2.5);
        historyScore.down = Math.min(historyScore.down, 2.5);

        trendScore.up = Math.min(trendScore.up, 3.5); 
        trendScore.down = Math.min(trendScore.down, 3.5); 

        volScore.up = Math.min(volScore.up, 5.5); 
        volScore.down = Math.min(volScore.down, 5.5); 
        
        patternScore.up = Math.min(patternScore.up, 3.5);
        patternScore.down = Math.min(patternScore.down, 3.5);

        upScore = historyScore.up + trendScore.up + volScore.up + patternScore.up;
        downScore = historyScore.down + trendScore.down + volScore.down + patternScore.down;

        let netScore = Math.abs(upScore - downScore);
        if (isNaN(netScore)) netScore = 0;

        if (upScore === downScore) {
            brainText.push("Data is perfectly tied. Using directional EMA trend alignment as the structural tie-breaker.");
            if (ema9 >= ema21) { upScore += 1.5; } else { downScore += 1.5; }
            netScore = Math.abs(upScore - downScore);
        }
        
        let currentPred = (upScore > downScore) ? "UP" : "DOWN";
        brainText.push(`Conclusion: The aggregate weight of the technical data firmly favors ${currentPred}.`);
        console.log(`📊 Category Breakdown [Target #${targetEpoch}] -> History: U:${historyScore.up}/D:${historyScore.down} | Trend: U:${trendScore.up}/D:${trendScore.down} | Volatility/BB: U:${volScore.up}/D:${volScore.down} | Patterns: U:${patternScore.up}/D:${patternScore.down}`);
        
        const ThoughtProcess = brainText.join(" ");
        let numericConfidence = Math.min(92.0, 60 + (netScore * 2.5));
        let finalConfidence = numericConfidence.toFixed(1) + "%";
        let displayConf = finalConfidence;

        // --- TWEAK 3: THE CONFIDENCE FILTER (SKIP PROTOCOL) ---
        if (numericConfidence < 65.0) {
            brainText.push(`Confidence is too low (${finalConfidence}). Executing SKIP protocol to protect capital.`);
            currentPred = "SKIP";
            displayConf = `${finalConfidence} (Try: ${upScore > downScore ? "UP" : "DOWN"})`;
        }

        let laterUpProb = 50 + (ema9 > ema21 ? 10 : -10) + ((rsi - 50) * 0.4) + (recentUps > recentDowns ? 5 : -5);
        if (isNaN(laterUpProb)) laterUpProb = 50; 
        laterUpProb = Math.max(10, Math.min(90, laterUpProb));
        let laterDownProb = 100 - laterUpProb;
        let laterPred = laterUpProb > 50 ? "UP" : "DOWN";
        let laterMajorityProb = Math.max(laterUpProb, laterDownProb).toFixed(1);
        console.log(`🔥 Live Scan Update! Direction: ${currentPred} | current_conf: ${displayConf}`);
        
        memoryStore[`best_${targetEpoch}`] = {
            current_pred: currentPred,
            current_conf: displayConf,
            numeric: (numericConfidence - 1),
            later_pred: laterPred,
            later_conf: laterMajorityProb,
            rsi: rsi,
            macd: currentMACD,
            price: currentClose,
            thought_process: ThoughtProcess
        };
        await updateMarketStats(rsi, currentMACD, currentClose, currentPred, displayConf, laterPred, laterMajorityProb, ThoughtProcess);
    } catch (e) {
        console.error("Brain Failed:", e);
    }
}

async function lockInPrediction(targetEpoch) {
    const bestData = memoryStore[`best_${targetEpoch}`];
    if (!bestData || bestData.numeric === -1) return;

    // --- TWEAK 4: LATE EXPECTED VALUE (EV) FILTER ---
    // Execute this right before lock-in so PancakeSwap pools are actually populated
    if (bestData.current_pred === "UP" || bestData.current_pred === "DOWN") {
        try {
            const liveRoundData = await contract.rounds(targetEpoch);
            const bullPool = parseFloat(ethers.utils.formatUnits(liveRoundData.bullAmount, 18));
            const bearPool = parseFloat(ethers.utils.formatUnits(liveRoundData.bearAmount, 18));
            const totalPool = bullPool + bearPool;

            let expectedMultiplier = 0;
            if (totalPool > 0) {
                // PCS takes a ~3% fee, leaving 97% for the reward pool
                const rewardPool = totalPool * 0.97;
                if (bestData.current_pred === "UP" && bullPool > 0) expectedMultiplier = rewardPool / bullPool;
                if (bestData.current_pred === "DOWN" && bearPool > 0) expectedMultiplier = rewardPool / bearPool;
            }

            const MINIMUM_PAYOUT = await getDynamicThreshold();
            if (expectedMultiplier > 0 && expectedMultiplier < MINIMUM_PAYOUT) {
                const skipReason = `Risk/Reward skewed at lock-in. Payout too low (${expectedMultiplier.toFixed(2)}x). Executing SKIP to preserve EV.`;
                console.log(`⚠️ ${skipReason}`);
                
                // Override the prediction to SKIP but keep the visual UI intact
                const originalSide = bestData.current_pred;
                bestData.current_pred = "SKIP";
                
                // Format: "82.5% (Try: UP)"
                // Make sure we only append "(Try:" if it isn't already there from Tweak 3
                if (!bestData.current_conf.includes("(Try:")) {
                    bestData.current_conf = `${bestData.current_conf} (Try: ${originalSide})`;
                }
                bestData.thought_process += ` | ${skipReason}`;
            } else if (expectedMultiplier >= MINIMUM_PAYOUT) {
                console.log(`✅ Risk/Reward favorable at lock-in. Potential payout: ${expectedMultiplier.toFixed(2)}x.`);
                bestData.thought_process += ` | Final EV confirmed at ${expectedMultiplier.toFixed(2)}x.`;
            }
        } catch (error) {
            console.warn("Could not fetch live pool sizes for EV calculation:", error.message);
        }
    }

    memoryStore[`locked_${targetEpoch}`] = true;
    console.log(`\n🔒 ROUND LIVE! Locking in best prediction for Epoch #${targetEpoch}: ${bestData.current_pred} (${bestData.current_conf})`);
    
    if (bestData.numeric >= 65.0 && bestData.current_pred !== "SKIP") {
        const webhookUrl = "https://discord.com/api/webhooks/1520463983998537800/T1xaGGZJ7YA_aw7JnbVKkyf9HwWta8D3W3VbuDhw5_vEiBtrqKqnzG37VIKH9WcwABx8";
        fetch(webhookUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username: "Cake Alert Bot 🍰",
                content: `🚨 **High Confidence Alert!** 🚨\nEpoch: #${targetEpoch}\nPrediction: **${bestData.current_pred}**\nConfidence: **${bestData.current_conf}**`
            })
        }).catch(err => console.error("Failed to send webhook:", err));
    }
    
    const { error } = await supabaseClient.from('prediction_logs').upsert([{ 
        epoch_id: targetEpoch, 
        predicted_side: bestData.current_pred, 
        result: 'PENDING',
        confidence: bestData.current_conf,
        is_locked: true,        
    }], { 
        onConflict: 'epoch_id' 
    });
    if (error) console.error("❌ Early Supabase insert error:", error);
    await updateMarketStats(bestData.rsi, bestData.macd, bestData.price, "NONE", "Calculating...", bestData.later_pred, bestData.later_conf, bestData.thought_process);
}

async function verifyResult(epochToCheck) {
    try {
        const round = await contract.rounds(epochToCheck);
        if (!round.oracleCalled) return; 

        const lockPrice = parseFloat(ethers.utils.formatUnits(round.lockPrice, 8));
        const closePrice = parseFloat(ethers.utils.formatUnits(round.closePrice, 8));
        
        let actualResult;
        if (closePrice === lockPrice) {
            actualResult = "TIE";
        } else {
            actualResult = closePrice > lockPrice ? "UP" : "DOWN"; 
        }
        
        const { data, error: fetchError } = await supabaseClient
            .from('prediction_logs')
            .select('*')
            .eq('epoch_id', epochToCheck)
            .single();
        if (fetchError || !data) return;
        if (data.result !== 'PENDING') return;
        
        let resultStatus;
        if (actualResult === "TIE") {
            resultStatus = "TIE";
        } else if (data.predicted_side.startsWith("SKIP")) {
            resultStatus = "SKIP/" + actualResult;
        } else {
            resultStatus = (data.predicted_side === actualResult) ? "WIN" : "LOSS"; 
        }

        console.log(`\n⚖️ [Epoch ${epochToCheck}] Resolving... Result: ${resultStatus}`);
        const { error: updateError } = await supabaseClient
            .from('prediction_logs')
            .update({ result: resultStatus })
            .eq('epoch_id', epochToCheck);
        if (updateError) {
            console.error(`❌ Supabase Update Error for Epoch ${epochToCheck}:`, updateError.message);
            return;
        }

        const { data: recentLogs } = await supabaseClient
            .from('prediction_logs')
            .select('result, confidence')
            .in('result', ['WIN', 'LOSS', 'SKIP/UP', 'SKIP/DOWN'])
            .order('epoch_id', { ascending: false })
            .limit(15);
        if (recentLogs && recentLogs.length > 0) {
            const mixedWins = recentLogs.filter(l => l.result === 'WIN' || l.result === 'SKIP/UP').length;
            const mixedRate = ((mixedWins / recentLogs.length) * 100).toFixed(1);

            const trendLogs = recentLogs.filter(l => {
                const match = l.confidence.match(/(\d+(?:\.\d+)?)/);
                return match ? parseFloat(match[1]) >= 55.0 : false;
            });
            const trendWins = trendLogs.filter(l => l.result === 'WIN' || l.result === 'SKIP/UP').length;
            const trendRate = trendLogs.length > 0 ? ((trendWins / trendLogs.length) * 100).toFixed(1) : "0.0";

            console.log(`📈 Mixed Market (Overall Average): ${mixedRate}%`);
            console.log(`🚀 Trend Market (Conviction > 55%): ${trendRate}%`);
        }
    } catch(e) { 
        console.error("Result Verification Failed:", e);
    }
}

// Start bot and HTTP listener
startBot();
const http = require('http');
http.createServer((req, res) => { res.writeHead(200); res.end('Bot running'); }).listen(process.env.PORT || 3000);
