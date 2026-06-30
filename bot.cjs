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

// Local storage for our zero-latency candles
let localCandles = [];
let isInitialFetchDone = false;
let binanceSleepUntil = 0; 

// NEW: Error tracking for Exponential Backoff
let consecutiveLoopErrors = 0; 
let startBotErrorCount = 0;

// WebSocket Stream Manager
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
        
        // UPGRADE 1: Capture 'V' (Taker buy base asset volume) for Volume Delta calculation
        const candle = [
            kline.t,
            parseFloat(kline.o),
            parseFloat(kline.h),
            parseFloat(kline.l),
            parseFloat(kline.c),
            parseFloat(kline.v),
            parseFloat(kline.V || 0) // New: Taker Buy Volume
        ];

        // Update the current candle if time matches, otherwise push new candle
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

        // Reset the startBot error count upon a successful boot
        startBotErrorCount = 0; 

        console.log("✅ Connected to BSC successfully.");
        runLoop();
    } catch (error) {
        if (error.message.includes('418') || error.message.includes('429')) {
            // ... (Keep your existing Ban Header logic here exactly as it was) ...
            let sleepDurationMs = 5 * 60 * 1000;
            let penaltySource = "Default Fallback";

            const banMatch = error.message.match(/banned until (\d+)/);
            if (banMatch && banMatch[1]) {
                const banLiftTimestampMs = parseInt(banMatch[1], 10);
                const bufferMs = 5000; 
                sleepDurationMs = (banLiftTimestampMs - Date.now()) + bufferMs;
                penaltySource = "Binance Error Timestamp (+ 5s buffer)";
                if (sleepDurationMs <= 0) {
                    sleepDurationMs = 5 * 60 * 1000;
                    penaltySource = "Default Fallback (Timestamp was in the past)";
                }
            } else {
                const headers = error.response?.headers || error.responseHeaders;
                if (headers) {
                    const rawHeader = typeof headers.get === 'function' ? headers.get('retry-after') || headers.get('Retry-After') : headers['retry-after'] || headers['Retry-After'];
                    if (rawHeader) {
                        const retrySeconds = parseInt(rawHeader, 10);
                        if (!isNaN(retrySeconds)) {
                            sleepDurationMs = retrySeconds * 1000;
                            penaltySource = "Exact Binance Header";
                        }
                    }
                }
            }

            binanceSleepUntil = Date.now() + sleepDurationMs;
            const sleepMinutes = (sleepDurationMs / 1000 / 60).toFixed(2);

            console.error(`🚨 Binance Ban/Rate Limit Detected!`);
            console.error(`   -> Source: ${penaltySource}`);
            console.error(`   -> Entering Sleep Mode for ${sleepMinutes} minutes.`);
            setTimeout(startBot, sleepDurationMs);
        } else {
            // NEW: Exponential Backoff for general initialization errors (502s, timeouts, etc.)
            startBotErrorCount++;
            
            // 10s -> 20s -> 40s -> 80s -> Max 5 minutes (300,000ms)
            const fallbackDelayMs = Math.min(10000 * Math.pow(2, startBotErrorCount), 300000); 
            
            console.error(`❌ Initialization failed (Error: ${error.message}).`);
            console.error(`   -> Applying backoff penalty. Retrying in ${fallbackDelayMs / 1000}s...`);
            setTimeout(startBot, fallbackDelayMs);
        }
    }
}

async function runLoop() {
    try {
        await checkRound();
        
        // If successful, reset the error counter back to zero
        consecutiveLoopErrors = 0; 
        
        // Standard 2-second interval when healthy
        setTimeout(runLoop, 2000); 
    } catch (error) {
        consecutiveLoopErrors++;
        console.warn(`Loop error: ${error.message}`);
        
        // EXPONENTIAL BACKOFF: 2s -> 4s -> 8s -> 16s -> 32s -> max 60s
        const delayMs = Math.min(2000 * Math.pow(2, consecutiveLoopErrors), 60000);
        
        console.warn(`⚠️ RPC/Network hiccup detected. Applying backoff to prevent spam. Retrying in ${delayMs / 1000} seconds...`);
        setTimeout(runLoop, delayMs);
    }
}

async function checkRound() {
    const currentEpoch = (await contract.currentEpoch()).toNumber();

    // Garbage Collection for old epochs
    const staleEpoch = currentEpoch - 10;
    Object.keys(memoryStore).forEach(key => {
        if (key.includes(`_${staleEpoch}`)) delete memoryStore[key];
    });

    // SCAN THE CURRENT ROUND
    const nextRoundData = await contract.rounds(currentEpoch);
    const lockTimestamp = nextRoundData.lockTimestamp.toNumber();
    const now = Math.floor(Date.now() / 1000);
    const secondsLeft = lockTimestamp - now;

    // RESET AT START OF NEW ROUND
    if (secondsLeft > 102) {
        if (!memoryStore[`cleared_${currentEpoch}`]) {
            console.log(`⏳ Epoch #${currentEpoch} just started. Sleeping until 102s mark...`);
            let lastAnalysis = "";
            const lastData = memoryStore[`best_${currentEpoch - 1}`];

            if (lastData && lastData.thought_process) {
                lastAnalysis = `\n\n--- LAST MARKET ANALYSIS ---\n${lastData.thought_process}`;
            }

            await supabaseClient.from('market_stats').update({ 
                current_pred: 'NONE', current_conf: 'Calculating...',
                thought_process: `Waiting for initial 3-minute market settling...${lastAnalysis}`
            }).eq('id', 1);

            memoryStore[`cleared_${currentEpoch}`] = true;
        }
    }

    // SCAN 
    if (secondsLeft > 0 && secondsLeft <= 102 && !memoryStore[`locked_${currentEpoch}`]) {
        if (Date.now() - lastScrapeTime > SCRAPE_INTERVAL) {
            console.log(`📡 Scanning... Epoch #${currentEpoch} locks in ${secondsLeft}s`);
            await generatePrediction(currentEpoch);
            lastScrapeTime = Date.now();
        }
    }

    // LOCK-IN at 33 seconds
    if (secondsLeft <= 33 && secondsLeft > 0 && !memoryStore[`locked_${currentEpoch}`]) {
        if (!memoryStore[`best_${currentEpoch}`]) {
            console.warn(`⚠️ Failsafe triggered: No prediction generated for #${currentEpoch}. Forcing fallback.`);
            memoryStore[`best_${currentEpoch}`] = {
                current_pred: "NONE", current_conf: "binance trippin 1sec", numeric: 50,
                later_pred: "NONE", later_conf: "0%", rsi: 0, macd: 0, price: 0,
                thought_process: "Emergency Fallback: Binance data retrieval timed out before lock."
            };
        }
        console.log(`⏱️ Locking in Epoch #${currentEpoch}`);
        await lockInPrediction(currentEpoch);
    }

    // VERIFY PENDING EXPIRED ROUNDS
    if (currentEpoch > 1) await verifyResult(currentEpoch - 1);

    try {
        const { data: pendingLogs } = await supabaseClient.from('prediction_logs').select('epoch_id').eq('result', 'PENDING');
        if (pendingLogs && pendingLogs.length > 0) {
            for (let log of pendingLogs) {
                if (log.epoch_id <= currentEpoch - 2) await verifyResult(log.epoch_id);
            }
        }
    } catch (e) {
        console.error("Error checking pending rounds:", e);
    }
}

async function updateMarketStats(rsi, currentMACD, currentClose, currentPred = "NONE", currentConf = "0%", laterPred = "NONE", laterConf = "0%", thoughtProcess = "") {
    const { error } = await supabaseClient.from('market_stats').upsert([{ 
        id: 1, rsi: rsi, macd: currentMACD, price: currentClose,
        current_pred: currentPred, current_conf: currentConf,
        later_pred: laterPred, later_conf: laterConf,
        thought_process: thoughtProcess, updated_at: new Date().toISOString() 
    }]);
    if (error) console.error("Error updating stats:", error);
}

async function generatePrediction(targetEpoch) {
    try {
        memoryStore[`pred_${targetEpoch}`] = "PENDING";
        let candles = localCandles;
        
        if (!candles || candles.length < 50) {
            console.log("⚠️ Waiting for WebSocket to populate local candles... skipping prediction this tick.");
            return; 
        }

        const opens = candles.map(c => parseFloat(c[1]));
        const highs = candles.map(c => parseFloat(c[2]));
        const lows = candles.map(c => parseFloat(c[3]));
        const closes = candles.map(c => parseFloat(c[4]));
        const volumes = candles.map(c => parseFloat(c[5])); 
        
        // Parse Taker Buy Volume. If missing, estimate as 50% of total volume.
        const takerBuyVols = candles.map(c => c[6] !== undefined ? parseFloat(c[6]) : (parseFloat(c[5]) / 2));
        const currentClose = closes[closes.length - 1];

        // --- 1. VWAP CALCULATION (Crucial for 5m Charts) ---
        let cumVol = 0;
        let cumTypPriceVol = 0;
        // Calculate VWAP over the last 24 hours (approx 288 5m candles)
        const vwapLookback = Math.max(0, closes.length - 288); 
        for(let i = vwapLookback; i < closes.length; i++) {
            let typPrice = (highs[i] + lows[i] + closes[i]) / 3;
            cumVol += volumes[i];
            cumTypPriceVol += typPrice * volumes[i];
        }
        const vwap = cumTypPriceVol / cumVol;

        // --- 2. RSI CALCULATIONS (Maintained for frontend display & thresholds) ---
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

        // --- 3. EMAs & MACD (Maintained so the UI frontend doesn't break) ---
        const calculateEMAArray = (data, period) => {
            const k = 2 / (period + 1);
            let emaArray = [data[0]]; 
            for (let i = 1; i < data.length; i++) emaArray.push((data[i] * k) + (emaArray[i - 1] * (1 - k)));
            return emaArray;
        };

        const ema9 = calculateEMAArray(closes, 9)[closes.length - 1];
        const ema21 = calculateEMAArray(closes, 21)[closes.length - 1];
        const ema12Array = calculateEMAArray(closes, 12);
        const ema26Array = calculateEMAArray(closes, 26);
        const macdLineArray = ema12Array.map((v, i) => v - ema26Array[i]);
        const signalLineArray = calculateEMAArray(macdLineArray, 9);

        const currentMACD = macdLineArray[macdLineArray.length - 1];

        // --- 4. VOLUME DELTA ANALYSIS ---
        const currentVol = volumes[volumes.length - 1];
        const currentTakerBuy = takerBuyVols[takerBuyVols.length - 1];
        const currentTakerSell = currentVol - currentTakerBuy;
        const volDelta = currentTakerBuy - currentTakerSell;

        // --- 5. HISTORICAL TRENDS (Maintained to gauge contract momentum) ---
        let recentUps = 0, recentDowns = 0;
        const roundPromises = [];
        for(let i=1; i<=5; i++) roundPromises.push(contract.rounds(targetEpoch - i).catch(() => null));
        const pastRounds = await Promise.all(roundPromises);
        pastRounds.forEach(r => {
            if (r && r.oracleCalled) {
                const lp = parseFloat(ethers.utils.formatUnits(r.lockPrice, 8));
                const cp = parseFloat(ethers.utils.formatUnits(r.closePrice, 8));
                if (cp > lp) recentUps++; else if (cp < lp) recentDowns++;
            }
        });

        // --- 6. CATEGORY SCORING (Merged System) ---
        let upScore = 0, downScore = 0;
        let brainText = []; 
        let vwapScore = { up: 0, down: 0 }, volScore = { up: 0, down: 0 }, historyScore = { up: 0, down: 0 };

        // A. VWAP Structure (Is price above or below the institutional average?)
        const distFromVwap = ((currentClose - vwap) / vwap) * 100;
        if (currentClose > vwap) {
            vwapScore.up += 2.0;
            brainText.push(`Price is holding above VWAP (${vwap.toFixed(2)}), structural trend is bullish.`);
            if (distFromVwap > 1.5) { // Mean reversion trigger
                vwapScore.down += 1.5;
                brainText.push("Price is overextended from VWAP, risk of mean-reversion pullback.");
            }
        } else {
            vwapScore.down += 2.0;
            brainText.push(`Price is rejected below VWAP (${vwap.toFixed(2)}), structural trend is bearish.`);
            if (distFromVwap < -1.5) {
                vwapScore.up += 1.5;
                brainText.push("Price is heavily dragged below VWAP, risk of bounce.");
            }
        }

        // B. Order Flow / Volume Delta (Who is aggressively hitting the tape?)
        if (volDelta > (currentVol * 0.10)) { // Bulls control 10%+ of the net volume
            volScore.up += 2.5;
            brainText.push("Order Flow: Bulls are actively lifting the ask. Demand is aggressive.");
        } else if (volDelta < -(currentVol * 0.10)) {
            volScore.down += 2.5;
            brainText.push("Order Flow: Bears are actively hitting the bid. Supply is heavy.");
        } else {
            brainText.push("Order Flow: Neutral volume delta. Market is currently ranging.");
        }

        // C. RSI as a Filter
        if (rsi > 75 && volDelta < 0) {
            volScore.down += 2.0;
            brainText.push(`RSI is overbought (${rsi.toFixed(1)}) AND bears are stepping in. Reversal likely.`);
        } else if (rsi < 25 && volDelta > 0) {
            volScore.up += 2.0;
            brainText.push(`RSI is oversold (${rsi.toFixed(1)}) AND bulls are buying the dip. Bounce expected.`);
        }

        // D. History (Soft weight to maintain contract specific bias)
        if (recentUps >= 3) { historyScore.up += 1.0; }
        if (recentDowns >= 3) { historyScore.down += 1.0; }

        // --- FINAL CALCULATION ---
        upScore = vwapScore.up + volScore.up + historyScore.up;
        downScore = vwapScore.down + volScore.down + historyScore.down;

        let netScore = Math.abs(upScore - downScore);
        if (upScore === downScore) {
            brainText.push("Data is tied. Defaulting to VWAP alignment.");
            if (currentClose >= vwap) upScore += 0.5; else downScore += 0.5;
            netScore = Math.abs(upScore - downScore);
        }
        
        let currentPred = (upScore > downScore) ? "UP" : "DOWN";
        brainText.push(`Conclusion: Aggregate order flow and VWAP favors ${currentPred}.`);
        
        const ThoughtProcess = brainText.join(" ");
        
        // Realistic confidence scale tuned for the new scoring weights
        let numericConfidence = Math.min(85.0, 50 + (netScore * 5.0));
        let displayConf = numericConfidence.toFixed(1) + "%";
        
        // Retain dynamic Later Probability so the UI isn't static
        let laterUpProb = 50 + (ema9 > ema21 ? 10 : -10) + ((rsi - 50) * 0.4) + (recentUps > recentDowns ? 5 : -5);
        laterUpProb = Math.max(10, Math.min(90, laterUpProb));
        let laterPred = currentPred === "UP" ? "DOWN" : "UP"; // Cyclical assumption as requested
        let laterMajorityProb = Math.max(laterUpProb, 100 - laterUpProb).toFixed(1); 

        console.log(`🔥 Live Scan Update! Direction: ${currentPred} | current_conf: ${displayConf}`);
        
        // Send true MACD and RSI back to UI
        memoryStore[`best_${targetEpoch}`] = {
            current_pred: currentPred, current_conf: displayConf, numeric: numericConfidence,
            later_pred: laterPred, later_conf: laterMajorityProb + "%", rsi: rsi, macd: currentMACD, price: currentClose, thought_process: ThoughtProcess
        };

        await updateMarketStats(rsi, currentMACD, currentClose, currentPred, displayConf, laterPred, laterMajorityProb + "%", ThoughtProcess);
    } catch (e) {
        console.error("Brain Failed:", e);
    }
}

async function lockInPrediction(targetEpoch) {
    const bestData = memoryStore[`best_${targetEpoch}`];
    if (!bestData || bestData.numeric === -1) return;
    
    memoryStore[`locked_${targetEpoch}`] = true;
    console.log(`\n🔒 ROUND LIVE! Locking in best prediction for Epoch #${targetEpoch}: ${bestData.current_pred} (${bestData.current_conf})`);
    
    if (bestData.numeric >= 75.0) {
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
    
    // UPGRADE 4: Saving thought_process to Supabase for Whiff Tracking
    const { error } = await supabaseClient.from('prediction_logs').upsert([{ 
        epoch_id: targetEpoch, 
        predicted_side: bestData.current_pred, 
        result: 'PENDING',
        confidence: bestData.current_conf,
        is_locked: true,
        thought_process: bestData.thought_process // Saving the log to find out why it failed
    }], { onConflict: 'epoch_id' });
    
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
        if (closePrice === lockPrice) actualResult = "TIE";
        else actualResult = closePrice > lockPrice ? "UP" : "DOWN"; 
        
        const { data, error: fetchError } = await supabaseClient.from('prediction_logs').select('*').eq('epoch_id', epochToCheck).single();
        
        if (fetchError || !data) return;
        if (data.result !== 'PENDING') return;
        
        let resultStatus;
        if (actualResult === "TIE") resultStatus = "TIE";
        else if (data.predicted_side.startsWith("SKIP")) resultStatus = "SKIP/" + actualResult;
        else resultStatus = (data.predicted_side === actualResult) ? "WIN" : "LOSS"; 

        console.log(`\n⚖️ [Epoch ${epochToCheck}] Resolving... Result: ${resultStatus}`);
        
        // UPGRADE 5: Console Alert for Whiff Rate Auditing
        if (resultStatus === "LOSS" && data.thought_process) {
            console.log(`\n--- 🕵️ WHIFF AUDIT LOG [Epoch ${epochToCheck}] ---`);
            console.log(`Prediction: ${data.predicted_side} | Actual: ${actualResult}`);
            console.log(`What went wrong: ${data.thought_process}`);
            console.log(`----------------------------------------\n`);
        }

        const { error: updateError } = await supabaseClient.from('prediction_logs').update({ result: resultStatus }).eq('epoch_id', epochToCheck);
        if (updateError) { console.error(`❌ Supabase Update Error:`, updateError.message); return; }

        const { data: recentLogs } = await supabaseClient.from('prediction_logs').select('result, confidence').in('result', ['WIN', 'LOSS', 'SKIP/UP', 'SKIP/DOWN']).order('epoch_id', { ascending: false }).limit(15);
        if (recentLogs && recentLogs.length > 0) {
            const mixedWins = recentLogs.filter(l => l.result === 'WIN' || l.result === 'SKIP/UP').length;
            const trendLogs = recentLogs.filter(l => {
                const match = l.confidence.match(/(\d+(?:\.\d+)?)/);
                return match ? parseFloat(match[1]) >= 55.0 : false;
            });
            const trendWins = trendLogs.filter(l => l.result === 'WIN' || l.result === 'SKIP/UP').length;

            console.log(`📈 Mixed Market: ${((mixedWins / recentLogs.length) * 100).toFixed(1)}%`);
            console.log(`🚀 Trend Market: ${trendLogs.length > 0 ? ((trendWins / trendLogs.length) * 100).toFixed(1) : "0.0"}%`);
        }
    } catch(e) { 
        console.error("Result Verification Failed:", e);
    }
}

// Start bot and HTTP listener
startBot();
const http = require('http');
http.createServer((req, res) => { res.writeHead(200); res.end('Bot running'); }).listen(process.env.PORT || 3000);
