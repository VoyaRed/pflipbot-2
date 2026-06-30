// bot.cjs - UpsideDownCake 24/7 Engine 🍰 (Hyper-Optimized 5m Scalper + Time-Machine Failover)
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

const exchange = new ccxt.binance({
    enableRateLimit: true, 
    options: { defaultType: 'spot' }
});

// --- STATE VARIABLES ---
let provider, contract;
let lastEpochChecked = 0;
let memoryStore = {};
let lastScrapeTime = 0;
const SCRAPE_INTERVAL = 18000; 

let localCandles = [];
let isInitialFetchDone = false;
let binanceSleepUntil = 0; 
let isTestModeActive = false; 

// --- TIME MACHINE VARIABLES ---
let historicalTestSet = [];
let testModePointer = 1000; 

let consecutiveLoopErrors = 0; 
let startBotErrorCount = 0;

function startCandleStream() {
    const wsUrl = 'wss://stream.binance.com:9443/ws/bnbusdt@kline_5m';
    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
        console.log("🔌 Live WebSocket connected to Binance (BNB/USDT 5m Scalper Enabled)");
    });

    ws.on('message', (data) => {
        if (isTestModeActive) return; // Ignore live stream when time-traveling
        
        const message = JSON.parse(data);
        const kline = message.k; 

        if (Math.random() < 0.01) { 
            console.log(`✅ Heartbeat: Received tick for BNB/USDT. Current Close: ${kline.c}`);
        }
        
        const candle = [
            kline.t, parseFloat(kline.o), parseFloat(kline.h), parseFloat(kline.l),
            parseFloat(kline.c), parseFloat(kline.v), parseFloat(kline.V || 0) 
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
        if (!isTestModeActive) {
            console.log("🔌 WebSocket disconnected. Reconnecting in 5 seconds...");
            setTimeout(startCandleStream, 5000);
        }
    });
}

// --- NEW: TIME MACHINE DATA LOADER ---
async function initializeTimeMachine() {
    try {
        console.log("🌐 Booting Time Machine... Fetching 1500 historical testnet candles...");
        const res = await fetch('https://testnet.binance.vision/api/v3/klines?symbol=BNBUSDT&interval=5m&limit=1500');
        const data = await res.json();
        
        historicalTestSet = data.map(kline => [
            kline[0], parseFloat(kline[1]), parseFloat(kline[2]), parseFloat(kline[3]), 
            parseFloat(kline[4]), parseFloat(kline[5]), parseFloat(kline[9]) 
        ]);
        
        testModePointer = 1000; // Start looking at the 1000th candle
        console.log("⏳ Time Machine Ready. We have 500 future epochs known.");
    } catch(e) {
        console.error("Time Machine API fallback failed:", e);
    }
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
        } catch (e) {}
    }
    throw new Error("All RPC nodes failed.");
}

async function startBot() {
    console.log("🍰 UpsideDownCake 24/7 Engine Starting...");

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
        startBotErrorCount = 0; 

        console.log("✅ Connected to BSC successfully.");
        runLoop();
    } catch (error) {
        if (error.message.includes('418') || error.message.includes('429')) {
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
            isTestModeActive = true; 

            console.error(`🚨 Binance Ban/Rate Limit Detected! -> Source: ${penaltySource} -> Activating Time Machine for ${sleepMinutes} minutes.`);
            
            // Connect to Time Machine DB and keep running loop alongside live contract
            const fastest = await findFastestRPC();
            provider = fastest.provider;
            contract = fastest.contract;
            await initializeTimeMachine();
            runLoop(); 
            
        } else {
            startBotErrorCount++;
            const fallbackDelayMs = Math.min(10000 * Math.pow(2, startBotErrorCount), 300000); 
            console.error(`❌ Initialization failed (Error: ${error.message}). Applying backoff penalty. Retrying in ${fallbackDelayMs / 1000}s...`);
            setTimeout(startBot, fallbackDelayMs);
        }
    }
}

async function runLoop() {
    try {
        if (isTestModeActive && Date.now() >= binanceSleepUntil) {
            console.log("🌞 Rate limit lifted. Returning to present timeline...");
            isTestModeActive = false;
            localCandles = [];
            startBot(); 
            return;
        }

        await checkRound();
        consecutiveLoopErrors = 0; 
        setTimeout(runLoop, 2000); 
    } catch (error) {
        consecutiveLoopErrors++;
        const delayMs = Math.min(2000 * Math.pow(2, consecutiveLoopErrors), 60000);
        console.warn(`⚠️ RPC/Network hiccup detected. Retrying in ${delayMs / 1000} seconds...`);
        setTimeout(runLoop, delayMs);
    }
}

async function checkRound() {
    const currentEpoch = (await contract.currentEpoch()).toNumber();

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
            
            // Advance Time Machine
            if (isTestModeActive) {
                testModePointer++; 
                if (testModePointer >= historicalTestSet.length - 1) testModePointer = 1000; // Loop safety
                console.log(`⏰ Time Machine advanced. Frame: ${testModePointer}/1500`);
            }

            let lastAnalysis = "";
            const lastData = memoryStore[`best_${currentEpoch - 1}`];

            if (lastData && lastData.thought_process) {
                lastAnalysis = `\n\n--- LAST MARKET ANALYSIS ---\n${lastData.thought_process}`;
            }

            await supabaseClient.from('market_stats').update({ 
                current_pred: 'NONE', current_conf: 'Calculating...',
                test_mode: isTestModeActive, 
                thought_process: `Waiting for initial 3-minute market settling...${lastAnalysis}`
            }).eq('id', 1);

            memoryStore[`cleared_${currentEpoch}`] = true;
        }
    }

    if (secondsLeft > 0 && secondsLeft <= 102 && !memoryStore[`locked_${currentEpoch}`]) {
        if (Date.now() - lastScrapeTime > SCRAPE_INTERVAL) {
            await generatePrediction(currentEpoch);
            lastScrapeTime = Date.now();
        }
    }

    if (secondsLeft <= 15 && secondsLeft > 0 && !memoryStore[`locked_${currentEpoch}`]) {
        if (!memoryStore[`best_${currentEpoch}`]) {
            memoryStore[`best_${currentEpoch}`] = {
                current_pred: "NONE", current_conf: "binance trippin 1sec", numeric: 50,
                later_pred: "NONE", later_conf: "0%", rsi: 0, macd: 0, price: 0,
                thought_process: "Emergency Fallback: Binance data retrieval timed out before lock."
            };
        }
        await lockInPrediction(currentEpoch);
    }

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
        test_mode: isTestModeActive,
        thought_process: thoughtProcess, updated_at: new Date().toISOString() 
    }]);

    if (error) console.error("Error updating stats:", error);
}

// *** ORIGINAL CALCULATION LOGIC ***
async function generatePrediction(targetEpoch) {
    try {
        memoryStore[`pred_${targetEpoch}`] = "PENDING";

        // TIME MACHINE DATA OVERRIDE
        if (isTestModeActive && historicalTestSet.length > 0) {
            localCandles = historicalTestSet.slice(testModePointer - 1000, testModePointer);
        }

        let candles = localCandles;
        if (!candles || candles.length < 50) return; 

        const opens = candles.map(c => parseFloat(c[1]));
        const highs = candles.map(c => parseFloat(c[2]));
        const lows = candles.map(c => parseFloat(c[3]));
        const closes = candles.map(c => parseFloat(c[4]));
        const volumes = candles.map(c => parseFloat(c[5])); 
        const takerBuyVols = candles.map(c => c[6] !== undefined ? parseFloat(c[6]) : (parseFloat(c[5]) / 2));
        const currentClose = closes[closes.length - 1];

        let tr = [];
        for (let i = 1; i < closes.length; i++) {
            tr.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
        }
        let atr = tr.slice(tr.length - 14).reduce((a, b) => a + b, 0) / 14;

        let cumVol = 0; let cumTypPriceVol = 0;
        const vwapLookback = Math.max(0, closes.length - 288);
        for(let i = vwapLookback; i < closes.length; i++) {
            let typPrice = (highs[i] + lows[i] + closes[i]) / 3;
            cumVol += volumes[i]; cumTypPriceVol += typPrice * volumes[i];
        }
        const vwap = cumTypPriceVol / cumVol;

        let recentUps = 0, recentDowns = 0;
        for(let i = closes.length - 3; i < closes.length; i++) {
            if(closes[i] > opens[i]) recentUps++; else if(closes[i] < opens[i]) recentDowns++;
        }

        let gains = [], losses = [];
        for (let i = 1; i < closes.length; i++) {
            let diff = closes[i] - closes[i - 1];
            gains.push(diff > 0 ? diff : 0); losses.push(diff < 0 ? Math.abs(diff) : 0);
        }
        let avgGain = gains.slice(gains.length - 14).reduce((a, b) => a + b, 0) / 14;
        let avgLoss = losses.slice(losses.length - 14).reduce((a, b) => a + b, 0) / 14;
        let rsi = avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss)));

        const calculateEMAArray = (data, period) => {
            const k = 2 / (period + 1); let emaArray = [data[0]]; 
            for (let i = 1; i < data.length; i++) emaArray.push((data[i] * k) + (emaArray[i - 1] * (1 - k))); return emaArray;
        };
        const currentMACD = calculateEMAArray(closes, 12)[closes.length - 1] - calculateEMAArray(closes, 26)[closes.length - 1];

        let multiCandleDelta = 0; let totalWindowVol = 0;
        for(let i = closes.length - 3; i < closes.length; i++) {
            let cv = volumes[i]; let ctb = takerBuyVols[i]; let cts = cv - ctb;
            multiCandleDelta += (ctb - cts); totalWindowVol += cv;
        }
        const deltaPercentage = (multiCandleDelta / totalWindowVol) * 100;

        let upScore = 0, downScore = 0; let brainText = [];
        
        if (isTestModeActive) {
            brainText.push("🧪 **TIME MACHINE ACTIVE**: Utilizing historical market vectors to bypass rate limits.");
        }

        const distFromVwap = ((currentClose - vwap) / vwap) * 100;
        if (currentClose > vwap) {
            upScore += 3.0; brainText.push(`Price is securely above institutional VWAP (${vwap.toFixed(2)}). Structure is bullish.`);
            if (distFromVwap > 1.2) { downScore += 1.5; brainText.push("Volatility expansion away from structural VWAP implies micro reversion risk."); }
        } else {
            downScore += 3.0; brainText.push(`Price is pinned under structural VWAP (${vwap.toFixed(2)}). Bearish grip active.`);
            if (distFromVwap < -1.2) { upScore += 1.5; brainText.push("Extended micro-expansion below structural baseline suggests dynamic bounce risk."); }
        }

        if (deltaPercentage > 8.0) { upScore += 3.5; brainText.push(`Order Flow Audit: Bulls dominating execution tape (+${deltaPercentage.toFixed(1)}% delta).`); } 
        else if (deltaPercentage < -8.0) { downScore += 3.5; brainText.push(`Order Flow Audit: Bears flooding ask lines (-${deltaPercentage.toFixed(1)}% delta).`); } 

        if (recentUps >= 2 && currentClose > opens[opens.length - 1]) upScore += 1.5;
        if (recentDowns >= 2 && currentClose < opens[opens.length - 1]) downScore += 1.5;
        
        const pointSpread = Math.abs(currentClose - opens[opens.length - 1]);
        if (pointSpread < (atr * 0.25)) { upScore -= 1.0; downScore -= 1.0; brainText.push("Volatility Compression Warning: Local compression under safe ATR scaling parameters."); }

        if (rsi > 78) { upScore -= 2.0; downScore += 1.0; brainText.push(`Velocity Threshold Warning: RSI Overextended (${rsi.toFixed(1)}).`); }
        if (rsi < 22) { downScore -= 2.0; upScore += 1.0; brainText.push(`Velocity Threshold Warning: RSI Compressed (${rsi.toFixed(1)}).`); }

        let currentPred = "NONE"; let displayConf = "0.0%"; let numericConfidence = 50;
        let netScore = Math.abs(upScore - downScore);
        
        if (netScore < 2.5 || (rsi > 42 && rsi < 58 && Math.abs(deltaPercentage) < 5.0)) {
            currentPred = "SKIP"; displayConf = upScore >= downScore ? "Try: UP" : "Try: DOWN"; numericConfidence = 49.0;
        } else {
            currentPred = (upScore > downScore) ? "UP" : "DOWN"; numericConfidence = Math.min(88.0, 52 + (netScore * 4.5));
            displayConf = numericConfidence.toFixed(1) + "%";
        }

        // --- TIME MACHINE FUTURE INSIGHT LOGIC ---
        let laterPred = currentPred === "UP" ? "DOWN" : "UP";
        let laterMajorityProb = (50 + (netScore * 2.0)).toFixed(1) + "%";
        let futurePrice = 0;
        
        if (isTestModeActive && historicalTestSet[testModePointer]) {
            // Peek at the very next candle in our local array to see the absolute future!
            futurePrice = historicalTestSet[testModePointer][4];
            let actualFutureResult = futurePrice > currentClose ? "UP" : "DOWN";
            let fakeOutCome = currentPred === actualFutureResult ? "WIN" : "LOSS";
            
            brainText.push(`\n\n🔮 **KNOWN OUTCOME**: The close price will be exactly $${futurePrice.toFixed(4)}. Current vector guarantees a ${fakeOutCome}!`);
            
            // Sneak the future data into laterMajorityProb so the UI can rip it out and display it over the Oracle Price
            laterMajorityProb = `$${futurePrice.toFixed(4)} (Bot ${fakeOutCome})`;
        }

        const ThoughtProcess = brainText.join(" ");

        memoryStore[`best_${targetEpoch}`] = {
            current_pred: currentPred, current_conf: displayConf, numeric: numericConfidence,
            later_pred: laterPred, later_conf: laterMajorityProb, rsi: rsi, macd: currentMACD, 
            price: currentClose, futurePrice: futurePrice, thought_process: ThoughtProcess
        };

        await updateMarketStats(rsi, currentMACD, currentClose, currentPred, displayConf, laterPred, laterMajorityProb, ThoughtProcess);
    } catch (e) {
        console.error("Brain Execution Failed:", e);
    }
}

async function lockInPrediction(targetEpoch) {
    const bestData = memoryStore[`best_${targetEpoch}`];
    if (!bestData) return;
    
    memoryStore[`locked_${targetEpoch}`] = true;

    if (bestData.numeric >= 76.0 && bestData.current_pred !== "SKIP" && !isTestModeActive) {
        const webhookUrl = "https://discord.com/api/webhooks/1520463983998537800/T1xaGGZJ7YA_aw7JnbVKkyf9HwWta8D3W3VbuDhw5_vEiBtrqKqnzG37VIKH9WcwABx8";
        fetch(webhookUrl, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username: "Cake Alert Bot 🍰",
                content: `🚨 **High Conviction 5m Signal** 🚨\nEpoch: #${targetEpoch}\nTarget Path: **${bestData.current_pred}**\nEngine Conviction: **${bestData.current_conf}**`
            })
        }).catch(err => console.error("Discord Webhook Failed:", err));
    }
    
    const { error } = await supabaseClient.from('prediction_logs').upsert([{ 
        epoch_id: targetEpoch, 
        predicted_side: bestData.current_pred, 
        result: 'PENDING',
        confidence: bestData.current_conf,
        is_locked: true,
        thought_process: bestData.thought_process 
    }], { onConflict: 'epoch_id' });

    if (error) console.error("❌ Early Supabase Insert Error:", error);
    await updateMarketStats(bestData.rsi, bestData.macd, bestData.price, "NONE", "Calculating...", bestData.later_pred, bestData.later_conf, bestData.thought_process);
}

async function verifyResult(epochToCheck) {
    try {
        const round = await contract.rounds(epochToCheck);
        const { data, error: fetchError } = await supabaseClient.from('prediction_logs').select('*').eq('epoch_id', epochToCheck).single();
        if (fetchError || !data || data.result !== 'PENDING') return;

        let actualResult;
        let resultStatus;

        // --- TIME MACHINE VERIFICATION HIJACK ---
        // If we test with historical data against a live contract, it will record fake losses on the real ledger.
        // We bypass the smart contract Oracle here and use the future price we grabbed out of our time machine array.
        if (isTestModeActive) {
            const mockData = memoryStore[`best_${epochToCheck}`];
            if (!mockData) return;
            
            actualResult = mockData.futurePrice > mockData.price ? "UP" : "DOWN";
            resultStatus = data.predicted_side === "SKIP" ? `SKIP/${actualResult}` : (data.predicted_side === actualResult ? "WIN" : "LOSS");
            
            // Prepend TEST- so the UI knows not to mix this into the real live Win Rate calculations
            resultStatus = `TEST-${resultStatus}`; 
            console.log(`\n⚖️ [Epoch ${epochToCheck}] TIME MACHINE SETTLED. Simulated Result: ${resultStatus}`);
        } else {
            // NORMAL LIVE VERIFICATION
            if (!round.oracleCalled) return; 
            const lockPrice = parseFloat(ethers.utils.formatUnits(round.lockPrice, 8));
            const closePrice = parseFloat(ethers.utils.formatUnits(round.closePrice, 8));
            
            if (closePrice === lockPrice) actualResult = "TIE";
            else actualResult = closePrice > lockPrice ? "UP" : "DOWN";

            if (actualResult === "TIE") resultStatus = "TIE";
            else if (data.predicted_side === "SKIP") resultStatus = "SKIP/" + actualResult;
            else resultStatus = (data.predicted_side === actualResult) ? "WIN" : "LOSS"; 
            
            console.log(`\n⚖️ [Epoch ${epochToCheck}] Round Complete. Final Settlement Status: ${resultStatus}`);
        }

        await supabaseClient.from('prediction_logs').update({ result: resultStatus }).eq('epoch_id', epochToCheck);

        // Standard Console Analytics
        const { data: recentLogs } = await supabaseClient.from('prediction_logs').select('result, confidence').in('result', ['WIN', 'LOSS', 'SKIP/UP', 'SKIP/DOWN']).order('epoch_id', { ascending: false }).limit(15);
        if (recentLogs && recentLogs.length > 0) {
            const mixedWins = recentLogs.filter(l => l.result === 'WIN' || l.result === 'SKIP/UP').length;
            const trendLogs = recentLogs.filter(l => { const match = l.confidence.match(/(\d+(?:\.\d+)?)/); return match ? parseFloat(match[1]) >= 55.0 : false; });
            const trendWins = trendLogs.filter(l => l.result === 'WIN' || l.result === 'SKIP/UP').length;
            console.log(`📈 Mixed Win Engine Rate: ${((mixedWins / recentLogs.length) * 100).toFixed(1)}%`);
        }
    } catch(e) { 
        console.error("Result Verification Failed:", e);
    }
}

startBot();
const http = require('http');
http.createServer((req, res) => { res.writeHead(200); res.end('Bot active'); }).listen(process.env.PORT || 3000);
