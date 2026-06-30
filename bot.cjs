// bot.cjs - UpsideDownCake 24/7 Engine 🍰
const { ethers } = require('ethers');
const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
const ccxt = require('ccxt'); 

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
    try {
        const fastest = await findFastestRPC();
        provider = fastest.provider;
        contract = fastest.contract;
        console.log("✅ Connected to BSC successfully.");
        runLoop();
    } catch (error) {
        console.error("❌ Initialization failed, retrying in 10s...", error);
        setTimeout(startBot, 10000);
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

async function checkRound() {
    const currentEpoch = (await contract.currentEpoch()).toNumber();
    
    // Garbage Collection for old epochs
    const staleEpoch = currentEpoch - 10;
    Object.keys(memoryStore).forEach(key => {
        if (key.includes(`_${staleEpoch}`)) {
            delete memoryStore[key];
        }
    });

    // --- 1. SCAN THE CURRENT ROUND ---
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
            console.warn(`⚠️ Failsafe triggered: No prediction generated for #${currentEpoch}. Forcing fallback direction prediction.`);
            memoryStore[`best_${currentEpoch}`] = {
                current_pred: "NONE", // Default fallback direction
                current_conf: "binance trippin 1sec",
                numeric: 50,
                later_pred: "NONE",
                later_conf: "0%",
                rsi: 0,
                macd: 0,
                price: 0,
                thought_process: "Emergency Fallback: Binance data retrieval timed out before lock."
            };
        }
        
        console.log(`⏱️ Locking in Epoch #${currentEpoch}`);
        await lockInPrediction(currentEpoch);
    }

    // --- 3. VERIFY PENDING EXPIRED ROUNDS ---
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
        
        let candles = null;
        
        try {
            candles = await exchange.fetchOHLCV('BNB/USDT', '5m', undefined, 1000);
            if (Array.isArray(candles) && candles.length >= 50) {
                console.log("✅ Binance market data successfully retrieved.");
            } else {
                throw new Error("Insufficient candles returned from Binance.");
            }
        } catch (e) {
            console.error("Direct connection failed:", e.message);
            throw e; 
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
            if (avgLoss !== 0) {
                currentRsi = 100 - (100 / (1 + (avgGain / avgLoss)));
            } else if (avgGain === 0) {
                currentRsi = 0;
            }
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

        // EMA Helper
        const calculateEMAArray = (data, period) => {
            const k = 2 / (period + 1);
            let emaArray = [data[0]]; 
            for (let i = 1; i < data.length; i++) {
                emaArray.push((data[i] * k) + (emaArray[i - 1] * (1 - k)));
            }
            return emaArray;
        };

        // EMA & MACD
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
        const hasVolumeSpike = currentVol > (volSMA20 * 1.5);

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

        // --- SCORING MATRIX ---
        let upScore = 0, downScore = 0;
        let brainText = []; 

        if (rsiSlope > 0.5) brainText.push("RSI is aggressively rising; momentum is strong.");
        if (rsiSlope < -0.5) brainText.push("RSI is skyrocketing downward; bearish momentum is accelerating.");
        if (rsiAcceleration < 0 && rsi > 60) brainText.push("Warning: RSI rise is slowing down; potential overbought reversal.");
        
        if (recentUps >= 3) { upScore += 1; brainText.push("Recent historical rounds lean bullish."); }
        if (recentUps === 5) upScore += 1.5;
        if (recentDowns >= 3) { downScore += 1; brainText.push("Recent historical rounds lean bearish."); }
        if (recentDowns === 5) downScore += 1.5;

        const prevOpen = opens[opens.length - 2];
        const prevClose = closes[closes.length - 2];
        const prevHigh = highs[highs.length - 2];
        const prevLow = lows[lows.length - 2];
        
        const upperWick = prevHigh - Math.max(prevOpen, prevClose);
        const lowerWick = Math.min(prevOpen, prevClose) - prevLow;
        const bodySize = Math.max(Math.abs(prevClose - prevOpen), 0.0001);
        const roc3 = ((currentClose - closes[closes.length - 4]) / closes[closes.length - 4]) * 100;
        
        let trSum = 0;
        for (let i = closes.length - 14; i < closes.length; i++) {
            const highLow = highs[i] - lows[i];
            const highClose = Math.abs(highs[i] - closes[i-1]);
            const lowClose = Math.abs(lows[i] - closes[i-1]);
            trSum += Math.max(highLow, highClose, lowClose);
        }
        const atrPercentage = ((trSum / 14) / currentClose) * 100;
        
        let bbWidth = (upperBB - lowerBB) / sma;
        let isChoppy = bbWidth < 0.0015;

        if (atrPercentage < 0.05 || isChoppy) {
            brainText.push("Volatility is extremely low, executing a Mean-Reversion selection.");
            if (currentClose < sma) {
                upScore += 2.5;
                brainText.push("Price is lagging beneath the Moving Average, forcing a counter-structural upcall.");
                if (rsi < 40) { upScore += 1.5; brainText.push(`RSI is low at ${rsi.toFixed(1)}, optimizing safety threshold.`); }
            } else if (currentClose > sma) {
                downScore += 2.5;
                brainText.push("Price is floating above the Moving Average, forcing a counter-structural downcall.");
                if (rsi > 60) { downScore += 1.5; brainText.push(`RSI is elevated at ${rsi.toFixed(1)}, optimizing resistance threshold.`); }
            }
        } else {
            brainText.push("Market is showing structural momentum, engaging Trend analysis.");
            if (ema9 > ema21) { upScore += 2.0; brainText.push("Fast EMA(9) leads Slow EMA(21) (Bullish configuration)."); }
            if (ema9 < ema21) { downScore += 2.0; brainText.push("Fast EMA(9) trails Slow EMA(21) (Bearish configuration)."); }
            
            if (currentMACD > currentSignal && currentHist > prevHist) { upScore += 2.5; brainText.push("MACD histogram is expanding upward, displaying strong structural expansion."); }
            if (currentMACD < currentSignal && currentHist < prevHist) { downScore += 2.5; brainText.push("MACD histogram is expanding downward, displaying strong structural compression."); }
            
            if (roc3 > 0.15) upScore += 3.0;
            if (roc3 < -0.15) downScore += 3.0; 

            if (upperWick > bodySize * 2) { downScore += 3.5; brainText.push("Spotted a long upper wick on the previous candle, predicting supply overhead."); }
            if (lowerWick > bodySize * 2) { upScore += 3.5; brainText.push("Spotted a long lower wick on the previous candle, predicting clear demand protection."); }

            if (currentClose > upperBB && rsi > 72) { downScore += 4.5; brainText.push("Price pierced the Upper Bollinger Band with overextended RSI. Overrides forced a downward bias."); }
            if (currentClose < lowerBB && rsi < 28) { upScore += 4.5; brainText.push("Price pierced the Lower Bollinger Band with crushed RSI. Overrides forced an upward bias."); }
        }

        let netScore = Math.abs(upScore - downScore);
        if (isNaN(netScore)) netScore = 0;

        // Tie-breaker initialization
        if (upScore === downScore) {
            brainText.push("Data is perfectly tied. Using directional EMA trend alignment as the structural tie-breaker.");
            if (ema9 >= ema21) { upScore += 1.5; } else { downScore += 1.5; }
            netScore = Math.abs(upScore - downScore);
        }
        
        // FIX: The prediction is now permanently tied to the exact direction the bot favors.
        let currentPred = (upScore > downScore) ? "UP" : "DOWN";
        brainText.push(`Conclusion: The aggregate weight of the technical data firmly favors ${currentPred}.`);
        
        const ThoughtProcess = brainText.join(" ");
        let numericConfidence = Math.min(99.1, 55 + (netScore * 4.0));
        let finalConfidence = numericConfidence.toFixed(1) + "%";
        let displayConf = finalConfidence;

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

        console.log("DEBUG: MACD value being sent:", currentMACD);
        console.log("DEBUG: RSI value being sent:", rsi);
        
        await updateMarketStats(rsi, currentMACD, currentClose, currentPred, displayConf, laterPred, laterMajorityProb, ThoughtProcess);
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
            resultStatus = "SKIP/" + actualResult; // Legacy handler for old rows
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
