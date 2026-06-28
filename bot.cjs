// bot.cjs - UpsideDownCake 24/7 Engine 🍰
const { ethers } = require('ethers');
const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');

// --- CONFIG ---
const SUPABASE_URL = 'https://tggqamigkruvhoqkyxrq.supabase.co';
const SUPABASE_KEY = 'sb_publishable_HVa5hO_AyTxmsI_iIgrDBA_jSenZuSD';
const supabaseClient = createClient(SUPABASE_URL, SUPABASE_KEY);
const PREDICT_ADDR = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA";
const ABI = [
    "function currentEpoch() view returns (uint256)", 
    "function rounds(uint256) view returns (uint256 epoch, uint256 startTimestamp, uint256 lockTimestamp, uint256 closeTimestamp, int256 lockPrice, int256 closePrice, uint256 lockOracleId, uint256 closeOracleId, uint256 totalAmount, uint256 bullAmount, uint256 bearAmount, uint256 rewardBaseCalAmount, uint256 rewardAmount, bool oracleCalled)"
];

// --- STATE VARIABLES ---
let provider, contract;
let lastEpochChecked = 0;
let memoryStore = {};
let lastScrapeTime = 0;
const SCRAPE_INTERVAL = 22000; // Only scrape every 22 seconds

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
    
    // --- 1. SCAN THE CURRENT ROUND ---
    const nextRoundData = await contract.rounds(currentEpoch);
    const lockTimestamp = nextRoundData.lockTimestamp.toNumber();
    const now = Math.floor(Date.now() / 1000);
    const secondsLeft = lockTimestamp - now;

    // 0. RESET AT START OF NEW ROUND
    // This ensures the database is wiped clean for the first 3 minutes
    if (secondsLeft > 102) {
        if (!memoryStore[`cleared_${currentEpoch}`]) {
            console.log(`⏳ Epoch #${currentEpoch} just started. Sleeping until 102s mark...`);
            await supabaseClient
                .from('market_stats')
                .update({ current_pred: 'NONE', current_conf: 'Calculating...' })
                .eq('id', 1);
            memoryStore[`cleared_${currentEpoch}`] = true;
        }
    }

    // 1. SCAN (Cache-aware & Stops after lock-in)
    // Notice the new "!memoryStore[`locked_${currentEpoch}`]" condition
    if (secondsLeft > 0 && secondsLeft <= 102 && !memoryStore[`locked_${currentEpoch}`]) {
        if (Date.now() - lastScrapeTime > SCRAPE_INTERVAL) {
            console.log(`📡 Scanning... Epoch #${currentEpoch} locks in ${secondsLeft}s`);
            await generatePrediction(currentEpoch);
            lastScrapeTime = Date.now();
        }
    }

     // 2. LOCK-IN at 30 seconds
    if (secondsLeft <= 30 && secondsLeft > 0 && memoryStore[`best_${currentEpoch}`] && !memoryStore[`locked_${currentEpoch}`]) {
        console.log(`⏱️30s Threshold hit! Locking in Epoch #${currentEpoch}`);
        await lockInPrediction(currentEpoch);
    }

    // --- 3. VERIFY PENDING EXPIRED ROUNDS ---
    // (Keep your existing verification code down here...)

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

async function updateMarketStats(rsi, macd, price, currentPred = "NONE", currentConf = "0%", laterPred = "NONE", laterConf = "0%") {
    const { error } = await supabaseClient
        .from('market_stats')
        .upsert([{ 
            id: 1, 
            rsi: rsi, 
            macd: macd, 
            price: price,
            current_pred: currentPred,
            current_conf: currentConf,
            later_pred: laterPred,
            later_conf: laterConf,
            updated_at: new Date().toISOString() 
        }]);
    if (error) console.error("Error updating stats:", error);
}

async function generatePrediction(targetEpoch) {
    try {
        memoryStore[`pred_${targetEpoch}`] = "PENDING";
        
        const apiKey = process.env.SCRAPINGBEE_KEY;
        if (!apiKey) {
            console.error("❌ CRITICAL: SCRAPINGBEE_KEY environment variable is missing!");
            return; 
        }
        
        const targetUrl = "https://api.binance.com/api/v3/klines?symbol=BNBUSDT&interval=5m&limit=100";
        const scrapingBeeUrl = `https://app.scrapingbee.com/api/v1/?api_key=${apiKey}&url=${encodeURIComponent(targetUrl)}`;
        const options = {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
        };
        
        let candles = null;
        for (let i = 0; i < 3; i++) {
            try {
                const res = await fetch(scrapingBeeUrl, options);
                if (res.ok) {
                    candles = await res.json();
                    break;
                } else {
                    console.log(`ScrapingBee attempt ${i+1} failed with status: ${res.status}`);
                }
            } catch (e) {
                console.log(`ScrapingBee attempt ${i+1} caught error: ${e.message}`);
            }
            await new Promise(resolve => setTimeout(resolve, 2000));
        }

        if (!candles) throw new Error("ScrapingBee API failed after 3 retries");

        const opens = candles.map(c => parseFloat(c[1]));
        const highs = candles.map(c => parseFloat(c[2]));
        const lows = candles.map(c => parseFloat(c[3]));
        const closes = candles.map(c => parseFloat(c[4]));
        const volumes = candles.map(c => parseFloat(c[5])); 
        const currentClose = closes[closes.length - 1];

        
        // RSI
        let gains = 0, losses = 0;
        for(let i = closes.length - 14; i < closes.length; i++) {
            const diff = closes[i] - closes[i-1];
            if (diff > 0) gains += diff; else losses += Math.abs(diff);
        }
        const avgGain = gains / 14; 
        const avgLoss = losses / 14;
        let rsi = 100;
        if (avgLoss !== 0) rsi = 100 - (100 / (1 + (avgGain / avgLoss)));
        else if (avgGain === 0) rsi = 50;

        // BB
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
            let emaArray = [data]; 
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
        
        // --- NEW: FETCH HISTORICAL EPOCH DATA FOR MICRO-TREND ---
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

        // Scoring Logic
        let upScore = 0, downScore = 0;

        // Apply Historical Micro-trend weight
        if (recentUps >= 3) upScore += 1;
        if (recentUps === 5) upScore += 1.5; // Strong sustained trend
        if (recentDowns >= 3) downScore += 1;
        if (recentDowns === 5) downScore += 1.5;

        // --- 1. EXTENDED CANDLE DATA (Needed for Wicks) ---
        
        // --- 2. PRICE ACTION (Wick Analysis) ---
        const prevOpen = opens[opens.length - 2];
        const prevClose = closes[closes.length - 2];
        const prevHigh = highs[highs.length - 2];
        const prevLow = lows[lows.length - 2];
        
        const upperWick = prevHigh - Math.max(prevOpen, prevClose);
        const lowerWick = Math.min(prevOpen, prevClose) - prevLow;
        const bodySize = Math.max(Math.abs(prevClose - prevOpen), 0.0001);

        // --- 3. MOMENTUM (Rate of Change) ---
        const roc3 = ((currentClose - closes[closes.length - 4]) / closes[closes.length - 4]) * 100;

        // --- 4. VOLATILITY (Average True Range Proxy) ---
        let trSum = 0;
        for (let i = closes.length - 14; i < closes.length; i++) {
            const highLow = highs[i] - lows[i];
            const highClose = Math.abs(highs[i] - closes[i-1]);
            const lowClose = Math.abs(lows[i] - closes[i-1]);
            trSum += Math.max(highLow, highClose, lowClose);
        }
        const atrPercentage = ((trSum / 14) / currentClose) * 100;

        // Existing Choppiness check
        let bbWidth = (upperBB - lowerBB) / sma;
        let isChoppy = bbWidth < 0.0015;

        // --- 5. THE DECISION ENGINE ---
        if (atrPercentage < 0.05 || isChoppy) {
            // CHOP MARKET LOGIC: Mean reversion without overinflating confidence
            // If we are below the SMA, guess UP (bouncing back up to the mean)
            if (currentClose < sma) {
                upScore += 1.0;
            } 
            // If we are above the SMA, guess DOWN (falling back to the mean)
            else if (currentClose > sma) {
                downScore += 1.0;
            }
            // By only adding 1.0 and skipping the heavy trend modifiers below,
            // the netScore stays low, preventing inflated confidence percentages.
            
        } else {
            // TRENDING MARKET LOGIC
            // Trend Alignment (MACD & EMA)
            if (ema9 > ema21) upScore += 1.5;
            if (ema9 < ema21) downScore += 1.5;
            if (currentMACD > currentSignal && currentHist > prevHist) upScore += 2;
            if (currentMACD < currentSignal && currentHist < prevHist) downScore += 2;

            // Momentum (ROC Velocity)
            if (roc3 > 0.15) upScore += 2.5;
            if (roc3 < -0.15) downScore += 2.5; 

            // Price Action Reversals (Wicks)
            if (upperWick > bodySize * 2) downScore += 3.5;
            if (lowerWick > bodySize * 2) upScore += 3.5;

            // Snap-Back Reversals (Extreme RSI + BB)
            if (currentClose > upperBB && rsi > 72) downScore += 4;
            if (currentClose < lowerBB && rsi < 28) upScore += 4;
        }
                // Determine Winner with functional SKIP logic
        let prediction;
        if (upScore === 0 && downScore === 0) {
            prediction = "SKIP";
        } else {
            prediction = (upScore >= downScore) ? "UP" : "DOWN";
        }
        
        // --- THE FIX: Use Net Score for true conviction ---
        // If UP is 8 and DOWN is 7, netScore is 1 (Low conviction).
        // If UP is 8 and DOWN is 0, netScore is 8 (High conviction).
        let netScore = Math.abs(upScore - downScore);
        
        // Scale the net score realistically. (Max theoretical net is ~15)
        // 15 * 3.25 = 48.75. Added to base 50 = max ~98.75%
        let numericConfidence = Math.min(99.1, 50 + (netScore * 3.25));
        let finalConfidence = numericConfidence.toFixed(1) + "%";
        
        let displayConf = finalConfidence;
        if (prediction === "SKIP") {
            let tryPred = (upScore >= downScore) ? "UP" : "DOWN";
            displayConf = `SKIP (${tryPred} ${finalConfidence})`;
        }

        // Calculate "Later" Likelihood
        let laterUpProb = 50 + (ema9 > ema21 ? 10 : -10) + ((rsi - 50) * 0.4) + (recentUps > recentDowns ? 5 : -5);
        laterUpProb = Math.max(10, Math.min(90, laterUpProb)); 
        let laterDownProb = 100 - laterUpProb;
        let laterPrediction = laterUpProb > 50 ? "UP" : "DOWN";
        let laterMajorityProb = Math.max(laterUpProb, laterDownProb).toFixed(1);

        // --- NEW: THE MEMORY VAULT (OVERWRITE MODE) ---
        console.log(`🔥 Live Scan Update! Conf: ${displayConf}`);
        
        // Always overwrite with the newest scan!
        memoryStore[`best_${targetEpoch}`] = {
            pred: prediction,
            conf: displayConf,
            numeric: (numericConfidence - 1),
            laterPrediction: laterPrediction,
            laterMajorityProb: laterMajorityProb,
            rsi: rsi,
            currentMACD: currentMACD,
            currentClose: currentClose
        };

        // Push the live scan straight to the UI!
        await updateMarketStats(rsi, currentMACD, currentClose, prediction, displayConf, laterPrediction, laterMajorityProb);

    } catch (e) {
        console.error("Brain Failed:", e);
    }
}

async function lockInPrediction(targetEpoch) {
    const bestData = memoryStore[`best_${targetEpoch}`];
    if (!bestData || bestData.numeric === -1) return;

    // Mark it as locked so we don't spam the database
    memoryStore[`locked_${targetEpoch}`] = true;

    console.log(`\n🔒 ROUND LIVE! Locking in best prediction for Epoch #${targetEpoch}: ${bestData.pred} (${bestData.conf})`);

    // 1. Webhook Alert (Only fires if confidence is 75% or higher)
    if (bestData.pred !== "SKIP" && bestData.numeric >= 75.0) {
        const webhookUrl = "https://discord.com/api/webhooks/1520463983998537800/T1xaGGZJ7YA_aw7JnbVKkyf9HwWta8D3W3VbuDhw5_vEiBtrqKqnzG37VIKH9WcwABx8";
        fetch(webhookUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
            username: "Cake Alert Bot 🍰",
            content: `🚨 **High Confidence Alert!** 🚨\nEpoch: #${targetEpoch}\nPrediction: **${bestData.pred}**\nConfidence: **${bestData.conf}**`
            })
        }).catch(err => console.error("Failed to send webhook:", err));
    }

    // 2. Insert into Supabase (This triggers your UI)
    const { error } = await supabaseClient.from('prediction_logs').insert([{ 
        epoch_id: targetEpoch, 
        predicted_side: bestData.pred, 
        result: 'PENDING',
        confidence: bestData.conf,
        is_locked: true,        // Keeps your lock-in animation logic
    }]);
    
    if (error) {
        console.error("❌ Early Supabase insert error:", error);
    }

    // 3. Update visual stats including the new LATER values, and reset the LIVE prediction
    await updateMarketStats(bestData.rsi, bestData.currentMACD, bestData.currentClose, "NONE", "Calculating...", bestData.laterPrediction, bestData.laterMajorityProb);
}


async function verifyResult(epochToCheck) {
    try {
        const round = await contract.rounds(epochToCheck);
        if (!round.oracleCalled) return; 

        const lockPrice = parseFloat(ethers.utils.formatUnits(round.lockPrice, 8));
        const closePrice = parseFloat(ethers.utils.formatUnits(round.closePrice, 8));
        const actualResult = closePrice > lockPrice ? "UP" : "DOWN"; 
        
        // Added error tracking to the fetch
        const { data, error: fetchError } = await supabaseClient
            .from('prediction_logs')
            .select('*')
            .eq('epoch_id', epochToCheck)
            .single();
            
        if (fetchError || !data) return;

        // Skip if this record somehow already got resolved
        if (data.result !== 'PENDING') return;

        let resultStatus;
        if (data.predicted_side === "SKIP") {
            resultStatus = "SKIP/" + actualResult;
        } else {
            resultStatus = (data.predicted_side === actualResult) ? "WIN" : "LOSS"; 
        }

        console.log(`\n⚖️ [Epoch ${epochToCheck}] Resolving... Result: ${resultStatus}`);
        
        // CRITICAL FIX: Removed 'close_price' so Supabase doesn't reject the update
        const { error: updateError } = await supabaseClient
            .from('prediction_logs')
            .update({ result: resultStatus })
            .eq('epoch_id', epochToCheck);

        if (updateError) {
            console.error(`❌ Supabase Update Error for Epoch ${epochToCheck}:`, updateError.message);
            return;
        }

        // Improved: Added .limit(15) directly to the database query for efficiency
        const { data: recentLogs } = await supabaseClient
            .from('prediction_logs')
            .select('result, confidence')
            .in('result', ['WIN', 'LOSS', 'SKIP/UP', 'SKIP/DOWN'])
            .order('epoch_id', { ascending: false })
            .limit(15);

                if (recentLogs && recentLogs.length > 0) {
            // 1. MIXED MARKET: The Average of EVERYTHING (Total performance)
            const mixedWins = recentLogs.filter(l => l.result === 'WIN' || l.result === 'SKIP/UP').length;
            const mixedRate = ((mixedWins / recentLogs.length) * 100).toFixed(1);

            // 2. TREND MARKET: High Conviction Only (Confidence > 55%)
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
startBot();

const http = require('http');
http.createServer((req, res) => { res.writeHead(200); res.end('Bot running'); }).listen(process.env.PORT || 3000);
