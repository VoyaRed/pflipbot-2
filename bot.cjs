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
    setTimeout(runLoop, 10000);
}

async function checkRound() {
    const currentEpoch = (await contract.currentEpoch()).toNumber();
    const nextEpoch = currentEpoch;
    const expiredEpoch = currentEpoch > 1 ? currentEpoch - 2 : 0;

    // 1. Predict the NEXT round
    const nextRoundData = await contract.rounds(nextEpoch);
    const lockTimestamp = nextRoundData.lockTimestamp.toNumber();
    const now = Math.floor(Date.now() / 1000);
    const secondsLeft = lockTimestamp - now;

    if (!memoryStore[`pred_${nextEpoch}`] && secondsLeft > 0 && secondsLeft <= 75) {
        console.log(`\n⏳ Epoch #${nextEpoch} is closing in ${secondsLeft}s. Analyzing market...`);
        await generatePrediction(nextEpoch);
    }

    // 2. Verify the EXPIRED round
    if (currentEpoch > lastEpochChecked && lastEpochChecked !== 0 && expiredEpoch > 0) {
        await verifyResult(expiredEpoch);
    }
    
    lastEpochChecked = currentEpoch;
}

async function generatePrediction(targetEpoch) {
    try {
        // 1. Access the variable you set in Render
        const apiKey = process.env.SCRAPINGBEE_KEY;
        
        // 2. Safety check: Ensure the key actually exists
        if (!apiKey) {
            console.error("❌ CRITICAL: SCRAPINGBEE_KEY environment variable is missing!");
            return; // Stop the function if we don't have a key
        }
        
        const targetUrl = "https://api.binance.com/api/v3/klines?symbol=BNBUSDT&interval=5m&limit=100";
        // Dynamically inject the API key here
        const scrapingBeeUrl = `https://app.scrapingbee.com/api/v1/?api_key=${apiKey}&url=${encodeURIComponent(targetUrl)}`;
        
        const options = {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept': 'application/json'
            }
        };

        // Use let instead of const so the retry loop can reassign it
        let candles = null;

        // 3. Fetch with Retry Logic (Try 3 times)
        for (let i = 0; i < 3; i++) {
            try {
                const res = await fetch(scrapingBeeUrl, options);
                if (res.ok) {
                    candles = await res.json();
                    break; // Success! Break out of the retry loop
                } else {
                    console.log(`ScrapingBee attempt ${i+1} failed with status: ${res.status}`);
                }
            } catch (e) {
                console.log(`ScrapingBee attempt ${i+1} caught error: ${e.message}`);
            }
            await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2s before retry
        }

        if (!candles) throw new Error("ScrapingBee API failed after 3 retries");

        // --- REST OF YOUR EXISTING LOGIC ---
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
            for (let i = 1; i < data.length; i++) emaArray.push((data[i] * k) + (emaArray[i - 1] * (1 - k)));
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

        await updateMarketStats(rsi, currentMACD, currentClose);

        // Volume
        const volSMA20 = volumes.slice(-20).reduce((a,b)=>a+b,0) / 20;
        const currentVol = volumes[volumes.length - 1];
        const hasVolumeSpike = currentVol > (volSMA20 * 1.5);

        // Scoring Logic
        let upScore = 0, downScore = 0;
        let bbWidth = (upperBB - lowerBB) / sma;
        let isChoppy = bbWidth < 0.0015;
        
        if (isChoppy) {
            if (hasVolumeSpike) {
                if (currentClose >= upperBB || rsi > 65) downScore += 4;
                if (currentClose <= lowerBB || rsi < 35) upScore += 4;
            }
        } else {
            if (ema9 > ema21) upScore += 1;
            if (ema9 < ema21) downScore += 1; 
            if (rsi > 68) downScore += 2;
            if (rsi < 32) upScore += 2; 
            if (currentClose > upperBB) downScore += 2;
            if (currentClose < lowerBB) upScore += 2;
            if (currentMACD > currentSignal && currentHist > prevHist) upScore += 2;
            if (currentMACD < currentSignal && currentHist < prevHist) downScore += 2;
            
            if (hasVolumeSpike) {
                const opens = parseFloat(candles[candles.length - 1][1]);
                if (currentClose > opens) upScore += 1; 
                if (currentClose < opens) downScore += 1;
            }
        }

        let prediction = "NONE";
        let winningScore = 0, tryScore = 0;
        let tryPred = (currentMACD > currentSignal || rsi < 45) ? "UP" : "DOWN";
        
        if (upScore > downScore && upScore >= 4) { 
            prediction = "UP"; 
            winningScore = upScore;
        } else if (downScore > upScore && downScore >= 4) { 
            prediction = "DOWN";
            winningScore = downScore; 
        } else { 
            prediction = "SKIP";
            tryScore = (tryPred === "UP") ? upScore : downScore; 
        }
        
        let finalConfidence = Math.min(99.1, 50 + (winningScore * 8.5)).toFixed(1) + "%";
        let tryConf = Math.min(98.5, 50 + (tryScore * 8.5)).toFixed(1) + "%";
        let displayConf = prediction === "SKIP" ? `Chop (Try: ${tryPred} ${tryConf})` : finalConfidence;

        memoryStore[`pred_${targetEpoch}`] = { pred: prediction, conf: displayConf };
        console.log(`🤖 [Epoch ${targetEpoch}] Decision: ${prediction} (Conf: ${displayConf})`);

        // Insert into original Supabase Database immediately
        const { error } = await supabaseClient.from('prediction_logs').insert([{ 
            epoch_id: targetEpoch, 
            predicted_side: prediction, 
            result: 'PENDING',
            confidence: displayConf
        }]);
        if (error) console.error("❌ Early Supabase insert error:", error);

        await updateMarketStats(rsi, currentMACD, currentClose);

    } catch (e) {
        console.error("Brain Failed:", e);
    }
}

async function updateMarketStats(rsi, macd, price) {
    const { error } = await supabaseClient
        .from('market_stats')
        .upsert([{ 
            id: 1, 
            rsi: rsi, 
            macd: macd, 
            price: price,
            updated_at: new Date().toISOString() 
        }]);
    if (error) console.error("Error updating stats:", error);
}

async function verifyResult(epochToCheck) {
    try {
        const round = await contract.rounds(epochToCheck);
        if (!round.oracleCalled) return; 

        const lockPrice = parseFloat(ethers.utils.formatUnits(round.lockPrice, 8));
        const closePrice = parseFloat(ethers.utils.formatUnits(round.closePrice, 8));
        const actualResult = closePrice > lockPrice ? "UP" : "DOWN";
        
        const { data } = await supabaseClient.from('prediction_logs').select('*').eq('epoch_id', epochToCheck).single();
        
        if (data) {
            const isWin = (data.predicted_side === "SKIP" || data.predicted_side.includes('SKIP')) ? "SKIPPED" : ((data.predicted_side === actualResult) ? "WIN" : "LOSS");
            console.log(`\n⚖️ [Epoch ${epochToCheck}] Resolving... Result: ${isWin}`);

            // Update Database with win/loss
            await supabaseClient.from('prediction_logs')
                .update({ result: isWin })
                .eq('epoch_id', epochToCheck);
        }
    } catch(e) { console.error("Result Verification Failed:", e); }
}

startBot();

// Keep-Alive for Render
const http = require('http');
http.createServer((req, res) => { res.writeHead(200); res.end('Bot running'); }).listen(process.env.PORT || 3000);
