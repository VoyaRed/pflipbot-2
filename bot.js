// bot.js - UpsideDownCake 24/7 Engine 🍰
const { ethers } = require('ethers');
const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
const res = await fetch(url, {
    headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
});

// --- SUPABASE CONFIG ---
const SUPABASE_URL = 'https://tggqamigkruvhoqkyxrq.supabase.co';
const SUPABASE_KEY = 'sb_publishable_HVa5hO_AyTxmsI_iIgrDBA_jSenZuSD';
const supabaseClient = createClient(SUPABASE_URL, SUPABASE_KEY);

// --- BLOCKCHAIN CONFIG ---
const PREDICT_ADDR = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA";
const ABI = [
    "function currentEpoch() view returns (uint256)", 
    "function rounds(uint256) view returns (uint256 epoch, uint256 startTimestamp, uint256 lockTimestamp, uint256 closeTimestamp, int256 lockPrice, int256 closePrice, uint256 lockOracleId, uint256 closeOracleId, uint256 totalAmount, uint256 bullAmount, uint256 bearAmount, uint256 rewardBaseCalAmount, uint256 rewardAmount, bool oracleCalled)"
];

const CHAINLINK_BNB_USD = "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE";
const CHAINLINK_ABI = [
    "function latestRoundData() external view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)"
];

// --- STATE VARIABLES ---
let provider, contract;
let lastEpochChecked = 0;
let memoryStore = {}; // Replaces localStorage for the backend

// --- CORE FUNCTIONS ---

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
    // Loop every 10 seconds
    setTimeout(runLoop, 10000); 
}

async function checkRound() {
    const currentEpoch = (await contract.currentEpoch()).toNumber();
    const nextEpoch = currentEpoch;
    const expiredEpoch = currentEpoch > 1 ? currentEpoch - 2 : 0;

    // --- 1. Check if we need to predict the NEXT round ---
    const nextRoundData = await contract.rounds(nextEpoch);
    const lockTimestamp = nextRoundData.lockTimestamp.toNumber();
    const now = Math.floor(Date.now() / 1000);
    const secondsLeft = lockTimestamp - now;

    if (!memoryStore[`pred_${nextEpoch}`] && secondsLeft > 0 && secondsLeft <= 75) {
        console.log(`\n⏳ Epoch #${nextEpoch} is closing in ${secondsLeft}s. Analyzing market...`);
        await generatePrediction(nextEpoch);
    }

    // --- 2. Check if we need to verify the EXPIRED round ---
    if (currentEpoch > lastEpochChecked && lastEpochChecked !== 0 && expiredEpoch > 0) {
        await verifyResult(expiredEpoch);
    }
    
    lastEpochChecked = currentEpoch;
}

// --- THE BRAIN ---
async function generatePrediction(targetEpoch) {
    try {
        // 1. Get current Oracle bet price
        const predOracleContract = new ethers.Contract(CHAINLINK_BNB_USD, CHAINLINK_ABI, provider);
        const [, price] = await predOracleContract.latestRoundData();
        const oraclePrice = parseFloat(ethers.utils.formatUnits(price, 8)) - 1; 
        const currentBetPrice = oraclePrice.toFixed(4); 

        // 2. Fetch Klines (Removed CORS proxies, hitting Binance directly)
        const endpoints = [
            "https://api.binance.com/api/v3/klines?symbol=BNBUSDT&interval=5m&limit=100",
            "https://api1.binance.com/api/v3/klines?symbol=BNBUSDT&interval=5m&limit=100"
        ];

        let candles;
        for (let url of endpoints) {
            try {
                const res = await fetch(url);
                if (res.ok) { candles = await res.json(); break; }
            } catch (e) {}
        }
        if (!candles) throw new Error("Binance API failed");
        
        const closes = candles.map(c => parseFloat(c[4])); 
        const volumes = candles.map(c => parseFloat(c[5])); 
        const currentClose = closes[closes.length - 1];

        // --- INDICATOR CALCULATIONS ---

        // A. RSI (14)
        let gains = 0, losses = 0;
        for(let i = closes.length - 14; i < closes.length; i++) {
            const diff = closes[i] - closes[i-1];
            if (diff > 0) gains += diff; else losses += Math.abs(diff);
        }
        const avgGain = gains / 14; 
        const avgLoss = losses / 14;
        let rsi = 100;
        if (avgLoss !== 0) { rsi = 100 - (100 / (1 + (avgGain / avgLoss))); } else if (avgGain === 0) { rsi = 50; }

        // B. Bollinger Bands (20, 2)
        const bbPeriod = 20;
        const bbSlice = closes.slice(-bbPeriod);
        const sma = bbSlice.reduce((a, b) => a + b, 0) / bbPeriod;
        const variance = bbSlice.reduce((acc, val) => acc + Math.pow(val - sma, 2), 0) / bbPeriod;
        const stdDev = Math.sqrt(variance);
        const upperBB = sma + (stdDev * 2);
        const lowerBB = sma - (stdDev * 2);

        // Helper: Full EMA Array
        const calculateEMAArray = (data, period) => {
            const k = 2 / (period + 1);
            let emaArray = [data[0]];
            for (let i = 1; i < data.length; i++) {
                emaArray.push((data[i] * k) + (emaArray[i - 1] * (1 - k)));
            }
            return emaArray;
        };

        // C. EMA (9 & 21)
        const ema9 = calculateEMAArray(closes, 9)[closes.length - 1];
        const ema21 = calculateEMAArray(closes, 21)[closes.length - 1];

        // D. MACD (12, 26, 9)
        const ema12Array = calculateEMAArray(closes, 12);
        const ema26Array = calculateEMAArray(closes, 26);
        const macdLineArray = ema12Array.map((v, i) => v - ema26Array[i]);
        const signalLineArray = calculateEMAArray(macdLineArray, 9);
        
        const currentMACD = macdLineArray[macdLineArray.length - 1];
        const currentSignal = signalLineArray[signalLineArray.length - 1];
        const currentHist = currentMACD - currentSignal;
        const prevHist = (macdLineArray[macdLineArray.length - 2] - signalLineArray[signalLineArray.length - 2]);

        // E. Volume Spikes
        const volSMA20 = volumes.slice(-20).reduce((a,b)=>a+b,0) / 20;
        const currentVol = volumes[volumes.length - 1];
        const hasVolumeSpike = currentVol > (volSMA20 * 1.5); 

        // --- UPGRADED BRAIN LOGIC SCORING ---
        let upScore = 0;
        let downScore = 0;
        
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

        // --- FINALIZE PREDICTION ---
        let prediction = "NONE";
        let winningScore = 0;
        let tryScore = 0;
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

        // Save to internal memory instead of localStorage
        if (prediction === "SKIP") {
            memoryStore[`pred_${targetEpoch}`] = { pred: "SKIP", conf: "Chop / Low Vol", try: tryPred, tryConf: tryConf, betPrice: currentBetPrice };
            console.log(`🤖 [Epoch ${targetEpoch}] Decision: SKIP (Chop / Low Vol). Would have tried: ${tryPred}`);
        } else {
            memoryStore[`pred_${targetEpoch}`] = { pred: prediction, conf: finalConfidence, betPrice: currentBetPrice };
            console.log(`🤖 [Epoch ${targetEpoch}] Decision: ${prediction} (Conf: ${finalConfidence}, Bet Price: $${currentBetPrice})`);
        }   

    } catch (e) { 
        console.error("Brain Failed:", e); 
    }
}

// --- VERIFY & LOG TO DATABASE ---
async function verifyResult(epochToCheck) {
    try {
        if (epochToCheck <= 0) return;
        const round = await contract.rounds(epochToCheck);
        if (!round.oracleCalled) return; 

        const lockPrice = parseFloat(ethers.utils.formatUnits(round.lockPrice, 8));
        const closePrice = parseFloat(ethers.utils.formatUnits(round.closePrice, 8));
        const actualResult = closePrice > lockPrice ? "UP" : "DOWN";
        
        // Retrieve prediction from memory
        const botData = memoryStore[`pred_${epochToCheck}`];
        
        if (botData) {
            const isWin = (botData.pred === "SKIP") ? "SKIPPED" : ((botData.pred === actualResult) ? "WIN" : "LOSS");
            
            console.log(`\n⚖️ [Epoch ${epochToCheck}] Resolving...`);
            console.log(`   Bot Guessed: ${botData.pred}`);
            console.log(`   Actual Result: ${actualResult}`);
            console.log(`   Outcome: ${isWin}`);
            
            // Push to Supabase Database
            const { error } = await supabaseClient.from('prediction_logs').insert([{ 
                epoch_id: epochToCheck, 
                predicted_side: botData.pred, 
                result: isWin,
                confidence: botData.conf
            }]);

            if (error) {
                console.error("❌ Supabase insert error:", error);
            } else {
                console.log(`✅ Successfully logged Epoch ${epochToCheck} to Database.`);
                // Clean up memory to prevent memory leaks over time
                delete memoryStore[`pred_${epochToCheck}`];
            }
        }
    } catch(e) {
        console.error("Result Verification Failed:", e);
    }
}

// Kick off the engine
startBot();
