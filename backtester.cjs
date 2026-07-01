// backtester.cjs - UpsideDownCake Backtesting Engine 🍰
const fs = require('fs');
const path = require('path');

// --- CONFIG ---
const STARTING_BANKROLL = 100.0; // Start with 100 BNB/USD/Units
const BASE_BET = 1.0;            // Base bet size
const HISTORY_FILE = path.join(__dirname, 'history.json');
const OUTPUT_FILE = path.join(__dirname, 'backtest_chart.html');

function runBacktest() {
    console.log("🍰 Loading UpsideDownCake Historical Data...");

    if (!fs.existsSync(HISTORY_FILE)) {
        console.error("❌ Error: history.json not found in the current directory.");
        return;
    }

    // 1. Parse history.json (JSON Lines format)
    const fileContent = fs.readFileSync(HISTORY_FILE, 'utf-8');
    const lines = fileContent.split('\n').filter(line => line.trim() !== '');
    
    // Parse and sort chronologically by epoch
    const rounds = lines.map(line => JSON.parse(line)).sort((a, b) => parseInt(a.epoch) - parseInt(b.epoch));
    
    console.log(`✅ Loaded ${rounds.length} historical rounds.`);
    console.log("⚙️ Running simulations...");

    // --- STRATEGY 1: MARTINGALE (Always bet BULL, double on loss) ---
    let martingaleBankroll = STARTING_BANKROLL;
    let currentMartingaleBet = BASE_BET;
    let martingaleHistory = [];

    // --- STRATEGY 2: TREND FOLLOWER (Bet the exact result of the previous round) ---
    let trendBankroll = STARTING_BANKROLL;
    const trendBet = BASE_BET;
    let lastResult = 'bull'; // Default starting assumption
    let trendHistory = [];

    const labels = [];

    // 2. Run the Loop
    for (let i = 0; i < rounds.length; i++) {
        const round = rounds[i];
        const result = round.resultString; // "bull", "bear", or "draw"
        const bullMult = parseFloat(round.bullMultiplier);
        const bearMult = parseFloat(round.bearMultiplier);
        
        labels.push(`Epoch ${round.epoch}`);

        // --- Execute Martingale ---
        if (result === 'bull') {
            // Win: Get initial bet back + profit
            martingaleBankroll += (currentMartingaleBet * bullMult) - currentMartingaleBet;
            currentMartingaleBet = BASE_BET; // Reset to base on win
        } else if (result === 'bear') {
            // Loss: Subtract bet, double next bet
            martingaleBankroll -= currentMartingaleBet;
            currentMartingaleBet *= 2; 
        }
        // If draw, bankroll stays the same, bet stays the same
        martingaleHistory.push(martingaleBankroll.toFixed(2));

        // --- Execute Trend Follower ---
        if (result !== 'draw') {
            if (result === lastResult) {
                // Win
                const activeMult = result === 'bull' ? bullMult : bearMult;
                trendBankroll += (trendBet * activeMult) - trendBet;
            } else {
                // Loss
                trendBankroll -= trendBet;
            }
            lastResult = result; // Update the trend to the newest result
        }
        trendHistory.push(trendBankroll.toFixed(2));
    }

    console.log("📊 Generating Chart HTML...");

    // 3. Spitting out the Chart
    const chartHtml = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>UpsideDownCake Backtest 🍰</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <link href="https://fonts.googleapis.com/css2?family=Kanit:wght@400;600;800&display=swap" rel="stylesheet">
        <style>
            body { 
                background: #1a0f14; 
                color: #fce4ec; 
                font-family: 'Kanit', sans-serif; 
                padding: 40px; 
            }
            .container {
                background: #2d1b24;
                padding: 30px;
                border-radius: 24px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.5);
                border: 2px solid #4a2c3a;
            }
            h1 { color: #ff4757; text-align: center; }
            .stats { display: flex; justify-content: space-around; margin-top: 20px; font-size: 1.2rem;}
            .win { color: #31d0aa; font-weight: bold;}
            .loss { color: #ed4b9e; font-weight: bold;}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>UpsideDownCake Backtest Results 🍰</h1>
            <canvas id="pnlChart" height="100"></canvas>
            <div class="stats">
                <div>
                    <span>Martingale Final PnL: </span>
                    <span class="${martingaleBankroll >= STARTING_BANKROLL ? 'win' : 'loss'}">$${martingaleBankroll.toFixed(2)}</span>
                </div>
                <div>
                    <span>Trend Follower Final PnL: </span>
                    <span class="${trendBankroll >= STARTING_BANKROLL ? 'win' : 'loss'}">$${trendBankroll.toFixed(2)}</span>
                </div>
            </div>
        </div>
        <script>
            const ctx = document.getElementById('pnlChart').getContext('2d');
            new Chart(ctx, {
                type: 'line',
                data: {
                    labels: ${JSON.stringify(labels)},
                    datasets: [
                        {
                            label: 'Martingale (Bull Only)',
                            data: ${JSON.stringify(martingaleHistory)},
                            borderColor: '#ff4757',
                            backgroundColor: 'rgba(255, 71, 87, 0.1)',
                            borderWidth: 2,
                            pointRadius: 0,
                            fill: true,
                            tension: 0.1
                        },
                        {
                            label: 'Trend Follower (Last Result)',
                            data: ${JSON.stringify(trendHistory)},
                            borderColor: '#31d0aa',
                            backgroundColor: 'rgba(49, 208, 170, 0.1)',
                            borderWidth: 2,
                            pointRadius: 0,
                            fill: true,
                            tension: 0.1
                        }
                    ]
                },
                options: {
                    responsive: true,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        tooltip: { enabled: true },
                        legend: { labels: { color: '#fce4ec', font: { family: 'Kanit' } } }
                    },
                    scales: {
                        x: { grid: { color: '#4a2c3a' }, ticks: { color: '#b39baf' } },
                        y: { grid: { color: '#4a2c3a' }, ticks: { color: '#b39baf' } }
                    }
                }
            });
        </script>
    </body>
    </html>`;

    fs.writeFileSync(OUTPUT_FILE, chartHtml);
    console.log(`✅ Backtest complete! PnL Chart generated at: ${OUTPUT_FILE}`);
    console.log(`   -> Just double-click the HTML file to view the graph.`);
}

runBacktest();