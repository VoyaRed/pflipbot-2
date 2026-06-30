const { HttpsProxyAgent } = require('https-proxy-agent');

// The 4 public, auto-updating free proxy lists
const PROXY_SOURCES = [
  'https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/protocols/http/data.txt',
  'https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt',
  'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
  'https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_ssl.txt'
];

// In-memory array to store current functional proxies
let proxyList = [];

/**
 * Fetches proxies from all 4 sources simultaneously, normalizes them, and filters duplicates.
 */
async function refreshProxyPool() {
  console.log("🔄 Fetching fresh proxies from all sources...");
  const uniqueProxies = new Set();

  // Fetch all sources concurrently
  const requests = PROXY_SOURCES.map(async (url) => {
    try {
      const response = await fetch(url);
      if (!response.ok) return [];
      const text = await response.text();
      return text.split('\n');
    } catch (err) {
      console.warn(`⚠️ Failed to fetch proxies from source: ${url}`);
      return [];
    }
  });

  const results = await Promise.all(requests);

  // Clean and parse the strings
  for (const lines of results) {
    for (let line of lines) {
      line = line.trim();
      // Skip empty lines or comments
      if (!line || line.startsWith('#') || line.startsWith('//')) continue;

      // Ensure the proxy string contains a protocol prefix
      if (!line.startsWith('http://') && !line.startsWith('https://')) {
        line = `http://${line}`;
      }

      uniqueProxies.add(line);
    }
  }

  proxyList = Array.from(uniqueProxies);
  console.log(`✅ Proxy pool refreshed. Loaded ${proxyList.length} unique proxies.`);
}

/**
 * Selects a random proxy from the active pool and tracks its URL string.
 * Returns { agent, url } or { agent: null, url: null } if the pool is dry.
 */
function getRandomProxy() {
  if (proxyList.length === 0) {
    return { agent: null, url: null };
  }

  const randomIndex = Math.floor(Math.random() * proxyList.length);
  const proxyUrl = proxyList[randomIndex];

  return {
    agent: new HttpsProxyAgent(proxyUrl),
    url: proxyUrl
  };
}

/**
 * Removes a bad or timed-out proxy from the pool so the bot avoids reusing it.
 */
function banProxy(proxyUrl) {
  if (!proxyUrl) return;
  const initialLength = proxyList.length;
  proxyList = proxyList.filter(url => url !== proxyUrl);
  
  if (proxyList.length < initialLength) {
    console.log(`🚫 Banned dead proxy: ${proxyUrl} | Remaining in pool: ${proxyList.length}`);
  }

  // Auto-refresh the pool if it drops below a threshold safety margin
  if (proxyList.length < 10) {
    refreshProxyPool().catch(err => console.error("Failed to auto-refresh pool:", err));
  }
}

module.exports = {
  refreshProxyPool,
  getRandomProxy,
  banProxy
};
