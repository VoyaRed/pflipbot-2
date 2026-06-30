const fs = require('fs');
const path = require('path');
const { HttpsProxyAgent } = require('https-proxy-agent');

// Function to load and parse proxies from proxies.txt
function loadProxies() {
  try {
    const filePath = path.resolve(process.cwd(), 'proxies.txt');
    
    // Read file and split by newlines, filtering out empty lines or comments
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const proxies = fileContent
      .split('\n')
      .map(line => line.trim())
      .filter(line => line && !line.startsWith('#'));

    if (proxies.length === 0) {
      throw new Error("Proxy list is empty.");
    }
    
    return proxies;
  } catch (error) {
    console.error("Failed to load proxies.txt. Falling back to direct connection:", error.message);
    return [];
  }
}

// Keep the proxy list in memory so you don't read the disk on every request
const proxyList = loadProxies();

/**
 * Selects a random proxy and returns an HttpsProxyAgent instance.
 * Returns null if no proxies are available, allowing a direct connection fallback.
 */
function getRandomProxyAgent() {
  if (proxyList.length === 0) return null;

  // Pick a random index
  const randomIndex = Math.floor(Math.random() * proxyList.length);
  const proxyUrl = proxyList[randomIndex];

  console.log(`Routing request through proxy: ${proxyUrl}`);

  return new HttpsProxyAgent(proxyUrl);
}

// Export using CommonJS syntax
module.exports = {
  getRandomProxyAgent
};
