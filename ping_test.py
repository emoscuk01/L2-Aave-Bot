import asyncio
import time
import json
import websockets
import aiohttp

# Tüm RPC'lerin listesi
# Tüm RPC'lerin listesi
ENDPOINTS = [
    # WSS Endpointler
    {"name": "Alchemy (ARB WSS)", "url": "wss://arb-mainnet.g.alchemy.com/v2/9K426BEyXtnSJENVdiUIG", "type": "wss"},
    {"name": "Alchemy (BASE WSS)", "url": "wss://base-mainnet.g.alchemy.com/v2/9K426BEyXtnSJENVdiUIG", "type": "wss"},
    {"name": "Chainstack (ARB WSS)", "url": "wss://arbitrum-mainnet.core.chainstack.com/f6b667dd6909fb9e959eec995098690b", "type": "wss"},
    {"name": "QuickNode (ARB WSS)",  "url": "wss://fittest-solitary-snow.arbitrum-mainnet.quiknode.pro/81afc8597af6aa9dd9f0a24259312d51b15a5aff", "type": "wss"},
    
    # HTTP Endpointler
    {"name": "Infura (ARB HTTP)",    "url": "https://arbitrum-mainnet.infura.io/v3/bdb722c1577644c1a25ff5e179b9603d", "type": "http"},
    {"name": "LlamaNodes (BASE HTTP)","url": "https://base.llamarpc.com", "type": "http"},
    
    # Public RPC'ler (Karşılaştırma için)
    {"name": "Public Arbitrum (HTTP)","url": "https://arb1.arbitrum.io/rpc", "type": "http"},
    {"name": "Public Base (HTTP)",    "url": "https://mainnet.base.org", "type": "http"}
]

PAYLOAD = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []})
HEADERS = {'Content-Type': 'application/json'}

async def measure_wss(url, name):
    try:
        async with websockets.connect(url, ping_interval=None) as ws:
            latencies = []
            for _ in range(5):
                t0 = time.perf_counter()
                await ws.send(PAYLOAD)
                await ws.recv()
                t1 = time.perf_counter()
                latencies.append((t1 - t0) * 1000)
                await asyncio.sleep(0.1)
            return sum(latencies) / len(latencies)
    except Exception as e:
        print(f"❌ {name} WSS Hatası: {e}")
        return None

async def measure_http(url, name):
    try:
        # ClientSession bağlantıyı açık tutar (Keep-Alive), botun yaptığı gibi gerçekçi ms verir
        async with aiohttp.ClientSession() as session:
            # İlk atış TLS el sıkışmasıdır (Handshake), onu listeye dahil etmiyoruz (ısınma atışı)
            await session.post(url, data=PAYLOAD, headers=HEADERS)
            
            latencies = []
            for _ in range(5):
                t0 = time.perf_counter()
                async with session.post(url, data=PAYLOAD, headers=HEADERS) as resp:
                    await resp.text()
                t1 = time.perf_counter()
                latencies.append((t1 - t0) * 1000)
                await asyncio.sleep(0.1)
            return sum(latencies) / len(latencies)
    except Exception as e:
        print(f"❌ {name} HTTP Hatası: {e}")
        return None

async def main():
    print("🚀 Multi-RPC Gecikme Testi Başlıyor (Isınma atışı hariç, 5 atış ortalaması)...\n")
    results = []
    
    for ep in ENDPOINTS:
        name, url, typ = ep["name"], ep["url"], ep["type"]
        print(f"Test ediliyor: {name}...")
        
        if typ == "wss":
            avg = await measure_wss(url, name)
        else:
            avg = await measure_http(url, name)
            
        if avg is not None:
            results.append((name, avg))
            
    print("\n" + "="*50)
    print("🏆 ŞAMPİYONLAR LİGİ (En Hızlıdan Yavaşa)")
    print("="*50)
    results.sort(key=lambda x: x[1])
    
    for i, (name, avg) in enumerate(results, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
        print(f"{medal} {name:.<30} {avg:>6.2f} ms")
    print("="*50)

if __name__ == "__main__":
    asyncio.run(main())