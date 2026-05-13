import asyncio
from web3 import AsyncWeb3, AsyncHTTPProvider

RPC = "https://arb-mainnet.g.alchemy.com/v2/hmJIUy5LpYIq8eW4OGtmy"
DP_ADDR = "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"

ABI = [
    {
        "inputs": [],
        "name": "getAllReservesTokens",
        "outputs": [
            {"components": [
                {"internalType": "string",  "name": "symbol",       "type": "string"},
                {"internalType": "address", "name": "tokenAddress", "type": "address"},
            ], "internalType": "struct IPoolDataProvider.TokenData[]", "name": "", "type": "tuple[]"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
        "name": "getReserveTokensAddresses",
        "outputs": [
            {"internalType": "address", "name": "aTokenAddress", "type": "address"},
            {"internalType": "address", "name": "stableDebtTokenAddress", "type": "address"},
            {"internalType": "address", "name": "variableDebtTokenAddress", "type": "address"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

async def main():
    w3 = AsyncWeb3(AsyncHTTPProvider(RPC))
    dp = w3.eth.contract(address=w3.to_checksum_address(DP_ADDR), abi=ABI)
    res = await dp.functions.getAllReservesTokens().call()
    
    for symbol, asset in res:
        if symbol in ["WETH", "WBTC", "USDC", "USDCn", "USDT", "ARB", "DAI", "wstETH", "LINK"]:
            tokens = await dp.functions.getReserveTokensAddresses(asset).call()
            print(f"{symbol}: addr={asset}, aToken={tokens[0]}, vToken={tokens[2]}")

asyncio.run(main())
