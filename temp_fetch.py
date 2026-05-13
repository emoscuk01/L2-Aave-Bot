import asyncio
import os
from web3 import AsyncWeb3, AsyncHTTPProvider

RPC = "https://arb-mainnet.g.alchemy.com/v2/hmJIUy5LpYIq8eW4OGtmy"
DP_ADDR = "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"

# ABI for getReserveTokensAddresses
ABI = [{
    "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
    "name": "getReserveTokensAddresses",
    "outputs": [
        {"internalType": "address", "name": "aTokenAddress", "type": "address"},
        {"internalType": "address", "name": "stableDebtTokenAddress", "type": "address"},
        {"internalType": "address", "name": "variableDebtTokenAddress", "type": "address"}
    ],
    "stateMutability": "view",
    "type": "function"
}]

async def main():
    w3 = AsyncWeb3(AsyncHTTPProvider(RPC))
    dp = w3.eth.contract(address=w3.to_checksum_address(DP_ADDR), abi=ABI)
    
    tokens = {
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "wstETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
        "LINK": "0xf97f4df75154AE04fb05E852Be92f8ce6bFc3f6B",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f" # checking wbtc vToken too
    }
    
    for sym, addr in tokens.items():
        res = await dp.functions.getReserveTokensAddresses(w3.to_checksum_address(addr)).call()
        print(f"{sym}: aToken={res[0]}, vToken={res[2]}")

asyncio.run(main())
