with open('cluster_sniper.py', 'r', encoding='utf-8') as f:
    text = f.read()

GHOST_CODE = """# ─────────────────────────────────────────────────────────────────────────────
# GHOST LOOP: Aave Parameter Sync
# ─────────────────────────────────────────────────────────────────────────────

GET_RESERVE_CONFIG_ABI = [{
    "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
    "name": "getReserveConfigurationData",
    "outputs": [
        {"internalType": "uint256", "name": "decimals", "type": "uint256"},
        {"internalType": "uint256", "name": "ltv", "type": "uint256"},
        {"internalType": "uint256", "name": "liquidationThreshold", "type": "uint256"},
        {"internalType": "uint256", "name": "liquidationBonus", "type": "uint256"},
        {"internalType": "uint256", "name": "reserveFactor", "type": "uint256"},
        {"internalType": "bool", "name": "usageAsCollateralEnabled", "type": "bool"},
        {"internalType": "bool", "name": "borrowingEnabled", "type": "bool"},
        {"internalType": "bool", "name": "stableBorrowRateEnabled", "type": "bool"},
        {"internalType": "bool", "name": "isActive", "type": "bool"},
        {"internalType": "bool", "name": "isFrozen", "type": "bool"}
    ],
    "stateMutability": "view",
    "type": "function"
}]

async def aave_parameter_sync(w3: AsyncWeb3, state: ClusterSniperState) -> None:
    tag = "GHOST-SYNC"
    poll_interval = 14400  # 4 saat
    cs = AsyncWeb3.to_checksum_address
    
    # Bekleme olmadan argümanlar init edildikten sonra 1 dakika bekle
    await asyncio.sleep(60)
    
    dp = w3.eth.contract(address=cs(AAVE_DATA_PROVIDER), abi=GET_RESERVE_CONFIG_ABI)
    
    while True:
        try:
            for r in KNOWN_AAVE_RESERVES:
                try:
                    res = await dp.functions.getReserveConfigurationData(cs(r["asset"])).call()
                    new_lt = res[2] / 10000.0
                    old_lt = r["lt"]
                    
                    if abs(new_lt - old_lt) > 0.0001:
                        logger.info("[%s] %s LT güncellendi: %%%.1f -> %%%.1f", 
                                    tag, r["symbol"], old_lt * 100, new_lt * 100)
                        
                        r["lt"] = new_lt
                        
                        # Hedefleri güncelle
                        for bs in state.burst_states:
                            if r["symbol"] in bs.target.hf_collaterals:
                                bs.target.hf_collaterals[r["symbol"]].lt = new_lt

                except Exception as e:
                    pass
                    
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error("[%s] Döngü hatası: %s", tag, e)
            
        await asyncio.sleep(poll_interval)

"""

# Insert the code before `def parse_args():`
if 'def parse_args()' in text:
    text = text.replace('def parse_args()', GHOST_CODE + 'def parse_args()')

# Add the task to main()
old_tasks = """        asyncio.create_task(
            state_change_watcher(
                state=cluster_state,
                w3_alchemy=w3_alchemy,
                wss_url=alchemy_wss,
                trigger_lock=asyncio.Lock()
            ),
            name="state-watcher",
        ),"""

new_tasks = old_tasks + """
        asyncio.create_task(
            aave_parameter_sync(w3_alchemy, cluster_state),
            name="ghost-sync",
        ),"""

text = text.replace(old_tasks, new_tasks)

# Also update the log
old_log = 'logger.info("        4 paralel task başlatıldı:")'
new_log = 'logger.info("        5 paralel task başlatıldı:")'
text = text.replace(old_log, new_log)
old_log2 = 'logger.info("          ▶ motor2-hf")'
new_log2 = 'logger.info("          ▶ motor2-hf")\n    logger.info("          ▶ ghost-sync")'
text = text.replace(old_log2, new_log2)

with open('cluster_sniper.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Success')
