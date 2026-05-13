import re
import os

with open('cluster_sniper.py', 'r', encoding='utf-8') as f:
    code = f.read()

# I will cleanly split using string splits
p1 = code.split("async def state_change_watcher")[0]
p2 = "async def motor2_hf_monitor(" + code.split("async def motor2_hf_monitor(")[1]

fixed_watcher = """async def state_change_watcher(
    state:        ClusterSniperState,
    w3_alchemy:   AsyncWeb3,
    wss_url:      str,
    trigger_lock: asyncio.Lock,
) -> None:
    tag     = "STATE-WATCHER"
    backoff = WSS_INITIAL_BACKOFF

    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    def _addr_to_topic(addr: str) -> str:
        return "0x" + "0" * 24 + addr.lower().lstrip("0x")

    target_topic_map: Dict[str, List[BurstState]] = {}
    for bs in state.burst_states:
        if not bs.confirmed:
            topic = _addr_to_topic(bs.target.address)
            target_topic_map.setdefault(topic, []).append(bs)

    logger.info("[%s] %d hedef, %d Aave token Transfer izlemesi.",
                tag, len(target_topic_map), len(ALL_AAVE_TOKENS))

    while True:
        try:
            async with websockets.connect(
                wss_url,
                ping_interval=10,
                ping_timeout=10,
                max_size=2**20,
            ) as ws:
                logger.info("[%s] WSS Bağlandı.", tag)
                
                sub_request = {
                    "id": 2,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": ALL_AAVE_TOKENS,
                            "topics": [transfer_topic]
                        }
                    ]
                }
                await ws.send(json.dumps(sub_request))
                
                async for message in ws:
                    data = json.loads(message)
                    if "params" not in data:
                        continue
                        
                    result = data["params"]["result"]
                    topics = result.get("topics", [])
                    if not topics or len(topics) < 3:
                        continue
                        
                    from_topic = topics[1]
                    to_topic = topics[2]
                    
                    affected = {}
                    if from_topic in target_topic_map:
                        for bs in target_topic_map[from_topic]:
                            affected[bs.target.address] = bs
                    if to_topic in target_topic_map:
                        for bs in target_topic_map[to_topic]:
                            affected[bs.target.address] = bs
                            
                    if not affected:
                        continue
                        
                    for bs in affected.values():
                        if bs.confirmed:
                            continue
                        t = bs.target
                        
                        ev_addr = result.get("address", "").lower()
                        matched_res = None
                        for r in KNOWN_AAVE_RESERVES:
                            if ev_addr == r["atoken"].lower() or ev_addr == r["vtoken"].lower():
                                matched_res = r
                                break
                                
                        if not matched_res:
                            continue

                        c_raw, d_raw = await fetch_user_reserve(
                            w3_alchemy, matched_res["asset"], t.address
                        )
                        
                        sym = matched_res["symbol"]
                        if c_raw > 0:
                            amount = c_raw / (10 ** matched_res["decimals"])
                            t.hf_collaterals[sym] = HFEntry(
                                symbol=sym, asset=matched_res["asset"], amount=amount, 
                                lt=matched_res["lt"], price_key=sym
                            )
                        elif sym in t.hf_collaterals:
                            del t.hf_collaterals[sym]
                            
                        if d_raw > 0:
                            amount = d_raw / (10 ** matched_res["decimals"])
                            t.hf_debts[sym] = HFEntry(
                                symbol=sym, asset=matched_res["asset"], amount=amount, 
                                lt=0.0, price_key=sym
                            )
                        elif sym in t.hf_debts:
                            del t.hf_debts[sym]

                        if sym == t.coll_token:
                            t.coll_amount = c_raw / (10 ** t.coll_decimals)
                        if sym == t.debt_token:
                            t.debt_amount = d_raw / (10 ** t.debt_decimals)

                        old_hf = t.in_memory_hf
                        t.compute_hf(state.oracle.prices)
                        t.recalculate(state.oracle)
                        bs.reset_thresholds()
                        
                        logger.info(
                            "[%s] %s | HF: %.6f → %.6f | Kâr: $%.2f",
                            tag, t.label, old_hf, t.in_memory_hf,
                            t.estimated_profit_usd,
                        )

        except Exception as e:
            logger.error("[%s] WSS Hata: %s | Yeniden bağlanılıyor...", tag, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WSS_MAX_BACKOFF)

# ─────────────────────────────────────────────────────────────────────────────
# Motor 1: Burst Fire Engine (Kör Nişancı)
# ─────────────────────────────────────────────────────────────────────────────

async def burst_fire_engine(
    state:       ClusterSniperState,
    w3:          AsyncWeb3,
    private_key: str,
    executor:    str,
) -> None:
    tag = "Motor1(Burst)"
    if state.dry_run:
        logger.info("[%s] DRY_RUN = True. Tx atılmayacak.", tag)

    w3_alchemy = AsyncWeb3(AsyncHTTPProvider(os.getenv("ARB_RPC", "")))
    base_gas_limit = 2000000

    while True:
        try:
            with state.oracle.lock:
                prices = dict(state.oracle.prices)

            for bs in state.burst_states:
                if bs.confirmed:
                    continue

                t = bs.target
                current_hf = t.compute_hf(prices)

                if current_hf > bs.bullet1_threshold:
                    continue

                if not bs.bullet1_sent and current_hf <= bs.bullet1_threshold:
                    logger.warning(
                        "[%s] 🔫 BULLET 1 ÖNCÜ | %-14s | HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet1_threshold
                    )
                    bs.bullet1_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 0, private_key, gas_limit, gas_mult, executor, state.dry_run),
                        name=f"burst-{t.label}-b0",
                    )

                if (bs.bullet1_sent and not bs.bullet2_sent and current_hf <= bs.bullet2_threshold):
                    logger.warning(
                        "[%s] 🔥 BULLET 2 SICAK | %-14s | HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet2_threshold,
                    )
                    bs.bullet2_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 1, private_key, gas_limit, gas_mult, executor, state.dry_run),
                        name=f"burst-{t.label}-b1",
                    )

                if (bs.bullet2_sent and not bs.bullet3_sent and current_hf <= bs.bullet3_threshold):
                    logger.warning(
                        "[%s] 💀 BULLET 3 ÖLÜMCÜL | %-14s | HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet3_threshold,
                    )
                    bs.bullet3_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 2, private_key, gas_limit, gas_mult, executor, state.dry_run),
                        name=f"burst-{t.label}-b2",
                    )

            await asyncio.sleep(0.001)

        except Exception as e:
            logger.error("[%s] Motor hatası: %s", tag, e)
            await asyncio.sleep(1)

"""

# Gas issues fixes inside execute_burst args inside burst_fire_engine: 
# The variables gas_limit, gas_mult don't exist here. They should be base_gas_limit, gas_mult if it exists, or just we use what was originally there.
# Let's fix that too. Wait, what was execute_burst arguments? 
# "execute_burst(w3, bs, 0, private_key, base_gas_limit, 1.0, executor, state.dry_run)"
# I'll replace gas_limit, gas_mult with base_gas_limit, 1.0 in my string.
fixed_watcher = fixed_watcher.replace('gas_limit, gas_mult', 'base_gas_limit, 1.0')

full_code = p1 + fixed_watcher + "\n" + p2

with open('cluster_sniper.py', 'w', encoding='utf-8') as f:
    f.write(full_code)
