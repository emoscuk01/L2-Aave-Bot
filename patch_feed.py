with open('cluster_sniper.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Fix FEED_MAP builder
old_feed_map = """def _build_feed_map() -> Dict[str, str]:
    feed_map: Dict[str, str] = {}
    for r in KNOWN_AAVE_RESERVES:
        feed_map[r["feed"].lower()] = r["symbol"]
    return feed_map

FEED_MAP: Dict[str, str] = _build_feed_map()"""
new_feed_map = """def _build_feed_map() -> Dict[str, List[str]]:
    feed_map: Dict[str, List[str]] = {}
    for r in KNOWN_AAVE_RESERVES:
        f = r["feed"].lower()
        if f not in feed_map:
            feed_map[f] = []
        feed_map[f].append(r["symbol"])
    return feed_map

FEED_MAP: Dict[str, List[str]] = _build_feed_map()"""
text = text.replace(old_feed_map, new_feed_map)

# 2. Fix Oracle Hub loop
old_hub_processing = """                    # FEED_MAP'ten hangi token'ın güncellendiğini bul
                    token_symbol = FEED_MAP.get(event_addr)
                    if not token_symbol or len(topics) < 2:
                        continue

                    try:
                        raw_price = _decode_topic_int256(topics[1])
                        price_usd = raw_price / CHAINLINK_DECIMALS
                    except Exception as dec_exc:
                        logger.debug("[%s] Decode hatası: %s", tag, dec_exc)
                        continue

                    if price_usd <= 0:
                        continue

                    # Fiyatı güncelle
                    state.oracle.prices[token_symbol] = price_usd
                    state.oracle.last_updated         = time.time()

                    logger.debug("[%s] %s = $%.4f", tag, token_symbol, price_usd)"""
new_hub_processing = """                    # FEED_MAP'ten hangi token'ın güncellendiğini bul
                    token_symbols = FEED_MAP.get(event_addr)
                    if not token_symbols or len(topics) < 2:
                        continue

                    try:
                        raw_price = _decode_topic_int256(topics[1])
                        price_usd = raw_price / CHAINLINK_DECIMALS
                    except Exception as dec_exc:
                        logger.debug("[%s] Decode hatası: %s", tag, dec_exc)
                        continue

                    if price_usd <= 0:
                        continue

                    # Fiyatı güncelle
                    for token_symbol in token_symbols:
                        state.oracle.prices[token_symbol] = price_usd
                        logger.debug("[%s] %s = $%.4f", tag, token_symbol, price_usd)
                    state.oracle.last_updated = time.time()"""
text = text.replace(old_hub_processing, new_hub_processing)

# 3. Fix main init_oracles_http
old_http_init = """            token_sym = FEED_MAP.get(feed_addr.lower(), "?")
            try:
                feed  = w3_alchemy.eth.contract(address=cs(feed_addr), abi=CHAINLINK_ROUND_ABI)
                _, ans, *_ = await feed.functions.latestRoundData().call()
                price = ans / CHAINLINK_DECIMALS
                cluster_state.oracle.prices[token_sym] = price
                cluster_state.oracle.last_updated      = time.time()
                logger.info("  %8s = $%.4f", token_sym, price)"""
new_http_init = """            token_syms = FEED_MAP.get(feed_addr.lower(), [])
            try:
                feed  = w3_alchemy.eth.contract(address=cs(feed_addr), abi=CHAINLINK_ROUND_ABI)
                _, ans, *_ = await feed.functions.latestRoundData().call()
                price = ans / CHAINLINK_DECIMALS
                for token_sym in token_syms:
                    cluster_state.oracle.prices[token_sym] = price
                    logger.info("  %8s = $%.4f", token_sym, price)
                cluster_state.oracle.last_updated = time.time()"""
text = text.replace(old_http_init, new_http_init)

with open('cluster_sniper.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("done")
