import re
import os

with open('cluster_sniper.py', 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Add KNOWN_AAVE_RESERVES
reserves_str = """
KNOWN_AAVE_RESERVES = [
    {"symbol": "WETH",   "asset": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "atoken": "0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8", "vtoken": "0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351", "decimals": 18, "lt": 0.825, "feed": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"},
    {"symbol": "WBTC",   "asset": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "atoken": "0x078f358208685046a11C85e8ad32895DED33A249", "vtoken": "0x92b42c66840C7AD907b4BF74879FF3eF7c529473", "decimals": 8,  "lt": 0.70,  "feed": "0x6ce185860a4963106506C203335A2910413708e9"},
    {"symbol": "USDC",   "asset": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "atoken": "0x724dc807b04555b71ed48a6896b6F41593b8C637", "vtoken": "0xf611aEb5013fD2c0511c9CD55c7dc5C1140741A6", "decimals": 6,  "lt": 0.86,  "feed": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3"},
    {"symbol": "USDC.e", "asset": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", "atoken": "0x625E7708f30cA75bfd92586e17077590C60eb4cD", "vtoken": "0xFCCf3cAbbe80101232d343252614b6A3eE81C989", "decimals": 6,  "lt": 0.86,  "feed": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3"},
    {"symbol": "USDT",   "asset": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "atoken": "0x6ab707Aca953eDAeFBc4fD23bA73294241490620", "vtoken": "0xfb00AC187a8Eb5AFAE4eACE434F493Eb62672df7", "decimals": 6,  "lt": 0.80,  "feed": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7"},
    {"symbol": "ARB",    "asset": "0x912CE59144191C1204E64559FE8253a0e49E6548", "atoken": "0x6533afac2E7BCCB20dca161449A13A32D391fb00", "vtoken": "0x44705f578135cC5d703b4c9c122528C73Eb87145", "decimals": 18, "lt": 0.72,  "feed": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6"},
    {"symbol": "DAI",    "asset": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "atoken": "0x82E64f49Ed5EC1bC6e43DAD4FC8Af9bb3A2312EE", "vtoken": "0x8619d80FB0141ba7F184CbF22fd724116D9f7ffC", "decimals": 18, "lt": 0.80,  "feed": "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB"},
    {"symbol": "wstETH", "asset": "0x5979D7b546E38E414F7E9822514be443A4800529", "atoken": "0x513c7E3a9c69cA3e22550eF58AC1C0088e918FFf", "vtoken": "0x77CA01483f379E58174739308945f044e1a764dc", "decimals": 18, "lt": 0.825, "feed": "0xb523AE262D20A936BC152e6023996e46FDC2A95D"},
    {"symbol": "LINK",   "asset": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4", "atoken": "0x191c10Aa4AF7C30e871E70C95dB0E4eb77237530", "vtoken": "0x953A573793604aF8d41F306FEb8274190dB4aE0e", "decimals": 18, "lt": 0.79,  "feed": "0x86E53CF1B870786351Da77A57575e79CB55812CB"}
]
"""
code = code.replace("def _build_feed_map() -> Dict[str, str]:", reserves_str + "\ndef _build_feed_map() -> Dict[str, str]:")

feed_updates = """
def _build_feed_map() -> Dict[str, str]:
    feed_map: Dict[str, str] = {}
    for r in KNOWN_AAVE_RESERVES:
        feed_map[r["feed"].lower()] = r["symbol"]
    return feed_map

FEED_MAP: Dict[str, str] = _build_feed_map()

ALL_FEED_ADDRESSES: List[str] = list(set([r["feed"] for r in KNOWN_AAVE_RESERVES]))

_all_aave = set()
for r in KNOWN_AAVE_RESERVES:
    _all_aave.add(r["atoken"])
    _all_aave.add(r["vtoken"])
ALL_AAVE_TOKENS: List[str] = list(_all_aave)
"""
code = re.sub(r'def _build_feed_map\(\) -> Dict\[str, str\]:.*?ALL_AAVE_TOKENS: List\[str\] = list\(_all_aave\)', feed_updates, code, flags=re.DOTALL)

dataclass_updates = """
@dataclass
class HFEntry:
    symbol: str
    asset: str
    amount: float
    lt: float
    price_key: str

@dataclass
class ClusterTarget:
    address:          str
    label:            str

    coll_token:       str   = ""
    coll_address:     str   = ""
    coll_atoken:      str   = ""
    coll_decimals:    int   = 18
    coll_amount:      float = 0.0
    coll_lt:          float = 0.0
    coll_bonus:       float = 0.0

    debt_token:       str   = ""
    debt_address:     str   = ""
    debt_vtoken:      str   = ""
    debt_decimals:    int   = 18
    debt_amount:      float = 0.0

    close_factor:     float = 0.5
    
    hf_collaterals: Dict[str, HFEntry] = field(default_factory=dict)
    hf_debts: Dict[str, HFEntry] = field(default_factory=dict)
    in_memory_hf:         float = 999.0
    estimated_profit_usd: float = 0.0

    def compute_hf(self, prices: Dict[str, float]) -> float:
        total_coll_usd_lt = 0.0
        total_debt_usd = 0.0
        
        for entry in self.hf_collaterals.values():
            total_coll_usd_lt += entry.amount * prices.get(entry.price_key, 0.0) * entry.lt
            
        for entry in self.hf_debts.values():
            total_debt_usd += entry.amount * prices.get(entry.price_key, 0.0)
            
        if total_debt_usd == 0.0:
            self.in_memory_hf = 999.0
        else:
            self.in_memory_hf = total_coll_usd_lt / total_debt_usd
            
        return self.in_memory_hf

    def recalculate(self, oracle: OracleState) -> None:
        if self.debt_amount <= 0:
            self.in_memory_hf = 999.0
            self.estimated_profit_usd = 0.0
            return

        debt_price   = oracle.get(self.debt_token)
        covered_usd  = self.debt_amount * self.close_factor * debt_price
        gross        = covered_usd * self.coll_bonus
        flash_fee    = covered_usd * FLASH_LOAN_FEE
        slippage     = covered_usd * DEX_SLIPPAGE
        self.estimated_profit_usd = gross - flash_fee - slippage - 0.05
        
        self.compute_hf(oracle.prices)

@dataclass
class BurstState:
    target:            ClusterTarget
    bullet1_threshold: float = 1.0010
    bullet2_threshold: float = 1.0005
    bullet3_threshold: float = 1.0000
    bullet1_sent:      bool  = False
    bullet2_sent:      bool  = False
    bullet3_sent:      bool  = False
    confirmed:         bool  = False
    base_nonce:        int   = -1
    last_fire_ts:      float = 0.0

    def reset_thresholds(self) -> None:
        m1 = float(os.getenv("BURST_M1", "0.001"))
        m2 = float(os.getenv("BURST_M2", "0.0005"))
        self.bullet1_threshold = 1.0 + m1
        self.bullet2_threshold = 1.0 + m2
        self.bullet3_threshold = 1.0
"""
code = re.sub(r'@dataclass\nclass ClusterTarget:.*?def reset_thresholds\(self\) -> None:.*?self\.bullet3_threshold = self.target.liq_ratio', dataclass_updates + "\n        pass", code, flags=re.DOTALL)


code = code.replace("async def init_all_targets(w3: AsyncWeb3, cluster: ClusterSniperState) -> None:", "async def init_all_targets(w3: AsyncWeb3, cluster: ClusterSniperState) -> None:\n    from collections import defaultdict")

init_replace = """
        for r in KNOWN_AAVE_RESERVES:
            c_raw, d_raw = await fetch_user_reserve(w3, r["asset"], t.address)
            
            if c_raw > 0:
                amount = c_raw / (10 ** r["decimals"])
                t.hf_collaterals[r["symbol"]] = HFEntry(
                    symbol=r["symbol"], asset=r["asset"], amount=amount, 
                    lt=r["lt"], price_key=r["symbol"]
                )
            
            if d_raw > 0:
                amount = d_raw / (10 ** r["decimals"])
                t.hf_debts[r["symbol"]] = HFEntry(
                    symbol=r["symbol"], asset=r["asset"], amount=amount, 
                    lt=0.0, price_key=r["symbol"]
                )

        coll_raw = int(t.hf_collaterals.get(t.coll_token).amount * (10 ** t.coll_decimals)) if t.coll_token in t.hf_collaterals else 0
        debt_raw = int(t.hf_debts.get(t.debt_token).amount * (10 ** t.debt_decimals)) if t.debt_token in t.hf_debts else 0

        if debt_raw == 0 and dossier.get("debt_address_alt"):
            alt_debt_address = dossier["debt_address_alt"]
            # Alternate is typically USDC.e
            sym = "USDC.e"
            _, debt_raw_alt = await fetch_user_reserve(w3, alt_debt_address, t.address)
            if debt_raw_alt > 0:
                debt_raw = debt_raw_alt
                t.debt_address = alt_debt_address
                t.debt_token = sym
                if sym in KNOWN_AAVE_RESERVES:
                    r = next((x for x in KNOWN_AAVE_RESERVES if x["symbol"] == sym), None)
                    if r:
                        t.hf_debts[sym] = HFEntry(symbol=sym, asset=alt_debt_address, amount=debt_raw_alt/(10**r['decimals']), lt=0.0, price_key=sym)
"""
code = re.sub(r'# ── Teminat bakiyesi ──.*?debt_raw = debt_raw_alt', init_replace + "\n", code, flags=re.DOTALL)

state_watcher_mod = """
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
"""
code = re.sub(r'for bs in affected.values\(\):.*?w3_alchemy, t\.debt_address, t\.address\n\s*\).*?t\.estimated_profit_usd,\n\s*\)', state_watcher_mod, code, flags=re.DOTALL)

burst_engine_mod = """
            for bs in state.burst_states:
                if bs.confirmed:
                    continue

                t = bs.target
                current_hf = t.compute_hf(prices)

                if current_hf > bs.bullet1_threshold:
                    continue

                if not bs.bullet1_sent and current_hf <= bs.bullet1_threshold:
                    logger.warning(
                        "[%s] 🔫 BULLET 1 ÖNCÜ | %-14s | "
                        "HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet1_threshold
                    )
                    bs.bullet1_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 0, private_key,
                                      gas_limit, gas_mult, executor, state.dry_run),
                        name=f"burst-{t.label}-b0",
                    )

                if (bs.bullet1_sent and not bs.bullet2_sent
                        and current_hf <= bs.bullet2_threshold):
                    logger.warning(
                        "[%s] 🔥 BULLET 2 SICAK | %-14s | "
                        "HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet2_threshold,
                    )
                    bs.bullet2_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 1, private_key,
                                      gas_limit, gas_mult, executor, state.dry_run),
                        name=f"burst-{t.label}-b1",
                    )

                if (bs.bullet2_sent and not bs.bullet3_sent
                        and current_hf <= bs.bullet3_threshold):
                    logger.warning(
                        "[%s] 💀 BULLET 3 ÖLÜMCÜL | %-14s | "
                        "HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet3_threshold,
                    )
                    bs.bullet3_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 2, private_key,
                                      gas_limit, gas_mult, executor, state.dry_run),
                        name=f"burst-{t.label}-b2",
                    )
"""
code = re.sub(r'for bs in state\.burst_states:.*?name=f"burst-{t\.label}-b2",\n\s*\)', burst_engine_mod, code, flags=re.DOTALL)

code = re.sub(r'current = t\.current_ratio\(o\).*?else:', 'current_hf = t.in_memory_hf\n            logger.info(\n                "  %-14s | RAM HF=%.6f | kâr=$%.2f",\n                t.label, current_hf, t.estimated_profit_usd,\n            )\n        else:', code, flags=re.DOTALL)

code = re.sub(r'def current_ratio\(self, oracle: OracleState\) -> float:.*?return oracle\.ratio\(self\.coll_token, self\.debt_token\)', '', code, flags=re.DOTALL)
code = re.sub(r'def ratio\(self, coll_symbol: str, debt_symbol: str\) -> float:.*?return coll_p / debt_p', '', code, flags=re.DOTALL)

with open('cluster_sniper.py', 'w', encoding='utf-8') as f:
    f.write(code)
