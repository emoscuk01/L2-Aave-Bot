"""
config.py — Merkezi Sinir Sistemi v8 (WSS Geçişi)
--------------------------------------------------
v7 → v8 Değişiklikler:

  1. [YENİ] ChainConfig.wss_url alanı eklendi.
     Her zincir için WebSocket (WSS) endpoint tanımlanabilir.
     wss_url boş bırakılırsa watcher.py HTTP polling moduna düşer.

  2. [YENİ] load_chains() artık Alchemy WSS endpoint'lerini yükler:
       OP   → wss://opt-mainnet.g.alchemy.com/v2/<KEY>
       ARB  → wss://arb-mainnet.g.alchemy.com/v2/<KEY>
       BASE → wss://base-mainnet.g.alchemy.com/v2/<KEY>
     .env'de OP_WSS / ARB_WSS / BASE_WSS tanımlı değilse sabit key ile
     varsayılan URL kullanılır.

  3. [KORUNDU] HF_ZOMBIE_MIN modül düzeyi export.
     aave_utils.py'deki dinamik close factor bu sabite bağımlı — dokunulmadı.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Set
from dotenv import load_dotenv

# Windows / IDE bazen GRAPH_API_KEY vb. sistem ortamında kalır; override=False iken .env yok sayılır
# → The Graph "auth error: API key not found". Üretimde gerçek secret'ı yalnızca .env veya -e ile verin.
load_dotenv(override=True)

_log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# MANUEL TASFİYE EŞİKLERİ (RT-Sync Kapalıyken Kullanılır)
# ─────────────────────────────────────────────────────────────────────────────
MANUAL_LT_VALUES = {
    "WETH":   0.84,
    "WBTC":   0.78,
    "USDC":   0.78,
    "USDC.e": 0.78,
    "USDT":   0.78,
    "ARB":    0.63,
    "DAI":    0.77,
    "wstETH": 0.79,
    "LINK":   0.75,
    "AAVE":   0.66
}

# ─────────────────────────────────────────────────────────────────────────────
# ABI'LER
# ─────────────────────────────────────────────────────────────────────────────

POOL_ABI = [{
    "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
    "name": "getUserAccountData",
    "outputs": [],
    "stateMutability": "view",
    "type": "function",
}]

MULTICALL3_ABI = [{
    "inputs": [
        {"internalType": "bool", "name": "requireSuccess", "type": "bool"},
        {"components": [
            {"internalType": "address", "name": "target",   "type": "address"},
            {"internalType": "bytes",   "name": "callData", "type": "bytes"},
        ], "internalType": "struct Multicall3.Call[]", "name": "calls", "type": "tuple[]"},
    ],
    "name": "tryAggregate",
    "outputs": [
        {"components": [
            {"internalType": "bool",  "name": "success",    "type": "bool"},
            {"internalType": "bytes", "name": "returnData", "type": "bytes"},
        ], "internalType": "struct Multicall3.Result[]", "name": "returnData", "type": "tuple[]"},
    ],
    "stateMutability": "view",
    "type": "function",
}]

DATA_PROVIDER_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "asset", "type": "address"},
            {"internalType": "address", "name": "user",  "type": "address"},
        ],
        "name": "getUserReserveData",
        "outputs": [
            {"internalType": "uint256", "name": "currentATokenBalance",     "type": "uint256"},
            {"internalType": "uint256", "name": "currentStableDebt",        "type": "uint256"},
            {"internalType": "uint256", "name": "currentVariableDebt",      "type": "uint256"},
            {"internalType": "uint256", "name": "principalStableDebt",      "type": "uint256"},
            {"internalType": "uint256", "name": "scaledVariableDebt",       "type": "uint256"},
            {"internalType": "uint256", "name": "stableBorrowRate",         "type": "uint256"},
            {"internalType": "uint256", "name": "liquidityRate",            "type": "uint256"},
            {"internalType": "uint40",  "name": "stableRateLastUpdated",    "type": "uint40"},
            {"internalType": "bool",    "name": "usageAsCollateralEnabled", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
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
]

ERC20_ABI = [{
    "inputs": [],
    "name": "decimals",
    "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
    "stateMutability": "view",
    "type": "function",
}]


# ─────────────────────────────────────────────────────────────────────────────
# ZİNCİR KONFİGÜRASYONU
# ─────────────────────────────────────────────────────────────────────────────

def _cs(addr: str) -> str:
    """
    EIP-55 checksum normalizasyonu.
    web3.py'nin sert checksum kontrolünden geçmek için tüm adresler
    ChainConfig oluşturulurken bu fonksiyondan geçirilir.
    """
    from web3 import Web3
    return Web3.to_checksum_address(addr)


@dataclass
class ChainConfig:
    tag:                   str
    rpc_url:               str          # HTTP RPC (fallback / The Graph için hâlâ kullanılır)
    pool_address:          str
    multicall_address:     str
    data_provider_address: str
    subgraph_url:          str
    gas_fee_usd:           float
    subgraph_schema:       str   = "positions"
    cold_interval:         int   = 300
    hot_interval:          float = 0.5

    # [v8 YENİ] WebSocket endpoint.
    # Dolu ise aave_utils.build_context() WSS üzerinden bağlanır.
    # Boş bırakılırsa HTTP fallback devreye girer.
    wss_url: str = ""

    def __post_init__(self):
        """
        Tüm kontrat adreslerini EIP-55 checksum formatına normalize et.
        URL alanları (rpc_url, subgraph_url, wss_url) dönüştürülmez.
        """
        if self.pool_address:
            self.pool_address = _cs(self.pool_address)
        if self.multicall_address:
            self.multicall_address = _cs(self.multicall_address)
        if self.data_provider_address:
            self.data_provider_address = _cs(self.data_provider_address)


def _graph_url(api_key: str, subgraph_id: str) -> str:
    """The Graph Studio API key + subgraph deployment id (Arbitrum One gateway)."""
    if not (api_key and subgraph_id):
        return ""
    return (
        f"https://gateway-arbitrum.network.thegraph.com"
        f"/api/{api_key}/subgraphs/id/{subgraph_id}"
    )


def _alchemy_wss(network: str, api_key: str) -> str:
    """
    Alchemy standart WSS URL'i oluşturur.
    Örnek: wss://arb-mainnet.g.alchemy.com/v2/<KEY>
    """
    return f"wss://{network}.g.alchemy.com/v2/{api_key}"


# [v8] Alchemy API key: .env'de tanımlıysa oradan, yoksa varsayılan key
_ALCHEMY_KEY = os.getenv(
    "ALCHEMY_API_KEY",
    "hmJIUy5LpYIq8eW4OGtmy",   # Varsayılan key — üretimde .env'e taşı!
)

# [v9] Altın WSS: ALCHEMY_WSS_URL tanımlıysa onu kullan, yoksa ARB_WSS fallback
GOLDEN_WSS_URL: str = os.getenv(
    "ALCHEMY_WSS_URL",
    os.getenv("ARB_WSS", ""),
)


def load_chains() -> List[ChainConfig]:
    """
    .env'deki RPC URL'lerine bakarak aktif zincirleri döndürür.
    RPC'si tanımlı olmayan zincir sessizce devre dışı kalır.
    """
    # https://thegraph.com/docs/en/subgraphs/querying-subgraphs/#query-a-subgraph-on-the-graph-network
    # Anahtar: The Graph Studio → API Keys. Boşsa subgraph_url boş kalır (cold scan Graph kullanmaz).
    api_key = os.getenv("GRAPH_API_KEY", "").strip().lstrip("\ufeff")
    _log.info("[CONFIG] GRAPH_API_KEY len=%d prefix=%s", len(api_key), api_key[:8] if api_key else "(boş)")
    # Multicall3: EIP-2470 standardı, tüm zincirlerde aynı adres
    MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"

    candidates = [
        # ── Arbitrum ─────────────────────────────────────────────────────────
        ChainConfig(
            tag                   = "ARB",
            rpc_url               = os.getenv("ARB_RPC", ""),
            # [v8] WSS endpoint: .env'de ARB_WSS tanımlıysa onu kullan,
            #      yoksa Alchemy varsayılan key ile otomatik oluştur.
            wss_url               = os.getenv(
                "ARB_WSS",
                _alchemy_wss("arb-mainnet", _ALCHEMY_KEY),
            ),
            pool_address          = "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
            multicall_address     = MULTICALL3,
            data_provider_address = "0x243Aa95cAC2a25651eda86e80bEe66114413c43b",
            subgraph_url          = (
                os.getenv("ARB_SUBGRAPH_URL", "").strip()
                or _graph_url(
                    api_key,
                    os.getenv("ARB_SUBGRAPH_ID", "4xyasjQeREe7PxnF6wVdobZvCw5mhoHZq3T7guRpuNPf"),
                )
            ),
            gas_fee_usd     = 0.10,
            subgraph_schema = "positions",
        ),
        # ── Base ─────────────────────────────────────────────────────────────
        ChainConfig(
            tag                   = "BASE",
            rpc_url               = os.getenv("BASE_RPC", ""),
            # [v8] WSS endpoint
            wss_url               = os.getenv(
                "BASE_WSS",
                _alchemy_wss("base-mainnet", _ALCHEMY_KEY),
            ),
            pool_address          = "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
            multicall_address     = MULTICALL3,
            data_provider_address = "0x0F43731EB8d45A581f4a36DD74F5f358bc90C73A",
            subgraph_url          = (
                os.getenv("BASE_SUBGRAPH_URL", "").strip()
                or _graph_url(
                    api_key,
                    os.getenv("BASE_SUBGRAPH_ID", "GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF"),
                )
            ),
            gas_fee_usd     = 0.05,
            subgraph_schema = "userReserves",
        ),
        # ── Optimism ─────────────────────────────────────────────────────────
        ChainConfig(
            tag                   = "OP",
            rpc_url               = os.getenv("OP_RPC", ""),
            # [v8] WSS endpoint
            wss_url               = os.getenv(
                "OP_WSS",
                _alchemy_wss("opt-mainnet", _ALCHEMY_KEY),
            ),
            pool_address          = "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
            multicall_address     = MULTICALL3,
            data_provider_address = "0x243Aa95cAC2a25651eda86e80bEe66114413c43b",
            subgraph_url          = (
                os.getenv("OP_SUBGRAPH_URL", "").strip()
                or _graph_url(
                    api_key,
                    os.getenv("OP_SUBGRAPH_ID", "3RWFxWNstn4nP3dXiDfKi9GgBoHx7xzc7APkXs1MLEgi"),
                )
            ),
            gas_fee_usd     = 0.05,
            subgraph_schema = "userReserves",
        ),
        # ── Linea ─────────────────────────────────────────────────────────────
        ChainConfig(
            tag                   = "LINEA",
            rpc_url               = os.getenv("LINEA_RPC", ""),
            wss_url               = os.getenv(
                "LINEA_WSS",
                _alchemy_wss("linea-mainnet", _ALCHEMY_KEY),
            ),
            pool_address          = "0xc47b8C00b0f69a36fa203Ffeac0334874574a8Ac",
            multicall_address     = MULTICALL3,
            data_provider_address = "0x47cd4b507B81cB831669c71c7077f4daF6762FF4",
            subgraph_url          = "",
            gas_fee_usd     = 0.03,
            subgraph_schema = "userReserves",
        ),
    ]

    active   = [c for c in candidates if c.rpc_url]
    inactive = [c.tag for c in candidates if not c.rpc_url]
    if inactive:
        _log.warning("RPC eksik, devre dışı: %s", inactive)
    for c in active:
        if c.tag in ("ARB", "BASE", "OP") and not (c.subgraph_url or "").strip():
            _log.warning(
                "%s: subgraph_url boş — GRAPH_API_KEY veya %s_SUBGRAPH_URL / id eksik; "
                "watcher cold scan borçlu listesini Graph ile çekemez.",
                c.tag,
                c.tag,
            )
    return active


# ─────────────────────────────────────────────────────────────────────────────
# FİLTRE SABİTLERİ
# ─────────────────────────────────────────────────────────────────────────────

HF_LIQUIDATABLE  = 1.00
HF_HOT_UPPER     = 1.05
HF_HOT_REMOVE    = 1.05
# [v7] HF_ZOMBIE_MIN modül düzeyi export — aave_utils.py dinamik close factor için içe aktarır.
# HF < HF_ZOMBIE_MIN → CLOSE_FACTOR = 1.0 (tam tasfiye)
# HF >= HF_ZOMBIE_MIN → CLOSE_FACTOR = 0.5 (%50 kural)
HF_ZOMBIE_MIN    = 0.95

MIN_DEBT_USD     = 100.0
MIN_PROFIT_USD   = 5.0      # Bu değerin altı → "Kârsız", hot_list'e ALINMAZ

FLASH_LOAN_FEE   = 0.0005
DEX_SLIPPAGE     = 0.003
CLOSE_FACTOR     = 0.5

GRAPH_BATCH_SIZE = 1000
COLD_CHUNK_SIZE  = 1000
HOT_CHUNK_SIZE   = 300

WAD          = 10 ** 18
USD_DECIMALS = 10 ** 8


# ─────────────────────────────────────────────────────────────────────────────
# VARLIK SINIFLANDIRMASI
# ─────────────────────────────────────────────────────────────────────────────

ASSET_CLASS: Dict[str, str] = {
    # Stablecoinler
    "USDC":    "STABLE", "USDC.e": "STABLE", "USDCe":  "STABLE",
    "USDT":    "STABLE", "DAI":    "STABLE", "LUSD":   "STABLE",
    "FRAX":    "STABLE", "GHO":    "STABLE", "crvUSD": "STABLE",
    "USDS":    "STABLE", "EURC":   "STABLE", "EURA":   "STABLE",
    "PYUSD":   "STABLE",
    # On-chain Unicode / bridge varyantları
    "USD₮0":   "STABLE", "USDT0":  "STABLE", "USDt":   "STABLE",
    "USDT.e":  "STABLE", "USDTe":  "STABLE",
    "USDC0":   "STABLE", "USDCn":  "STABLE",
    # ETH ailesi
    "WETH":    "ETH",    "ETH":    "ETH",    "wstETH": "ETH",
    "rETH":    "ETH",    "cbETH":  "ETH",    "weETH":  "ETH",
    "ezETH":   "ETH",    "rsETH":  "ETH",    "osETH":  "ETH",
    "ankrETH": "ETH",    "sfrxETH":"ETH",
    # BTC ailesi
    "WBTC":    "BTC",    "BTC":    "BTC",    "cbBTC":  "BTC",
    "tBTC":    "BTC",
    # Governance / Alt
    "LINK":    "ALT",    "AAVE":   "ALT",    "UNI":    "ALT",
    "CRV":     "ALT",    "BAL":    "ALT",    "SNX":    "ALT",
    "MKR":     "ALT",    "LDO":    "ALT",    "RPL":    "ALT",
    "ARB":     "ALT",    "OP":     "ALT",    "GMX":    "ALT",
    "WLD":     "ALT",    "COMP":   "ALT",
}

EMODE_BONUS = 0.01  # Aynı sınıf borç+teminat → %1

LIQUIDATION_BONUS_MAP: Dict[str, float] = {
    # Stablecoinler — %5
    "USDC": 0.05, "USDC.e": 0.05, "USDCe": 0.05,
    "USDT": 0.05, "DAI":    0.05, "LUSD":  0.05,
    "FRAX": 0.05, "GHO":    0.05, "crvUSD":0.05,
    "USDS": 0.05, "EURC":   0.05, "PYUSD": 0.05,
    # On-chain Unicode / bridge varyantları
    "USD₮0": 0.05, "USDT0": 0.05, "USDt":   0.05,
    "USDT.e":0.05, "USDTe": 0.05,
    "USDC0": 0.05, "USDCn": 0.05,
    # ETH ailesi — %5-7.5
    "WETH":    0.05, "ETH":     0.05,
    "wstETH":  0.06, "rETH":    0.06, "cbETH":   0.06,
    "weETH":   0.06, "ezETH":   0.075,"rsETH":   0.075,
    "osETH":   0.075,"sfrxETH": 0.075,
    # BTC ailesi — %7
    "WBTC": 0.07, "BTC": 0.07, "cbBTC": 0.07, "tBTC": 0.07,
    # Alt — %7.5-10
    "LINK": 0.08, "AAVE": 0.075, "UNI":  0.08,
    "CRV":  0.10, "BAL":  0.10,  "SNX":  0.10,
    "MKR":  0.075,"LDO":  0.075, "RPL":  0.10,
    "ARB":  0.075,"OP":   0.075, "GMX":  0.10,
    "COMP": 0.10, "WLD":  0.10,
}
DEFAULT_BONUS = 0.05

WHITELIST_SYMBOLS: Set[str] = {
    "USDC", "USDC.e", "USDCe", "USDT", "DAI", "LUSD", "FRAX",
    "GHO", "crvUSD", "USDS", "EURC", "EURA", "PYUSD",
    "USD₮0", "USDT0", "USDt", "USDT.e", "USDTe", "USDC0", "USDCn",
    "WETH", "ETH", "wstETH", "rETH", "cbETH", "weETH",
    "ezETH", "rsETH", "osETH", "sfrxETH", "ankrETH",
    "WBTC", "BTC", "cbBTC", "tBTC",
    "LINK", "AAVE", "UNI", "CRV", "BAL", "SNX",
    "MKR", "LDO", "RPL", "ARB", "OP", "GMX", "COMP", "WLD",
}