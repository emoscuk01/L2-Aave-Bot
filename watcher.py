"""
watcher.py — WSS Pub/Sub + Ghost Recovery + JSON Export + OptiPair Engine v13
------------------------------------------------------------------------------
Kullanım:
    python watcher.py ARB
    python watcher.py BASE
    python watcher.py OP
    python watcher.py           # Tüm zincirler

──────────────────────────────────────────────────────────────────────────────
v12 → v13 Değişiklik Logu — ZEKA KATMANI (OptiPairEngine)

PROBLEM (Godzilla Vakası):
  Eski sistem _usd_estimate() ile USD dağılımını oracle fiyatına DEĞİL,
  normalize token SAYISINA göre yapıyordu:
    Cüzdan: 10 WETH ($30.000) + 31 ARB ($31) teminat
    normalize_WETH = 10.0,  normalize_ARB = 31.0,  total = 41.0
    ARB'ye atanan USD  = 31/41 * 30.031 ≈ $22.700  ← TAMAMEN YANLIŞ
    WETH'e atanan USD  = 10/41 * 30.031 ≈  $7.300  ← TAMAMEN YANLIŞ
  Sonuç: bot $31'lık ARB'yi "optimal teminat" seçiyor,
         $30.000'lık WETH'i görmezden geliyor → Pair = ARB/WETH → eksi marj.

ÇÖZÜM — OptiPairEngine (v13):
  1. AaveOracle.getAssetsPrices() ile gerçek Chainlink fiyatı alınır:
       amount_usd = (raw / 10**decimals) × (oracle_price / 10**8)
       10 WETH × $3.000 = $30.000  ✓    31 ARB × $1 = $31  ✓
  2. Dust filtresi: < $100 olan pozisyonlar analizden çıkar.
     (31 ARB = $31 → dust → WETH seçilir)
  3. E-Mode tespiti: pool.getUserEModeCategory() ile kategori belirlenir.
     E-Mode aktifse yalnızca aynı kategorideki çiftler değerlendirilir.
  4. Likidite-Ağırlıklı Slippage Matrisi: flat %0.3 yerine gerçekçi değerler:
       STABLE/STABLE: %0.05   ETH/STABLE: %0.30   ALT/ALT: %2.00
     $2.000 kâr + %10 slippage  <  $1.500 kâr + %0.05 slippage → doğru seçim.
  5. Tüm (debt_i, coll_j) kombinasyonları puanlanır, en yüksek net_score kazanır.
  6. cluster_sniper.py için hazır JSON payload (to_dict() / get_payload()).
  7. Oracle cache: cold=60s / hot=13s (~1 blok) → RPC yükü minimal.
  8. Fallback: oracle veya zenginleştirme başarısızsa eski discover_target().

DOKUNULMAYAN v12 ÖZELLİKLERİ:
  TargetsStore JSON köprüsü, Ghost Recovery nonce kontrolü,
  WSS newHeads abonesi, auto-reconnect backoff, hot_scan_lock.

OPSİYONEL .env — HOT THROTTLE / CU:
  WATCHER_HOT_MIN_INTERVAL_SEC, WATCHER_HOT_EVERY_N_BLOCKS (ARB üzerinde ~4 blok/s için varsayılan 1.0s + 4 blok),
  WATCHER_HOT_POLL_INTERVAL_SEC (HTTP fallback hot poll),
  ALCHEMY_CU_* — tahmini CU sayacı (alchemy_cu_meter.py).
──────────────────────────────────────────────────────────────────────────────
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import websockets
from dotenv import load_dotenv

import config as cfg
from aave_utils import (
    ChainContext, WalletSnapshot,
    AssetPosition, TargetInfo,
    build_context,
    multicall_account_data,
    discover_target,           # OptiPairEngine'in fallback'i
    build_profit_from_info,
    load_reserve_cache,
    log_opportunity,
    log_hot_list_add,
)
from alchemy_cu_meter import cu_try_aggregate, log_cu_meter_banner_once, record_rpc_cu
from rpc_rotator import (
    RPCRateLimited429,
    is_rate_limit_429,
    call_with_retry,
    get_rotator,
)
from wss_url_pool import (
    WssUrlPool,
    build_wss_urls_for_watcher,
    warn_if_single_endpoint,
    wss_transport_limited,
)
from telegram_utils import (
    TelegramNotifier,
    fmt_scan_start, fmt_scan_done,
    fmt_hot_list_add, fmt_opportunity,
    fmt_autopsy_liquidated, fmt_autopsy_repaid,
)

load_dotenv(override=True)

# ── Loglama ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(
            stream=open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
        )
    ],
)
logger = logging.getLogger(__name__)
log_cu_meter_banner_once()

# ── Telegram ──────────────────────────────────────────────────────────────────
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
tg = TelegramNotifier(chat_id=_CHAT_ID)

# ── API Kalkanı sabitleri ─────────────────────────────────────────────────────
NULL_STREAK_LIMIT = 3
MAX_SKIP_LIMIT    = 150_000
AUTOPSY_DEBT_DROP = 0.40
AUTOPSY_COLL_DROP = 0.30
MULTICALL_BATCH_SIZE = int(os.getenv("WATCHER_MULTICALL_BATCH_SIZE", "50"))
MULTICALL_THROTTLE_SEC = float(os.getenv("WATCHER_MULTICALL_THROTTLE_SEC", "0.2"))
SCAN_TAIL_SLEEP_SEC = 60
RATE_LIMIT_COOLDOWN_SEC = 20
MIN_COLD_SLEEP_SEC   = int(os.getenv("WATCHER_MIN_COLD_SLEEP_SEC", "60"))

# Arbitrum One: sequencer blokları tipik olarak ~0.25 s aralıkla üretilir (≈4 blok/s).
# Süre zincir yüküyle dalgalanır; kesin sabit bir blok süresi yoktur (Kaynak: Arb dokümantasyonu / nonce ile ölçüm).
# Her newHeads'te hot_scan → CU/s ve 429; zaman + blok stride ile seyreltme.
WATCHER_HOT_MIN_INTERVAL_SEC = float(os.getenv("WATCHER_HOT_MIN_INTERVAL_SEC", "1.0"))
WATCHER_HOT_EVERY_N_BLOCKS = max(0, int(os.getenv("WATCHER_HOT_EVERY_N_BLOCKS", "4")))

# ── WSS sabitleri ─────────────────────────────────────────────────────────────
WSS_PING_INTERVAL   = 30
WSS_PING_TIMEOUT    = 15
WSS_CLOSE_TIMEOUT   = 5
WSS_MAX_MSG_SIZE    = 2**20
WSS_INITIAL_BACKOFF = 2
WSS_MAX_BACKOFF     = 60

# ── JSON Export sabitleri ─────────────────────────────────────────────────────
# cluster_sniper.TARGETS_JSON_PATH ile aynı env (varsayılan: ./targets.json, Git'te yok)
TARGETS_FILE      = os.getenv("TARGETS_JSON", "targets.json")
TARGETS_MAX_QUEUE = 2048


# ═════════════════════════════════════════════════════════════════════════════
# ZEKA KATMANI ── OptiPairEngine v1
# ═════════════════════════════════════════════════════════════════════════════

# ── AaveOracle adresleri (Chainlink tabanlı, zincir başına) ───────────────────
AAVE_ORACLE_ADDRESSES: Dict[str, str] = {
    "ARB":   "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7",
    "BASE":  "0x2Cc0Fc26eD4563A5ce5e8bdcfe1A2878676Ae156",
    "OP":    "0xD81eb3728a631871a7eBBaD631b5f424909f0c77",
    "LINEA": "0xCFDAdA7DCd2e785cF706BaDBC2B8Af5084d595e9",
}

# AaveOracle ABI — yalnızca getAssetsPrices gerekli
AAVE_ORACLE_ABI: List[Dict] = [{
    "inputs":  [{"internalType": "address[]", "name": "assets", "type": "address[]"}],
    "name":    "getAssetsPrices",
    "outputs": [{"internalType": "uint256[]", "name": "",       "type": "uint256[]"}],
    "stateMutability": "view",
    "type":    "function",
}]

# ── Pool E-Mode ABI ───────────────────────────────────────────────────────────
# [v14] uint8 → uint256 düzeltmesi + getEModeCategoryData eklendi.
# getUserEModeCategory: Kullanıcının aktif E-Mode kategorisini döndürür.
# getEModeCategoryData: Kategori ID → (ltv, liqThreshold, liqBonus, priceSource, label)
POOL_EMODE_ABI: List[Dict] = [
    {
        "inputs":  [{"internalType": "address", "name": "user", "type": "address"}],
        "name":    "getUserEModeCategory",
        "outputs": [{"internalType": "uint256", "name": "",     "type": "uint256"}],
        "stateMutability": "view",
        "type":    "function",
    },
    {
        "inputs":  [{"internalType": "uint8", "name": "id", "type": "uint8"}],
        "name":    "getEModeCategoryData",
        "outputs": [{
            "components": [
                {"internalType": "uint16",  "name": "ltv",                  "type": "uint16"},
                {"internalType": "uint16",  "name": "liquidationThreshold", "type": "uint16"},
                {"internalType": "uint16",  "name": "liquidationBonus",     "type": "uint16"},
                {"internalType": "address", "name": "priceSource",          "type": "address"},
                {"internalType": "string",  "name": "label",                "type": "string"},
            ],
            "internalType": "struct DataTypes.EModeCategory",
            "name": "",
            "type": "tuple",
        }],
        "stateMutability": "view",
        "type":    "function",
    },
]

# ── DataProvider E-Mode ABI ───────────────────────────────────────────────────
# [v14] Her rezerv tokenının hangi E-Mode kategorisine ait olduğunu döndürür.
RESERVE_EMODE_ABI: List[Dict] = [{
    "inputs":  [{"internalType": "address", "name": "asset", "type": "address"}],
    "name":    "getReserveEModeCategory",
    "outputs": [{"internalType": "uint256", "name": "",      "type": "uint256"}],
    "stateMutability": "view",
    "type":    "function",
}]

# ── Likidite-Ağırlıklı Slippage Matrisi ──────────────────────────────────────
# Çiftin asset_class sınıflarına göre gerçekçi slippage katsayısı.
# Flat 0.003 yerine market-aware değerler kullanılır.
#
# Mantık sıralaması (düşük→yüksek slippage):
#   STABLE/STABLE → Curve stableswap, kaymaz      %0.05
#   ETH/ETH       → wstETH-WETH Balancer, minimal  %0.10
#   BTC/BTC       → tBTC-WBTC Curve                %0.20
#   ETH/STABLE    → UniV3 ETH/USDC derin havuz     %0.30 (standart)
#   ALT/STABLE    → orta likidite                  %0.80
#   ALT/ETH       → orta-düşük likidite            %1.00
#   ALT/ALT       → düşük likidite                 %2.00
#   bilinmeyen    → TKN_ veya egzotik               %2.50
CLASS_SLIPPAGE: Dict[Tuple[str, str], float] = {
    ("STABLE", "STABLE"): 0.0005,
    ("ETH",    "ETH"):    0.001,
    ("BTC",    "BTC"):    0.002,
    ("ETH",    "STABLE"): 0.003,
    ("STABLE", "ETH"):    0.003,
    ("BTC",    "STABLE"): 0.003,
    ("STABLE", "BTC"):    0.003,
    ("ETH",    "BTC"):    0.005,
    ("BTC",    "ETH"):    0.005,
    ("ALT",    "STABLE"): 0.008,
    ("STABLE", "ALT"):    0.008,
    ("ALT",    "ETH"):    0.010,
    ("ETH",    "ALT"):    0.010,
    ("ALT",    "BTC"):    0.012,
    ("BTC",    "ALT"):    0.012,
    ("ALT",    "ALT"):    0.020,
}
DEFAULT_PAIR_SLIPPAGE = 0.025   # TKN_ ve bilinmeyen tokenlar için güvenli alt sınır

# Per-asset dust filtresi — $100 altı pozisyonlar analize girmez
# 31 ARB ≈ $31 → DUST → listeden çıkar → WETH seçilir
OPTI_DUST_USD = 100.0

# ── Volatilite Etki Filtresi (Stable-Heavy Guard) ────────────────────────────
# Toplam portföydeki volatil (STABLE olmayan) varlıkların USD oranı bu eşiğin
# altındaysa cüzdan "Stable-Heavy" kabul edilir ve fiyat hareketiyle tasfiye
# edilemez — yalnızca faiz birikimiyle HF düşebilir (aylar sürer).
# Örnek: $37k USDC + $30k USDT + $500 WBTC + $500 ARB → volatil oran ≈ %1.5
STABLE_HEAVY_VOLATILE_RATIO = 0.10

# ── Stable-Pair Bonus Fallback ────────────────────────────────────────────────
# STABLE/STABLE tasfiyelerinde (USDC borç → USDT teminat vb.) Aave V3 E-Mode
# bonusu genellikle %1 (10100). On-chain verisi yoksa bu sabit kullanılır.
# Gerçek bonus DEFAULT_BONUS (%5) değildir — %5 ile hesap yapmak hayali kâr üretir.
STABLE_PAIR_BONUS = 0.01

# Oracle fiyat cache TTL (saniye)
ORACLE_TTL_COLD = 60.0   # Cold scan: aynı tarama döngüsünde taze
ORACLE_TTL_HOT  = 13.0   # Hot scan: ~1 blok (~13s Arbitrum), her blokta taze


# ── Veri Yapıları ─────────────────────────────────────────────────────────────

@dataclass
class EnrichedPosition:
    """
    Oracle fiyatıyla zenginleştirilmiş tek bir Aave pozisyonu.
    amount_usd = (raw / 10**decimals) × oracle_price  ← Godzilla düzeltmesi
    """
    symbol:          str
    underlying_addr: str
    decimals:        int
    amount_raw:      int
    amount_usd:      float    # GERÇEK oracle fiyatıyla hesaplanan USD
    asset_class:     str      # "STABLE" | "ETH" | "BTC" | "ALT"


@dataclass
class ScoredPair:
    """
    Tek bir (debt, collateral) çiftinin tam puanlama çıktısı.

    net_score = gross − flash_fee − liquidity_adjusted_slip − gas
    to_dict() → cluster_sniper.py için hazır JSON payload.
    """
    debt:                    EnrichedPosition
    coll:                    EnrichedPosition
    bonus:                   float
    position_type:           str             # "E-MODE" | "HEDGE" | "NORMAL"
    effective_close_factor:  float
    debt_cover_usd:          float           # close factor + bottleneck uygulanmış
    debt_to_cover_wei:       int             # liquidationCall parametresi
    coll_to_receive_usd:     float
    gross_profit:            float
    adjusted_slippage_usd:   float           # likidite-ağırlıklı
    flash_fee_usd:           float
    net_score:               float           # sıralama kriteri
    emode_category:          int
    target_address:          str = ""

    def to_dict(self) -> Dict[str, Any]:
        """
        cluster_sniper.py'ın itiraz etmeden kabul edeceği temiz JSON payload.
        target_address, best_collateral_address, best_debt_address,
        estimated_profit_usd ve tüm yardımcı alanları içerir.
        """
        return {
            # ── Zorunlu alanlar (cluster_sniper.py beklentisi) ────────────────
            "target_address":           self.target_address,
            "best_collateral_address":  self.coll.underlying_addr,
            "best_debt_address":        self.debt.underlying_addr,
            "estimated_profit_usd":     round(self.net_score, 4),
            # ── Yardımcı alanlar (log / UI / debug) ───────────────────────────
            "collateral_symbol":        self.coll.symbol,
            "debt_symbol":              self.debt.symbol,
            "debt_cover_usd":           round(self.debt_cover_usd, 4),
            "debt_to_cover_wei":        self.debt_to_cover_wei,
            "gross_profit_usd":         round(self.gross_profit, 4),
            "flash_fee_usd":            round(self.flash_fee_usd, 4),
            "adjusted_slippage_usd":    round(self.adjusted_slippage_usd, 4),
            "bonus_pct":                round(self.bonus * 100, 2),
            "position_type":            self.position_type,
            "effective_close_factor":   self.effective_close_factor,
            "emode_category":           self.emode_category,
            "collateral_available_usd": round(self.coll.amount_usd, 2),
            "debt_total_usd":           round(self.debt.amount_usd, 2),
        }


class OptiPairEngine:
    """
    Aave V3 Optimal Pair Seçim Motoru (v13)
    ─────────────────────────────────────────
    aave_utils.discover_target()'ın yerine geçen zeka katmanı.

    Temel fark: token USD değerlerini oracle fiyatıyla hesaplar,
    tüm (debt, coll) kombinasyonlarını likidite-ağırlıklı net_score
    ile puanlar ve en kârlı + en likit çifti döndürür.

    Godzilla Vakası çözümü:
      31 ARB × $1.00 = $31 USD   → DUST → filtrele
      10 WETH × $3.000 = $30.000 → SEÇILDI ✓

    Kullanım:
        info    = await opti_engine.analyze(ctx, address, hf, debt_usd, coll_usd)
        payload = opti_engine.get_payload(address)   # cluster_sniper JSON'u
    """

    def __init__(self) -> None:
        # chain_tag → {token_addr.lower(): price_usd (float)}
        self._price_cache: Dict[str, Dict[str, float]] = {}
        self._price_ts:    Dict[str, float]            = {}
        # chain_tag → oracle kontrat nesnesi
        self._oracle_contracts: Dict[str, Any]         = {}
        # address → son ScoredPair (get_payload() için)
        self._last_scores: Dict[str, ScoredPair]       = {}
        # [v14] E-Mode on-chain cache
        # "TAG_catId" → bonus (float, örn. 0.01)
        self._emode_bonus_cache: Dict[str, float]       = {}
        # chain_tag → {token_addr.lower(): emode_category_id (int)}
        self._reserve_emode_cache: Dict[str, Dict[str, int]] = {}
        self._reserve_emode_ts:    Dict[str, float]          = {}
        # [v15] Stable-Heavy Volatilite Etki Filtresi
        self._stable_heavy_addrs: Set[str]                   = set()

    # ─── Public API ───────────────────────────────────────────────────────────

    async def analyze(
        self,
        ctx:            ChainContext,
        address:        str,
        hf:             float,
        total_debt_usd: float,
        total_coll_usd: float,
        hot_mode:       bool = False,
    ) -> Optional[TargetInfo]:
        """
        Bir hedef adres için oracle-fiyatlı optimal pair analizi.

        Adım adım:
          1. AaveOracle.getAssetsPrices() → gerçek fiyat haritası (TTL cache)
          2. pool.getUserEModeCategory()  → E-Mode kategori tespiti
          3. getUserReserveData multicall → ham bakiyeler
          4. amount_usd = (raw / 10**dec) × price  ← Godzilla düzeltmesi
          5. Dust filtresi: < $100 → çıkar
          6. Tüm (debt_i × coll_j) permütasyonlarını puanla
          7. En yüksek net_score → TargetInfo olarak döndür

        Fallback koşulları (eski discover_target() çağrılır):
          - Oracle adresi tanımlı değil
          - getAssetsPrices() RPC hatası
          - Enrich sonrası hiç borç veya teminat kalmadı

        hot_mode=True → oracle TTL = 13s (1 blok, taze fiyat)
        """
        tag = ctx.config.tag
        ttl = ORACLE_TTL_HOT if hot_mode else ORACLE_TTL_COLD

        # ── 1. Oracle fiyat haritasını al ─────────────────────────────────────
        price_map = await self._fetch_oracle_prices(ctx, ttl)
        if not price_map:
            logger.warning(
                "[%s-OPTI] Oracle boş → fallback: %s", tag, address[:10],
            )
            return await discover_target(ctx, address, hf, total_debt_usd, total_coll_usd)

        # ── 2. E-Mode tespiti ─────────────────────────────────────────────────
        emode_cat = await self._detect_emode(ctx, address)

        # ── 2b. E-Mode on-chain veri çekimi ───────────────────────────────────
        emode_bonus: float = 0.0
        reserve_emode_map: Dict[str, int] = {}
        if emode_cat > 0:
            emode_bonus     = await self._fetch_emode_bonus(ctx, emode_cat)
            reserve_emode_map = await self._fetch_reserve_emode_map(ctx)

        # ── 3-4-5. Oracle-fiyatlı pozisyon zenginleştirme + dust filtresi ─────
        debts, colls = await self._enrich_positions(ctx, address, price_map)

        if not debts or not colls:
            logger.warning(
                "[%s-OPTI] Zenginleştirme sonrası yetersiz pozisyon | %s | "
                "borç=%d teminat=%d → fallback",
                tag, address[:10], len(debts), len(colls),
            )
            return await discover_target(ctx, address, hf, total_debt_usd, total_coll_usd)

        # ── 5b. Stable-Heavy filtresi ─────────────────────────────────────
        if self._check_volatile_impact(debts, colls, address, tag):
            return None

        # ── 6. Tüm parileri puanla ────────────────────────────────────────────
        scored = self._score_all_pairs(
            debts, colls, hf,
            emode_cat, emode_bonus, reserve_emode_map,
            ctx.config.gas_fee_usd,
        )

        if not scored:
            logger.warning(
                "[%s-OPTI] Puanlanabilir pari yok "
                "(E-Mode filtre / tüm dust?) | %s",
                tag, address[:10],
            )
            return None

        best = scored[0]
        best.target_address = address
        self._last_scores[address] = best

        # Seçim özet logu
        logger.info(
            "[%s-OPTI] ✅ Optimal Pari | %s | "
            "BORÇ: %s $%.2f → TEMİNAT: %s $%.2f | "
            "E-Mode: %s | Slip: $%.3f | Net: $%.2f",
            tag, address[:10],
            best.debt.symbol, best.debt.amount_usd,
            best.coll.symbol, best.coll.amount_usd,
            f"Kat.{emode_cat}" if emode_cat else "Yok",
            best.adjusted_slippage_usd,
            best.net_score,
        )

        # DEBUG: alternatif parileri sıralamayla göster
        if logger.isEnabledFor(logging.DEBUG) and len(scored) > 1:
            for rank, s in enumerate(scored[:8], start=1):
                logger.debug(
                    "[%s-OPTI] #%d %s/%s | gross=$%.2f slip=$%.3f net=$%.2f [%s]",
                    tag, rank,
                    s.debt.symbol, s.coll.symbol,
                    s.gross_profit, s.adjusted_slippage_usd,
                    s.net_score, s.position_type,
                )

        # ── 7. TargetInfo'ya dönüştür (downstream uyumluluğu tam) ────────────
        return TargetInfo(
            address                    = address,
            hf                         = hf,
            total_debt_usd             = total_debt_usd,
            total_coll_usd             = total_coll_usd,
            debt_asset                 = best.debt.symbol,
            debt_asset_address         = best.debt.underlying_addr,
            debt_token_total_usd       = best.debt.amount_usd,
            debt_amount_usd            = best.debt_cover_usd,
            debt_to_cover_wei          = best.debt_to_cover_wei,
            collateral_asset           = best.coll.symbol,
            collateral_asset_address   = best.coll.underlying_addr,
            collateral_token_total_usd = best.coll.amount_usd,
            collateral_usd             = best.coll_to_receive_usd,
            bonus                      = best.bonus,
            position_type              = best.position_type,
            effective_close_factor     = best.effective_close_factor,
            all_debts = [
                AssetPosition(
                    symbol          = d.symbol,
                    underlying_addr = d.underlying_addr,
                    decimals        = d.decimals,
                    amount_raw      = d.amount_raw,
                    amount_usd      = d.amount_usd,
                )
                for d in debts
            ],
            all_collaterals = [
                AssetPosition(
                    symbol          = c.symbol,
                    underlying_addr = c.underlying_addr,
                    decimals        = c.decimals,
                    amount_raw      = c.amount_raw,
                    amount_usd      = c.amount_usd,
                )
                for c in colls
            ],
        )

    def get_payload(self, address: str) -> Optional[Dict[str, Any]]:
        """
        cluster_sniper.py için hazır JSON payload dict.
        analyze() çağrılmadan önce None döner.
        """
        score = self._last_scores.get(address)
        return score.to_dict() if score else None

    def invalidate_address(self, address: str) -> None:
        """Hot_list'ten çıkarılan adresin stale score cache'ini temizle."""
        self._last_scores.pop(address, None)
        self._stable_heavy_addrs.discard(address)

    def is_stable_heavy(self, address: str) -> bool:
        """Adresin Stable-Heavy filtresiyle işaretlenip işartelenmediğini döndür."""
        return address in self._stable_heavy_addrs

    # ─── Özel metodlar ────────────────────────────────────────────────────────

    async def _fetch_oracle_prices(
        self,
        ctx: ChainContext,
        ttl: float,
    ) -> Dict[str, float]:
        """
        AaveOracle.getAssetsPrices(all_reserve_addrs)
        Döndürür: {token_addr.lower(): price_usd (float, 8-dec normalize)}

        TTL: cold=60s, hot=13s
        Hata: eski cache döner (bayat ama bozuk değil); fallback düşülür.
        """
        tag = ctx.config.tag
        now = time.monotonic()

        # Cache geçerliyse direkt dön
        if (
            tag in self._price_ts
            and (now - self._price_ts[tag]) < ttl
            and tag in self._price_cache
        ):
            return self._price_cache[tag]

        oracle = await self._get_oracle(ctx)
        if oracle is None:
            logger.warning("[%s-OPTI] Oracle kontrat yok — tanımlı değil.", tag)
            return self._price_cache.get(tag, {})

        reserves = ctx.reserve_cache
        if not reserves:
            return {}

        token_addrs = [info[0] for info in reserves.values()]

        try:
            from web3 import AsyncWeb3 as _W3
            cs_addrs   = [_W3.to_checksum_address(a) for a in token_addrs]
            rot = get_rotator()
            raw_prices = await call_with_retry(
                lambda: oracle.functions.getAssetsPrices(cs_addrs).call(),
                rot, ctx.w3,
                context_label=f"{tag}-ORACLE",
                estimated_cu=cu_try_aggregate(1),
            )
        except Exception as exc:
            logger.error("[%s-OPTI] getAssetsPrices RPC hatası: %s", tag, exc)
            return self._price_cache.get(tag, {})

        price_map: Dict[str, float] = {}
        zero_cnt = 0
        for addr, raw_p in zip(token_addrs, raw_prices):
            if raw_p > 0:
                price_map[addr.lower()] = float(raw_p) / 1e8
            else:
                zero_cnt += 1

        self._price_cache[tag] = price_map
        self._price_ts[tag]    = now

        logger.debug(
            "[%s-OPTI] Oracle cache yenilendi: %d fiyat, %d sıfır",
            tag, len(price_map), zero_cnt,
        )
        return price_map

    async def _get_oracle(self, ctx: ChainContext) -> Optional[Any]:
        """AaveOracle kontrat nesnesini döndür (zincir başına bir kez oluşturulur)."""
        tag = ctx.config.tag
        if tag not in self._oracle_contracts:
            oracle_addr = AAVE_ORACLE_ADDRESSES.get(tag)
            if not oracle_addr:
                self._oracle_contracts[tag] = None
                return None
            from web3 import AsyncWeb3 as _W3
            self._oracle_contracts[tag] = ctx.w3.eth.contract(
                address=_W3.to_checksum_address(oracle_addr),
                abi=AAVE_ORACLE_ABI,
            )
        return self._oracle_contracts[tag]

    async def _detect_emode(self, ctx: ChainContext, address: str) -> int:
        """
        pool.getUserEModeCategory(address) → int

        [v14] ABI düzeltmesi: uint8 → uint256
        0 = Normal mod (E-Mode yok)
        >0 = Dinamik kategori ID (zincire göre değişir)
        """
        try:
            from web3 import AsyncWeb3 as _W3
            pool = ctx.w3.eth.contract(
                address=ctx.config.pool_address,
                abi=POOL_EMODE_ABI,
            )
            rot = get_rotator()
            cat: int = await call_with_retry(
                lambda: pool.functions.getUserEModeCategory(
                    _W3.to_checksum_address(address)
                ).call(),
                rot, ctx.w3,
                context_label="EMODE",
                estimated_cu=cu_try_aggregate(1),
            )
            if cat > 0:
                logger.debug(
                    "[OPTI] E-Mode Kat.%d tespit | %s", cat, address[:10],
                )
            return cat
        except Exception as exc:
            logger.debug("[OPTI] getUserEModeCategory hatası (fallback 0): %s", exc)
            return 0

    async def _fetch_emode_bonus(
        self, ctx: ChainContext, category_id: int,
    ) -> float:
        """
        [v14] Pool.getEModeCategoryData(cat_id) → on-chain tasfiye bonusu.

        Aave V3 bonusu 10000 bazlı:
          10100 → %1 bonus (0.01)
          10500 → %5 bonus (0.05)

        Sonuç RAM'de sonsuz cache'lenir (governance ile değişir, nadir).
        Hata alınırsa config.EMODE_BONUS sabitine düşer.
        """
        tag       = ctx.config.tag
        cache_key = f"{tag}_{category_id}"

        cached = self._emode_bonus_cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            pool = ctx.w3.eth.contract(
                address=ctx.config.pool_address,
                abi=POOL_EMODE_ABI,
            )
            rot = get_rotator()
            result = await call_with_retry(
                lambda: pool.functions.getEModeCategoryData(
                    category_id
                ).call(),
                rot, ctx.w3,
                context_label=f"{tag}-EMODE-BONUS",
                estimated_cu=cu_try_aggregate(1),
            )
            raw_bonus   = result[2]          # uint16 liquidationBonus
            label       = result[4]          # string label
            bonus_float = (raw_bonus - 10000) / 10000

            self._emode_bonus_cache[cache_key] = bonus_float
            logger.info(
                "[%s-OPTI] E-Mode Kat.%d ON-CHAIN | label='%s' | "
                "bonus=%.2f%% (raw: %d)",
                tag, category_id, label, bonus_float * 100, raw_bonus,
            )
            return bonus_float

        except Exception as exc:
            logger.warning(
                "[%s-OPTI] getEModeCategoryData(%d) hatası: %s → "
                "fallback config.EMODE_BONUS",
                tag, category_id, exc,
            )
            from config import EMODE_BONUS
            self._emode_bonus_cache[cache_key] = EMODE_BONUS
            return EMODE_BONUS

    async def _fetch_reserve_emode_map(
        self, ctx: ChainContext,
    ) -> Dict[str, int]:
        """
        [v14] Multicall: DataProvider.getReserveEModeCategory(asset)
        Tüm rezerv tokenlarının E-Mode kategori ID'lerini toplu sorgular.

        Dönen harita: {token_addr.lower(): category_id}
        Cache TTL: 300s (cold scan arası).
        """
        tag = ctx.config.tag
        now = time.monotonic()

        if (tag in self._reserve_emode_ts
                and (now - self._reserve_emode_ts[tag]) < 300.0
                and tag in self._reserve_emode_cache):
            return self._reserve_emode_cache[tag]

        reserves = ctx.reserve_cache
        if not reserves:
            return self._reserve_emode_cache.get(tag, {})

        from web3 import AsyncWeb3 as _W3
        from eth_abi import decode as _abi_decode

        token_addrs = [info[0] for info in reserves.values()]

        dp_contract = ctx.w3.eth.contract(
            address=ctx.config.data_provider_address,
            abi=RESERVE_EMODE_ABI,
        )

        calls = [
            (
                ctx.config.data_provider_address,
                dp_contract.encode_abi(
                    "getReserveEModeCategory",
                    args=[_W3.to_checksum_address(addr)],
                ),
            )
            for addr in token_addrs
        ]

        try:
            rot = get_rotator()
            results = await call_with_retry(
                lambda: ctx.multicall_contract.functions.tryAggregate(
                    False, calls,
                ).call(),
                rot, ctx.w3,
                context_label=f"{tag}-EMODE-MAP",
                estimated_cu=cu_try_aggregate(len(calls)),
            )
        except Exception as exc:
            logger.warning(
                "[%s-OPTI] Reserve E-Mode multicall hatası: %s", tag, exc,
            )
            return self._reserve_emode_cache.get(tag, {})

        emode_map: Dict[str, int] = {}
        for addr, (ok, ret) in zip(token_addrs, results):
            if ok and ret:
                try:
                    (cat_id,) = _abi_decode(["uint256"], ret)
                    emode_map[addr.lower()] = cat_id
                except Exception:
                    emode_map[addr.lower()] = 0
            else:
                emode_map[addr.lower()] = 0

        self._reserve_emode_cache[tag] = emode_map
        self._reserve_emode_ts[tag]    = now

        non_zero = {k: v for k, v in emode_map.items() if v > 0}
        logger.debug(
            "[%s-OPTI] Reserve E-Mode haritası: %d/%d token E-Mode'da",
            tag, len(non_zero), len(emode_map),
        )
        return emode_map

    async def _enrich_positions(
        self,
        ctx:       ChainContext,
        address:   str,
        price_map: Dict[str, float],
    ) -> Tuple[List[EnrichedPosition], List[EnrichedPosition]]:
        """
        getUserReserveData multicall → oracle fiyatıyla kesin USD hesabı.

        ── Godzilla Düzeltmesinin Kalbi ──────────────────────────────────────
        Eski yöntem (YANLIŞ):
          total_normalized = sum(raw_i / 10**dec_i)   ← token sayısı toplamı
          usd_i = (raw_i / 10**dec_i) / total_normalized * total_usd_from_getUserAccountData
          → 31 ARB token >> 10 WETH token → ARB'e $22.700 atandı, YANLIŞ!

        Yeni yöntem (DOĞRU):
          usd_i = (raw_i / 10**dec_i) × oracle_price_i
          → 31 ARB × $1.00  = $31    DUST → filtrele
          → 10 WETH × $3.000 = $30.000  → seçilir ✓
        ──────────────────────────────────────────────────────────────────────
        """
        from eth_abi import decode as _abi_decode
        from web3 import AsyncWeb3 as _W3

        reserves  = ctx.reserve_cache
        sym_order = list(reserves.keys())
        cs_addr   = _W3.to_checksum_address(address)

        # Tek multicall: tüm rezervler için getUserReserveData
        calls = [
            (
                ctx.config.data_provider_address,
                ctx.data_provider.encode_abi(
                    "getUserReserveData",
                    args=[_W3.to_checksum_address(reserves[sym][0]), cs_addr],
                ),
            )
            for sym in sym_order
        ]

        try:
            rot = get_rotator()
            raw_results = await call_with_retry(
                lambda: ctx.multicall_contract.functions.tryAggregate(
                    False, calls,
                ).call(),
                rot, ctx.w3,
                context_label=f"{ctx.config.tag}-ENRICH",
                estimated_cu=cu_try_aggregate(len(calls)),
            )
        except Exception as exc:
            logger.warning(
                "[%s-OPTI] Enrich multicall hatası | %s: %s",
                ctx.config.tag, address[:10], exc,
            )
            return [], []

        debts: List[EnrichedPosition] = []
        colls: List[EnrichedPosition] = []

        for i, (ok, ret) in enumerate(raw_results):
            if not ok or not ret:
                continue
            try:
                sym               = sym_order[i]
                tok_addr, decimals = reserves[sym]

                d = _abi_decode(
                    ["uint256","uint256","uint256","uint256","uint256",
                     "uint256","uint256","uint40","bool"],
                    ret,
                )
                a_balance    = d[0]           # currentATokenBalance
                stable_debt  = d[1]           # currentStableDebt
                var_debt     = d[2]           # currentVariableDebt
                coll_enabled = d[8]           # usageAsCollateralEnabled

                total_debt_raw = stable_debt + var_debt
                total_coll_raw = a_balance if coll_enabled else 0

                # ── Oracle fiyatı (USD, 8-dec normalleştirilmiş) ──────────────
                price_usd: float = price_map.get(tok_addr.lower(), 0.0)
                if price_usd <= 0:
                    continue   # Fiyatsız token → güvenle atla

                # Display sembol: TKN_ → gerçek isim
                display_sym = sym
                if sym.startswith("TKN_"):
                    display_sym = ctx.addr_to_symbol.get(tok_addr.lower(), sym)

                asset_class = self._asset_class(display_sym)
                divisor     = 10 ** decimals

                # ── Borç ─────────────────────────────────────────────────────
                if total_debt_raw > 0:
                    debt_usd_val = (total_debt_raw / divisor) * price_usd
                    if debt_usd_val >= OPTI_DUST_USD:
                        debts.append(EnrichedPosition(
                            symbol          = display_sym,
                            underlying_addr = tok_addr,
                            decimals        = decimals,
                            amount_raw      = total_debt_raw,
                            amount_usd      = debt_usd_val,
                            asset_class     = asset_class,
                        ))
                    else:
                        logger.debug(
                            "[OPTI] DUST borç: %s $%.2f | %s",
                            display_sym, debt_usd_val, address[:10],
                        )

                # ── Teminat ───────────────────────────────────────────────────
                if total_coll_raw > 0:
                    coll_usd_val = (total_coll_raw / divisor) * price_usd
                    if coll_usd_val >= OPTI_DUST_USD:
                        colls.append(EnrichedPosition(
                            symbol          = display_sym,
                            underlying_addr = tok_addr,
                            decimals        = decimals,
                            amount_raw      = total_coll_raw,
                            amount_usd      = coll_usd_val,
                            asset_class     = asset_class,
                        ))
                    else:
                        logger.debug(
                            "[OPTI] DUST teminat: %s $%.2f | %s",
                            display_sym, coll_usd_val, address[:10],
                        )

            except Exception:
                continue

        # Büyükten küçüğe sırala (en büyük önce → ilk iterasyonda optimal çift)
        debts.sort(key=lambda p: p.amount_usd, reverse=True)
        colls.sort(key=lambda p: p.amount_usd, reverse=True)
        return debts, colls

    def _score_all_pairs(
        self,
        debts:             List[EnrichedPosition],
        colls:             List[EnrichedPosition],
        hf:                float,
        emode_cat:         int,
        emode_bonus:       float,
        reserve_emode_map: Dict[str, int],
        gas_fee_usd:       float,
    ) -> List["ScoredPair"]:
        """
        [v14] Tüm (debt_i, coll_j) kombinasyonlarını net_score'a göre sıralar.

        E-Mode Zekâsı (v14 — Dinamik On-Chain):
          1. Kullanıcının emode_cat değeri > 0 ise:
          2. Her (debt, coll) çifti için reserve_emode_map'ten
             her iki tokenın da aynı E-Mode kategorisinde olup
             olmadığını kontrol eder.
          3. Eşleşme varsa → E-MODE, bonus = on-chain okunan gerçek bonus.
          4. Eşleşme yoksa → NORMAL/HEDGE bonuslarına düşer.

          ARTIK HARDCODED EMODE_CLASS KULLANILMIYOR.
          Tüm veriler on-chain'den gelir ve cache'lenir.
        """
        from config import (
            CLOSE_FACTOR, HF_ZOMBIE_MIN,
            FLASH_LOAN_FEE,
            LIQUIDATION_BONUS_MAP, DEFAULT_BONUS,
        )

        eff_cf = 1.0 if hf < HF_ZOMBIE_MIN else CLOSE_FACTOR

        scored: List[ScoredPair] = []

        for debt in debts:
            for coll in colls:
                if coll.underlying_addr.lower() == debt.underlying_addr.lower():
                    continue

                # ── E-Mode / Korelasyon filtresi ─────────────────────────────
                # E-Mode çiftleri (wstETH/WETH, weETH/WETH, ezETH/WETH vb.)
                # tasfiye edilemez: korelasyonlu varlıklar birlikte hareket
                # eder, HF pratik olarak 1.0 altına düşmez. Bonus %1 olsa
                # bile flash fee + gas ile kâr marjı yok. ATLA.
                is_emode_pair = False
                if emode_cat > 0 and emode_bonus > 0:
                    debt_ecat = reserve_emode_map.get(
                        debt.underlying_addr.lower(), 0,
                    )
                    coll_ecat = reserve_emode_map.get(
                        coll.underlying_addr.lower(), 0,
                    )
                    is_emode_pair = (
                        debt_ecat == emode_cat and coll_ecat == emode_cat
                    )

                # Fallback güvenlik ağı: on-chain tespit başarısız olsa bile
                # aynı sınıftaki korelasyonlu çiftleri filtrele.
                # ETH/ETH (wstETH/WETH) ve BTC/BTC (tBTC/WBTC) fiyat
                # korelasyonu nedeniyle HF pratik olarak 1.0 altına düşmez.
                # STABLE/STABLE BURADA YOK: stabil çiftler (USDC/USDT)
                # portföydeki volatil varlıklar düştüğünde tasfiye edilebilir.
                # Bonus %1 ile kârlı olup olmadığını _score mantığı belirler.
                is_correlated_fallback = (
                    debt.asset_class == coll.asset_class
                    and debt.asset_class in ("ETH", "BTC")
                )

                if is_emode_pair or is_correlated_fallback:
                    continue

                # ── Bonus & Pozisyon tipi ─────────────────────────────────────
                if debt.asset_class == "STABLE" and coll.asset_class == "STABLE":
                    # STABLE/STABLE: E-Mode aktifse on-chain bonus (%1),
                    # değilse STABLE_PAIR_BONUS fallback.
                    # %5 DEFAULT_BONUS ile hesap yapmak hayali kâr üretir.
                    bonus    = emode_bonus if emode_bonus > 0 else STABLE_PAIR_BONUS
                    pos_type = "STABLE-PAIR"
                elif (
                    (debt.asset_class == "STABLE"
                     and coll.asset_class in ("ETH", "BTC", "ALT"))
                    or (coll.asset_class == "STABLE"
                        and debt.asset_class in ("ETH", "BTC", "ALT"))
                ):
                    bonus    = LIQUIDATION_BONUS_MAP.get(
                        coll.symbol, DEFAULT_BONUS,
                    )
                    pos_type = "HEDGE"
                else:
                    bonus    = LIQUIDATION_BONUS_MAP.get(
                        coll.symbol, DEFAULT_BONUS,
                    )
                    pos_type = "NORMAL"

                # ── Close factor ──────────────────────────────────────────────
                debt_cover_usd    = debt.amount_usd * eff_cf
                debt_to_cover_wei = int(debt.amount_raw * eff_cf)

                # ── Collateral Bottleneck (v10 devam) ─────────────────────────
                required_coll = debt_cover_usd * (1 + bonus)
                if required_coll > coll.amount_usd > 0:
                    debt_cover_usd = coll.amount_usd / (1 + bonus)
                    ratio          = (debt_cover_usd / debt.amount_usd
                                      if debt.amount_usd > 0 else 0.0)
                    debt_to_cover_wei = int(debt.amount_raw * ratio)

                coll_to_receive = debt_cover_usd * (1 + bonus)

                # ── Kâr hesabı ────────────────────────────────────────────────
                gross     = debt_cover_usd * bonus
                flash_fee = debt_cover_usd * FLASH_LOAN_FEE

                slip_rate     = CLASS_SLIPPAGE.get(
                    (debt.asset_class, coll.asset_class),
                    DEFAULT_PAIR_SLIPPAGE,
                )
                adjusted_slip = debt_cover_usd * slip_rate
                net           = gross - flash_fee - adjusted_slip - gas_fee_usd

                scored.append(ScoredPair(
                    debt                   = debt,
                    coll                   = coll,
                    bonus                  = bonus,
                    position_type          = pos_type,
                    effective_close_factor = eff_cf,
                    debt_cover_usd         = debt_cover_usd,
                    debt_to_cover_wei      = debt_to_cover_wei,
                    coll_to_receive_usd    = coll_to_receive,
                    gross_profit           = gross,
                    adjusted_slippage_usd  = adjusted_slip,
                    flash_fee_usd          = flash_fee,
                    net_score              = net,
                    emode_category         = emode_cat,
                ))

        scored.sort(key=lambda s: s.net_score, reverse=True)
        return scored

    # On-chain semboller Unicode veya bridge ekleri içerebilir.
    # Önce orijinal sembolü dene, bulamazsan normalize versiyonunu dene.
    _SYMBOL_NORMALIZE = {
        "USD₮0": "USDT", "USDT0": "USDT", "USDt": "USDT",
        "USDT.e": "USDT", "USDTe": "USDT",
        "USDC0": "USDC", "USDCn": "USDC",
    }

    @staticmethod
    def _asset_class(symbol: str) -> str:
        """cfg.ASSET_CLASS tablosuna bak; bulamazsan normalize et, yoksa 'ALT'."""
        if symbol.startswith("TKN_"):
            return "ALT"
        cls = cfg.ASSET_CLASS.get(symbol)
        if cls:
            return cls
        normalized = OptiPairEngine._SYMBOL_NORMALIZE.get(symbol)
        if normalized:
            return cfg.ASSET_CLASS.get(normalized, "ALT")
        return "ALT"

    def _check_volatile_impact(
        self,
        debts:   List[EnrichedPosition],
        colls:   List[EnrichedPosition],
        address: str,
        tag:     str,
    ) -> bool:
        """
        Volatilite Etki Filtresi — Net Açık Pozisyon (Net Unhedged Exposure).

        Eski mantık (v15): tüm volatil varlıkların mutlak USD toplamını
        alıyordu. $5.855 WBTC teminat + $4.962 WBTC borç = $10.817 volatil
        olarak görülüyordu — YANLIŞ. Gerçek açık pozisyon sadece $893.

        Yeni mantık (v16 — Delta-Neutral tespiti):
          1. Pozisyonları asset_class'a göre grupla (BTC, ETH, ALT, STABLE).
          2. Her volatil grup için:
               net_exposure = abs(toplam_teminat_usd − toplam_borç_usd)
             Eşleşen kısım (hedge) birbirini nötrler, fiyat hareketi
             HF'yi değiştirmez. Sadece AÇIKTA kalan fark riski taşır.
          3. effective_volatile_usd = sum(net_exposure for each group)
          4. Oran eşik altındaysa → Stable-Heavy VEYA Delta-Neutral.
             Fiyat hareketiyle tasfiye imkansız → filtrele.

        Örnek:
          Teminat: $5.855 WBTC + $37.000 USDC
          Borç:    $4.962 WBTC + $30.000 USDT
          BTC grubu: net = abs(5855 − 4962) = $893
          STABLE grubu: atlanır (volatil değil)
          effective_volatile = $893
          total = $77.817
          oran = 893 / 77817 = %1.1 → ATILDI ✓
        """
        total_usd = (
            sum(p.amount_usd for p in debts)
            + sum(p.amount_usd for p in colls)
        )
        if total_usd <= 0:
            return False

        # asset_class → (coll_usd_sum, debt_usd_sum)
        class_buckets: Dict[str, List[float]] = {}
        for p in colls:
            bucket = class_buckets.setdefault(p.asset_class, [0.0, 0.0])
            bucket[0] += p.amount_usd
        for p in debts:
            bucket = class_buckets.setdefault(p.asset_class, [0.0, 0.0])
            bucket[1] += p.amount_usd

        effective_volatile_usd = 0.0
        for asset_class, (coll_sum, debt_sum) in class_buckets.items():
            if asset_class == "STABLE":
                continue
            effective_volatile_usd += abs(coll_sum - debt_sum)

        volatile_ratio = effective_volatile_usd / total_usd

        if volatile_ratio < STABLE_HEAVY_VOLATILE_RATIO:
            self._stable_heavy_addrs.add(address)
            logger.info(
                "[%s-OPTI] HEDGED/STABLE-HEAVY ATILDI | %s | "
                "Toplam: $%s | Net Açık Volatil: $%s (%.1f%%) | "
                "Delta-Neutral veya stabil ağırlık — tasfiye imkansız",
                tag, address[:10],
                f"{total_usd:,.0f}", f"{effective_volatile_usd:,.0f}",
                volatile_ratio * 100,
            )
            return True

        return False


# ── Global singleton: tüm zincirler aynı engine'i paylaşır ───────────────────
opti_engine = OptiPairEngine()


def targets_json_pair_meta(
    address: str,
    info: Optional[TargetInfo] = None,
) -> Dict[str, Any]:
    """
    targets.json'a yazılacak parite / bonus alanları.
    info yoksa son OptiPairEngine skoru (get_payload) kullanılır — hot güncellemelerinde korunur.
    """
    if info:
        return {
            "collateral_asset": info.collateral_asset,
            "debt_asset": info.debt_asset,
            "bonus_pct": round(info.bonus * 100, 4),
            "effective_close_factor": round(info.effective_close_factor, 6),
        }
    pl = opti_engine.get_payload(address)
    if not pl:
        return {}
    out: Dict[str, Any] = {}
    csym = pl.get("collateral_symbol")
    dsym = pl.get("debt_symbol")
    if csym:
        out["collateral_asset"] = csym
    if dsym:
        out["debt_asset"] = dsym
    if pl.get("bonus_pct") is not None:
        out["bonus_pct"] = pl["bonus_pct"]
    if pl.get("effective_close_factor") is not None:
        out["effective_close_factor"] = pl["effective_close_factor"]
    return out


# ═════════════════════════════════════════════════════════════════════════════
# TARGETS STORE — Streamlit JSON Export Köprüsü (v12, değiştirilmedi)
# ═════════════════════════════════════════════════════════════════════════════

class TargetsStore:
    """
    Bellekteki hot_list verilerini targets.json'a yansıtan köprü.
    Non-blocking (asyncio.Queue + to_thread). Atomik yazım (.tmp → replace).
    """
    _Op = Tuple[str, str, Optional[Dict[str, Any]]]

    def __init__(self, filepath: str = TARGETS_FILE) -> None:
        self._filepath = filepath
        self._data: Dict[str, Dict[str, Any]] = {}
        self._queue: asyncio.Queue            = asyncio.Queue()

    def upsert(
        self,
        chain:          str,
        address:        str,
        hf:             float,
        debt_usd:       float,
        collateral_usd: float,
        **extra,
    ) -> None:
        record: Dict[str, Any] = {
            "chain":          chain,
            "address":        address,
            "hf":             round(hf, 6),
            "debt_usd":       round(debt_usd, 2),
            "collateral_usd": round(collateral_usd, 2),
            "updated_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        for k, v in extra.items():
            if v is not None:
                record[k] = v
        self._enqueue(("upsert", address, record))

    def remove(self, address: str) -> None:
        self._enqueue(("remove", address, None))

    async def writer_loop(self) -> None:
        logger.info("[TargetsStore] Writer task başladı → %s", self._filepath)
        while True:
            try:
                first = await self._queue.get()
                ops: List[TargetsStore._Op] = [first]
                while not self._queue.empty():
                    try:
                        ops.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                for op, address, payload in ops:
                    if op == "upsert" and payload is not None:
                        prev = self._data.get(address, {})
                        self._data[address] = {**prev, **payload}
                    elif op == "remove":
                        self._data.pop(address, None)

                snapshot = list(self._data.values())
                await asyncio.to_thread(self._write_file, snapshot)

                logger.debug(
                    "[TargetsStore] %d işlem | Aktif: %d",
                    len(ops), len(self._data),
                )
            except asyncio.CancelledError:
                logger.info("[TargetsStore] Writer iptal edildi.")
                return
            except Exception as exc:
                logger.error("[TargetsStore] Writer hata: %s", exc)
                await asyncio.sleep(0.5)

    def _enqueue(self, op: _Op) -> None:
        if self._queue.qsize() >= TARGETS_MAX_QUEUE:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            self._queue.put_nowait(op)
        except asyncio.QueueFull:
            pass

    def _write_file(self, records: List[Dict[str, Any]]) -> None:
        tmp = self._filepath + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(records, fh, ensure_ascii=False, indent=2)
            os.replace(tmp, self._filepath)
        except Exception as exc:
            logger.error("[TargetsStore] Yazım hatası: %s", exc)
            try:
                os.remove(tmp)
            except OSError:
                pass


targets_store = TargetsStore(TARGETS_FILE)


# ─────────────────────────────────────────────────────────────────────────────
# THE GRAPH SORGULARI
# ─────────────────────────────────────────────────────────────────────────────

QUERY_POSITIONS = """
query($first: Int!, $skip: Int!) {
  positions(first: $first, skip: $skip,
    where: { side: BORROWER, balance_gt: "0" },
    orderBy: id, orderDirection: asc) {
    account { id }
  }
}
"""

QUERY_USER_RESERVES = """
query($first: Int!, $skip: Int!) {
  userReserves(first: $first, skip: $skip,
    where: { currentVariableDebt_gt: "0" },
    orderBy: id, orderDirection: asc) {
    user { id }
  }
}
"""


# ─────────────────────────────────────────────────────────────────────────────
# THE GRAPH — Borçlu Listesi + API Kalkanı
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_borrowers(
    session: aiohttp.ClientSession,
    ctx:     ChainContext,
) -> List[str]:
    from web3 import AsyncWeb3

    tag    = ctx.config.tag
    schema = ctx.config.subgraph_schema

    if schema == "positions":
        query, data_key = QUERY_POSITIONS, "positions"
        addr_fn = lambda p: p["account"]["id"]
    else:
        query, data_key = QUERY_USER_RESERVES, "userReserves"
        addr_fn = lambda p: p["user"]["id"]

    addresses: Set[str] = set()
    skip        = 0
    null_streak = 0

    _sub_url = ctx.config.subgraph_url or "(boş)"
    logger.info("[%s-COLD] Borçlu listesi çekiliyor (schema: %s) → %s...%s",
                tag, schema, _sub_url[:60], _sub_url[-20:] if len(_sub_url) > 60 else "")

    while True:
        if skip >= MAX_SKIP_LIMIT:
            logger.warning("[%s-COLD] API KALKANI: skip=%d sınırı.", tag, skip)
            break

        try:
            async with session.post(
                ctx.config.subgraph_url,
                json={
                    "query":     query,
                    "variables": {"first": cfg.GRAPH_BATCH_SIZE, "skip": skip},
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                raw_bytes = await resp.read()
                try:
                    data = json.loads(raw_bytes.decode("utf-8"))
                except json.JSONDecodeError:
                    logger.error(
                        "[%s-COLD] Graph yanıtı JSON değil (HTTP %s). İlk 400 bayt: %r",
                        tag,
                        resp.status,
                        raw_bytes[:400],
                    )
                    null_streak += 1
                    if null_streak >= NULL_STREAK_LIMIT:
                        break
                    await asyncio.sleep(2)
                    continue
                if resp.status >= 400:
                    logger.error(
                        "[%s-COLD] Graph HTTP %s — gövde: %s",
                        tag,
                        resp.status,
                        raw_bytes[:800].decode("utf-8", errors="replace"),
                    )
                    null_streak += 1
                    if null_streak >= NULL_STREAK_LIMIT:
                        break
                    await asyncio.sleep(2)
                    continue
        except Exception as exc:
            logger.error("[%s-COLD] Graph isteği hatası: %s", tag, exc)
            null_streak += 1
            if null_streak >= NULL_STREAK_LIMIT:
                break
            await asyncio.sleep(2)
            continue

        if data.get("errors"):
            logger.warning(
                "[%s-COLD] GraphQL errors (skip=%d): %s",
                tag,
                skip,
                json.dumps(data["errors"], ensure_ascii=False)[:2000],
            )

        if "errors" in data and data.get("data") is None:
            null_streak += 1
            if null_streak >= NULL_STREAK_LIMIT:
                logger.error(
                    "[%s-COLD] Graph yanıtında data=null — GRAPH_API_KEY veya %s_SUBGRAPH_ID / "
                    "%s_SUBGRAPH_URL kontrol edin (The Graph Studio).",
                    tag,
                    tag,
                    tag,
                )
                break
            await asyncio.sleep(1)
            continue

        positions = (data.get("data") or {}).get(data_key) or []

        if not positions:
            null_streak += 1
            if null_streak >= NULL_STREAK_LIMIT:
                logger.info(
                    "[%s-COLD] %d boş sayfa — tamamlandı. (%d adres). "
                    "Subgraph şeması veya id eskiyse %s_SUBGRAPH_URL / %s_SUBGRAPH_ID güncelleyin.",
                    tag,
                    null_streak,
                    len(addresses),
                    tag,
                    tag,
                )
                break
            await asyncio.sleep(0.5)
            continue

        null_streak = 0
        for p in positions:
            try:
                addresses.add(AsyncWeb3.to_checksum_address(addr_fn(p)))
            except Exception:
                continue

        if len(positions) < cfg.GRAPH_BATCH_SIZE:
            break
        skip += cfg.GRAPH_BATCH_SIZE

    logger.debug("[%s-COLD] %d tekil borçlu adres bulundu.", tag, len(addresses))
    return list(addresses)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — COLD SCAN
# ─────────────────────────────────────────────────────────────────────────────

async def cold_scan_loop(
    session: aiohttp.ClientSession,
    ctx:     ChainContext,
) -> None:
    """
    Geniş radar taraması.
    [v13] discover_target() → opti_engine.analyze() ile değiştirildi.
    """
    tag       = ctx.config.tag
    chain_cfg = ctx.config

    if not chain_cfg.subgraph_url:
        logger.warning(
            "[%s-COLD] Subgraph URL tanımsız — cold_scan devre dışı. "
            "Bu zincir yalnızca WSS (Hot Scan) ile çalışacak.", tag,
        )
        await load_reserve_cache(ctx)
        return

    await load_reserve_cache(ctx)

    while True:
        try:
            logger.debug("━" * 65)
            logger.debug("[%s-COLD] Geniş Radar Taraması başlıyor...", tag)

            borrowers = await fetch_borrowers(session, ctx)
            if not borrowers:
                logger.warning(
                    "[%s-COLD] Borçlu listesi boş, %ds sonra...",
                    tag, SCAN_TAIL_SLEEP_SEC,
                )
                await asyncio.sleep(SCAN_TAIL_SLEEP_SEC)
                continue

            tg.send(fmt_scan_start(tag, len(borrowers)))

            total   = len(borrowers)
            chunks  = [
                borrowers[i:i + cfg.COLD_CHUNK_SIZE]
                for i in range(0, total, cfg.COLD_CHUNK_SIZE)
            ]
            scanned = 0
            stats   = {"guvenli": 0, "kucuk": 0, "karsiz": 0, "hot_add": 0, "firsat": 0, "stable_heavy": 0}
            t0      = time.time()
            scan_aborted_429 = False

            for chunk in chunks:
                if scan_aborted_429:
                    break
                # Multicall: sabit paket boyutu + paketler arası throttle (RPC/CU)
                sub_chunks = [
                    chunk[i:i + MULTICALL_BATCH_SIZE]
                    for i in range(0, len(chunk), MULTICALL_BATCH_SIZE)
                ]
                results: List[Tuple[str, float, float, float]] = []
                for sub_chunk in sub_chunks:
                    try:
                        sub_results = await multicall_account_data(ctx, sub_chunk)
                    except RPCRateLimited429:
                        logger.warning(
                            "[%s-COLD] RPC 429 — bu geniş tarama turu iptal, %ds geri çekiliyor.",
                            tag, RATE_LIMIT_COOLDOWN_SEC,
                        )
                        await asyncio.sleep(RATE_LIMIT_COOLDOWN_SEC)
                        scan_aborted_429 = True
                        break
                    if sub_results:
                        results.extend(sub_results)
                    await asyncio.sleep(MULTICALL_THROTTLE_SEC)
                if scan_aborted_429:
                    break

                for address, hf, debt_usd, coll_usd in results:

                    if hf >= cfg.HF_HOT_UPPER:
                        stats["guvenli"] += 1
                        continue

                    if debt_usd < cfg.MIN_DEBT_USD:
                        stats["kucuk"] += 1
                        continue

                    # ── HF 1.00–1.05: Hot_list adayı ─────────────────────────
                    if cfg.HF_LIQUIDATABLE <= hf < cfg.HF_HOT_UPPER:
                        if address not in ctx.hot_list:
                            # [v13] Oracle-fiyatlı pair seçimi
                            info = await opti_engine.analyze(
                                ctx, address, hf, debt_usd, coll_usd,
                            )
                            if info is None and opti_engine.is_stable_heavy(address):
                                stats["stable_heavy"] += 1
                                continue

                            # ── STRICT DATA INTEGRITY ─────────────────────
                            # OptiPair başarısız → parite yok → ASLA JSON'a
                            # veya hot_list'e ekleme. build_profit_simple
                            # fallback'i kaldırıldı: eksik adresli hedef
                            # Sniper'ı çökertir.
                            if info is None or not info.debt_asset or not info.collateral_asset:
                                logger.debug(
                                    "[%s-COLD] PARİTE YOK → ATLA | %s | "
                                    "OptiPair sonuç döndüremedi",
                                    tag, address[:10],
                                )
                                stats["karsiz"] += 1
                                continue

                            profit = build_profit_from_info(info, chain_cfg.gas_fee_usd)
                            if not profit.is_profitable:
                                stats["karsiz"] += 1
                                continue

                            try:
                                _nonce = await ctx.w3.eth.get_transaction_count(address)
                                record_rpc_cu(
                                    rpc_method="eth_getTransactionCount",
                                    context_label=f"{tag}-COLD-nonce",
                                )
                            except Exception:
                                _nonce = 0

                            ctx.hot_list.add(address)
                            ctx.wallet_snapshot[address] = WalletSnapshot(
                                hf, debt_usd, coll_usd, nonce=_nonce,
                            )
                            stats["hot_add"] += 1
                            log_hot_list_add(tag, address, hf, debt_usd, info, profit)
                            targets_store.upsert(
                                tag, address, hf, debt_usd, coll_usd,
                                **targets_json_pair_meta(address, info),
                            )

                            tg.send(fmt_hot_list_add(
                                tag, address, hf,
                                total_debt_usd  = debt_usd,
                                total_coll_usd  = coll_usd,
                                debt_asset      = info.debt_asset,
                                debt_token_usd  = info.debt_token_total_usd,
                                coll_asset      = info.collateral_asset,
                                coll_token_usd  = info.collateral_token_total_usd,
                                pos_type        = info.position_type,
                                net_profit      = profit.net_profit,
                                debt_cover_usd  = info.debt_amount_usd,
                            ))
                        continue

                    # ── HF < 1.00 (veya zombie): Direkt fırsat ────────────────
                    # [v13] Oracle-fiyatlı pair seçimi
                    info = await opti_engine.analyze(
                        ctx, address, hf, debt_usd, coll_usd,
                    )
                    if info is None and opti_engine.is_stable_heavy(address):
                        stats["stable_heavy"] += 1
                        continue

                    # ── STRICT DATA INTEGRITY ─────────────────────────
                    # Parite bilgisi olmayan hedef asla fırsat sayılmaz.
                    if info is None or not info.debt_asset or not info.collateral_asset:
                        logger.debug(
                            "[%s-COLD] PARİTE YOK → FIRSAT ATLANDI | %s",
                            tag, address[:10],
                        )
                        stats["karsiz"] += 1
                        continue

                    profit = build_profit_from_info(info, chain_cfg.gas_fee_usd)

                    if not profit.is_profitable:
                        stats["karsiz"] += 1
                        continue

                    stats["firsat"] += 1
                    log_opportunity(f"{tag}-COLD", info, profit)
                    sniper_payload = opti_engine.get_payload(address)
                    if sniper_payload:
                        logger.info(
                            "[%s-COLD] 🎯 SNIPER PAYLOAD | %s",
                            tag, json.dumps(sniper_payload, ensure_ascii=False),
                        )
                    tg.send(fmt_opportunity(
                        f"{tag}-COLD", address, hf,
                        info.debt_amount_usd,  info.debt_asset,
                        info.collateral_usd,   info.collateral_asset,
                        info.bonus * 100,
                        profit.flash_fee, profit.dex_slippage,
                        profit.gas_fee,   profit.net_profit,
                    ))

                scanned += len(chunk)
                logger.debug(
                    "[%s-COLD] %d/%d | Güvenli: %d | Küçük: %d | "
                    "Kârsız: %d | StableHeavy: %d | Hot: +%d | Fırsat: %d",
                    tag, scanned, total,
                    stats["guvenli"], stats["kucuk"],
                    stats["karsiz"], stats["stable_heavy"],
                    stats["hot_add"], stats["firsat"],
                )

            if not scan_aborted_429:
                elapsed = time.time() - t0
                hiz     = total / elapsed if elapsed > 0 else 0
                logger.debug(
                    "[%s-COLD] Tamamlandı. Süre: %.1fs | Hız: %.0f/sn | Hot_list: %d",
                    tag, elapsed, hiz, len(ctx.hot_list),
                )
                tg.send(fmt_scan_done(
                    tag, elapsed, hiz,
                    stats["guvenli"], 0, stats["kucuk"],
                    stats["karsiz"], stats["hot_add"], stats["firsat"],
                    len(ctx.hot_list),
                ))

        except Exception as exc:
            logger.error("[%s-COLD] Döngü hatası: %s", tag, exc)

        await asyncio.sleep(SCAN_TAIL_SLEEP_SEC)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2a — HOT SCAN (TEK SEFERLİK)
# ─────────────────────────────────────────────────────────────────────────────

async def hot_scan_once(ctx: ChainContext, block_num: int) -> None:
    """
    Hot_list'teki tüm adresleri tek seferlik tarar.
    wss_block_listener() her yeni blokta Task olarak açar.

    [v13] TETİK adımında (HF < 1.00):
      discover_target() → opti_engine.analyze(..., hot_mode=True)
      hot_mode=True: oracle TTL = 13s (1 blok) — taze Chainlink fiyatı.

    Karar ağacı (v12 korundu):
      1. Snapshot al
      2. BAŞKASI VURDU      → remove + Telegram
      3. KENDİ ODEDİ/Ghost  → Ghost: sessiz güncelle | Aktif: remove + Telegram
      4. TOZ                 → remove
      5. KURTULDU/Ghost      → Ghost: sessiz remove | Aktif: remove + Telegram
      6. İZLE (1.00–1.05)   → upsert
      7. TETİK (< 1.00)     → opti_engine + upsert + fırsat
    """
    if ctx.hot_scan_lock.locked():
        logger.debug(
            "[%s-HOT] Blok #%d atlandı — önceki scan devam ediyor.",
            ctx.config.tag, block_num,
        )
        return

    async with ctx.hot_scan_lock:
        tag       = ctx.config.tag
        chain_cfg = ctx.config

        try:
            if not ctx.hot_list:
                return

            addresses = list(ctx.hot_list)
            chunks    = [
                addresses[i:i + cfg.HOT_CHUNK_SIZE]
                for i in range(0, len(addresses), cfg.HOT_CHUNK_SIZE)
            ]
            to_remove: Set[str] = set()
            scan_aborted_429 = False

            for chunk in chunks:
                if scan_aborted_429:
                    break
                sub_chunks = [
                    chunk[i:i + MULTICALL_BATCH_SIZE]
                    for i in range(0, len(chunk), MULTICALL_BATCH_SIZE)
                ]
                results: List[Tuple[str, float, float, float]] = []
                for sub_chunk in sub_chunks:
                    try:
                        sub_results = await multicall_account_data(ctx, sub_chunk)
                    except RPCRateLimited429:
                        logger.warning(
                            "[%s-HOT] RPC 429 — hot tarama iptal, %ds geri çekiliyor.",
                            tag, RATE_LIMIT_COOLDOWN_SEC,
                        )
                        await asyncio.sleep(RATE_LIMIT_COOLDOWN_SEC)
                        scan_aborted_429 = True
                        break
                    if sub_results:
                        results.extend(sub_results)
                    await asyncio.sleep(MULTICALL_THROTTLE_SEC)
                if scan_aborted_429:
                    break
                start_time = time.perf_counter()

                for address, hf, debt_usd, coll_usd in results:

                    # ── 1. Snapshot al / güncelle ─────────────────────────────
                    snap = ctx.wallet_snapshot.get(address)
                    if snap is None:
                        try:
                            _nonce = await ctx.w3.eth.get_transaction_count(address)
                            record_rpc_cu(
                                rpc_method="eth_getTransactionCount",
                                context_label=f"{tag}-HOT-snapshot-nonce",
                            )
                        except Exception:
                            _nonce = 0
                        ctx.wallet_snapshot[address] = WalletSnapshot(
                            hf, debt_usd, coll_usd, nonce=_nonce,
                        )
                        # ── STRICT DATA INTEGRITY ─────────────────────
                        # Snapshot ilk oluşturma: pair meta zaten varsa
                        # (cold_scan'den geldi) upsert et, yoksa sadece
                        # snapshot'ı kaydet, JSON'a eksik yazmaktan kaçın.
                        pair_meta = targets_json_pair_meta(address, None)
                        if pair_meta.get("debt_asset") and pair_meta.get("collateral_asset"):
                            targets_store.upsert(
                                tag, address, hf, debt_usd, coll_usd,
                                **pair_meta,
                            )
                        continue

                    prev_hf    = float(snap.hf)
                    prev_debt  = float(snap.debt_usd)
                    prev_coll  = float(snap.coll_usd)
                    prev_nonce = int(snap.nonce)

                    # Snapshot güncelle — nonce'u KORU
                    ctx.wallet_snapshot[address] = WalletSnapshot(
                        hf, debt_usd, coll_usd, nonce=prev_nonce,
                    )

                    debt_drop = (prev_debt - debt_usd) / prev_debt if prev_debt > 0 else 0.0
                    coll_drop = (prev_coll - coll_usd) / prev_coll if prev_coll > 0 else 0.0

                    # ── 2. BAŞKASI VURDU ──────────────────────────────────────
                    baskasin_vurdu = (
                        (debt_drop >= 0.50 and coll_drop >= AUTOPSY_COLL_DROP
                         and prev_debt > cfg.MIN_DEBT_USD)
                        or (prev_debt >= cfg.MIN_DEBT_USD
                            and debt_usd < cfg.MIN_DEBT_USD
                            and debt_drop >= 0.50)
                    )

                    if baskasin_vurdu:
                        logger.warning(
                            "[%s-HOT] BAŞKASI VURDU! | %s | "
                            "Borç: $%.2f→$%.2f (-%.0f%%) | HF: %.4f→%.4f",
                            tag, address, prev_debt, debt_usd,
                            debt_drop * 100, prev_hf, hf,
                        )
                        try:
                            tg.send(fmt_autopsy_liquidated(
                                tag, address, prev_hf, hf,
                                prev_debt, debt_usd, prev_coll, coll_usd,
                            ))
                        except Exception:
                            pass
                        targets_store.remove(address)
                        opti_engine.invalidate_address(address)
                        to_remove.add(address)
                        ctx.target_cache.pop(address, None)
                        ctx.wallet_snapshot.pop(address, None)
                        continue

                    # ── 3. KENDİ ODEDİ — Ghost Recovery korumalı ─────────────
                    kendi_odedi = (
                        debt_drop >= 0.10 and
                        coll_drop < 0.15 and
                        prev_debt > cfg.MIN_DEBT_USD and
                        debt_usd >= cfg.MIN_DEBT_USD
                    )

                    if kendi_odedi:
                        try:
                            current_nonce = await ctx.w3.eth.get_transaction_count(address)
                            record_rpc_cu(
                                rpc_method="eth_getTransactionCount",
                                context_label=f"{tag}-HOT-kendi_odedi-nonce",
                            )
                        except Exception:
                            current_nonce = prev_nonce + 1

                        is_passive = (prev_nonce > 0 and current_nonce == prev_nonce)

                        if is_passive:
                            logger.debug(
                                "[%s-HOT] GHOST (kendi_odedi) | %s | "
                                "debt_drop=%.1f%% | Nonce=%d (değişmedi)",
                                tag, address, debt_drop * 100, current_nonce,
                            )
                            ctx.wallet_snapshot[address] = WalletSnapshot(
                                hf, debt_usd, coll_usd, nonce=prev_nonce,
                            )
                            targets_store.upsert(
                                tag, address, hf, debt_usd, coll_usd,
                                **targets_json_pair_meta(address, None),
                            )
                            continue

                        logger.info(
                            "[%s-HOT] KENDİ ODEDİ | %s | "
                            "Borç: $%.2f→$%.2f (-%.0f%%) | Nonce: %d→%d",
                            tag, address, prev_debt, debt_usd,
                            debt_drop * 100, prev_nonce, current_nonce,
                        )
                        try:
                            tg.send(fmt_autopsy_repaid(
                                tag, address, prev_hf, hf,
                                prev_debt, debt_usd, prev_coll, coll_usd,
                            ))
                        except Exception:
                            pass
                        targets_store.remove(address)
                        opti_engine.invalidate_address(address)
                        to_remove.add(address)
                        ctx.target_cache.pop(address, None)
                        ctx.wallet_snapshot.pop(address, None)
                        continue

                    # ── 4. TOZ HESAP ──────────────────────────────────────────
                    if debt_usd < cfg.MIN_DEBT_USD:
                        logger.info(
                            "[%s-HOT] TOZ HESAP | %s | Borç: $%.4f",
                            tag, address, debt_usd,
                        )
                        if prev_debt >= cfg.MIN_DEBT_USD:
                            try:
                                tg.send(fmt_autopsy_liquidated(
                                    tag, address, prev_hf, hf,
                                    prev_debt, debt_usd, prev_coll, coll_usd,
                                ))
                            except Exception:
                                pass
                        targets_store.remove(address)
                        opti_engine.invalidate_address(address)
                        to_remove.add(address)
                        ctx.target_cache.pop(address, None)
                        ctx.wallet_snapshot.pop(address, None)
                        continue

                    # ── 5. KURTULDU — Ghost Recovery korumalı ─────────────────
                    if hf >= cfg.HF_HOT_REMOVE:
                        try:
                            current_nonce = await ctx.w3.eth.get_transaction_count(address)
                            record_rpc_cu(
                                rpc_method="eth_getTransactionCount",
                                context_label=f"{tag}-HOT-kurtuldu-nonce",
                            )
                        except Exception:
                            current_nonce = prev_nonce + 1

                        is_passive = (prev_nonce > 0 and current_nonce == prev_nonce)

                        if is_passive:
                            logger.info(
                                "[%s-HOT] 👻 GHOST RECOVERY | %s | "
                                "HF: %.4f→%.4f | Nonce: %d (değişmedi)",
                                tag, address, prev_hf, hf, current_nonce,
                            )
                        else:
                            logger.info(
                                "[%s-HOT] 🟢 KURTULDU | %s | "
                                "HF: %.4f→%.4f | Nonce: %d→%d",
                                tag, address, prev_hf, hf,
                                prev_nonce, current_nonce,
                            )
                            try:
                                tg.send(fmt_autopsy_repaid(
                                    tag, address, prev_hf, hf,
                                    prev_debt, debt_usd, prev_coll, coll_usd,
                                ))
                            except Exception:
                                pass

                        targets_store.remove(address)
                        opti_engine.invalidate_address(address)
                        to_remove.add(address)
                        ctx.target_cache.pop(address, None)
                        ctx.wallet_snapshot.pop(address, None)
                        continue

                    # ── 6. İZLE: HF 1.00–1.05 ────────────────────────────────
                    if hf >= cfg.HF_LIQUIDATABLE:
                        logger.debug(
                            "[%s-HOT] İzleniyor | %s | HF: %.4f | Borç: $%.2f",
                            tag, address, hf, debt_usd,
                        )
                        targets_store.upsert(
                            tag, address, hf, debt_usd, coll_usd,
                            **targets_json_pair_meta(address, None),
                        )
                        continue

                    # ── 7. TETİK: HF < 1.00 ──────────────────────────────────
                    # [v13] Cache temizle → oracle-fiyatlı taze analiz
                    ctx.target_cache.pop(address, None)
                    opti_engine.invalidate_address(address)

                    info = await opti_engine.analyze(
                        ctx, address, hf, debt_usd, coll_usd,
                        hot_mode=True,   # Oracle TTL = 1 blok (~13s)
                    )

                    # [v15] Stable-Heavy → hot_list'ten at
                    if info is None and opti_engine.is_stable_heavy(address):
                        logger.info(
                            "[%s-HOT] STABLE-HEAVY EVICT | %s | "
                            "Fiyat hareketiyle tasfiye imkansız → kaldırılıyor",
                            tag, address[:10],
                        )
                        targets_store.remove(address)
                        to_remove.add(address)
                        ctx.target_cache.pop(address, None)
                        ctx.wallet_snapshot.pop(address, None)
                        continue

                    # ── STRICT DATA INTEGRITY ─────────────────────────
                    # OptiPair başarısız → parite yok → JSON'a eksik
                    # yazmak Sniper'ı çökertir. Sadece HF/USD güncelle,
                    # pair alanları olmadan upsert YAPMA.
                    if info is None or not info.debt_asset or not info.collateral_asset:
                        logger.debug(
                            "[%s-HOT] PARİTE YOK → JSON GÜNCELLEME ATLANDI | %s",
                            tag, address[:10],
                        )
                        continue

                    profit = build_profit_from_info(info, chain_cfg.gas_fee_usd)

                    targets_store.upsert(
                        tag, address, hf, debt_usd, coll_usd,
                        **targets_json_pair_meta(address, info),
                    )

                    if not profit.is_profitable:
                        logger.debug(
                            "[%s-HOT] Kârsız | %s | Net: $%.4f [%s]",
                            tag, address, profit.net_profit,
                            info.position_type,
                        )
                        continue

                    # 🚨 FIRSAT!
                    log_opportunity(f"{tag}-HOT", info, profit)
                    sniper_payload = opti_engine.get_payload(address)
                    if sniper_payload:
                        logger.info(
                            "[%s-HOT] 🎯 SNIPER PAYLOAD | %s",
                            tag, json.dumps(sniper_payload, ensure_ascii=False),
                        )
                    try:
                        tg.send(fmt_opportunity(
                            f"{tag}-HOT", address, hf,
                            info.debt_amount_usd, info.debt_asset,
                            info.collateral_usd,  info.collateral_asset,
                            info.bonus * 100,
                            profit.flash_fee, profit.dex_slippage,
                            profit.gas_fee,   profit.net_profit,
                        ))
                    except Exception as tg_exc:
                        logger.debug("Telegram hata (fırsat): %s", tg_exc)

                end_time = time.perf_counter()
                logger.info(
                    "[%s-WSS] 🧠 Saf Python: %.3f ms (%d cüzdan)",
                    tag, (end_time - start_time) * 1000, len(chunk),
                )

            if to_remove:
                ctx.hot_list -= to_remove
                logger.info(
                    "[%s-HOT] Blok #%d | %d temizlendi. Kalan: %d",
                    tag, block_num, len(to_remove), len(ctx.hot_list),
                )

        except Exception as exc:
            logger.error("[%s-HOT] hot_scan_once hatası (blok #%d): %s", tag, block_num, exc)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2b — WSS BLOCK LİSTENER
# ─────────────────────────────────────────────────────────────────────────────

async def wss_block_listener(ctx: ChainContext, wss_pool: WssUrlPool = None) -> None:
    """
    WebSocket Pub/Sub — eth_subscribe("newHeads").
    Her yeni blok → hot_scan_once() non-blocking Task.
    Auto-reconnect: exponential backoff 2s → 60s.

    wss_pool verilmişse round-robin + 429 rotasyonu aktif.
    wss_url boşsa → _fallback_hot_poll().
    """
    tag = ctx.config.tag

    if not ctx.config.wss_url and wss_pool is None:
        logger.warning("[%s-WSS] WSS URL yok — polling moduna geçiliyor.", tag)
        await _fallback_hot_poll(ctx)
        return

    # Pool yoksa geriye uyumluluk: tek URL ile pool oluştur
    if wss_pool is None:
        wss_pool = WssUrlPool([ctx.config.wss_url])
        logger.warning(
            "[%s-WSS] WSS-POOL verilmedi, tek URL ile fallback pool oluşturuldu.", tag,
        )

    logger.info("[%s-WSS] Block listener başladı — pool size=%d", tag, wss_pool.size)
    backoff = WSS_INITIAL_BACKOFF

    while True:
        try:
            wss_url = await wss_pool.acquire()
            async with websockets.connect(
                wss_url,
                ping_interval = WSS_PING_INTERVAL,
                ping_timeout  = WSS_PING_TIMEOUT,
                close_timeout = WSS_CLOSE_TIMEOUT,
                max_size      = WSS_MAX_MSG_SIZE,
            ) as ws:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "eth_subscribe",
                    "params": ["newHeads"],
                }))

                raw_resp = await asyncio.wait_for(ws.recv(), timeout=10.0)
                resp     = json.loads(raw_resp)

                if "error" in resp:
                    raise ValueError(f"eth_subscribe hatası: {resp['error']}")

                sub_id = resp.get("result", "")
                if not sub_id:
                    raise ValueError(f"Geçersiz sub_id: {resp}")

                logger.info(
                    "[%s-WSS] Bağlandı. sub_id=%s... endpoint=...%s",
                    tag, sub_id[:16], wss_url[-30:],
                )
                backoff = WSS_INITIAL_BACKOFF

                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    params = msg.get("params", {})
                    if (msg.get("method") != "eth_subscription"
                            or params.get("subscription") != sub_id):
                        continue

                    result    = params.get("result", {})
                    block_hex = result.get("number", "0x0")
                    block_num = int(block_hex, 16) if block_hex.startswith("0x") else 0

                    logger.debug("[%s-WSS] Blok #%d", tag, block_num)

                    if not ctx.hot_list:
                        continue

                    now_mono = time.monotonic()
                    if now_mono - ctx.hot_scan_last_dispatch_mono < WATCHER_HOT_MIN_INTERVAL_SEC:
                        continue

                    if WATCHER_HOT_EVERY_N_BLOCKS > 0:
                        if block_num < ctx.hot_scan_next_allowed_block:
                            continue

                    ctx.hot_scan_last_dispatch_mono = now_mono
                    if WATCHER_HOT_EVERY_N_BLOCKS > 0:
                        ctx.hot_scan_next_allowed_block = block_num + WATCHER_HOT_EVERY_N_BLOCKS

                    asyncio.create_task(
                        hot_scan_once(ctx, block_num),
                        name=f"hot-{tag}-{block_num}",
                    )

        except asyncio.CancelledError:
            logger.info("[%s-WSS] Task iptal edildi.", tag)
            return
        except Exception as exc:
            if wss_transport_limited(exc):
                await wss_pool.on_transport_limit()
                logger.warning(
                    "[%s-WSS] 429/rate-limit → sonraki WSS endpoint'e geçiliyor. %ds backoff.",
                    tag, backoff,
                )
            else:
                logger.warning(
                    "[%s-WSS] Koptu: %s. %ds sonra yeniden...", tag, exc, backoff,
                )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WSS_MAX_BACKOFF)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2c — FALLBACK HOT POLL
# ─────────────────────────────────────────────────────────────────────────────

async def _fallback_hot_poll(ctx: ChainContext) -> None:
    """WSS URL tanımlı olmayan zincirler için HTTP polling."""
    tag       = ctx.config.tag
    chain_cfg = ctx.config
    idle_sleep_sec = max(chain_cfg.cold_interval, MIN_COLD_SLEEP_SEC)
    _poll_raw = os.getenv("WATCHER_HOT_POLL_INTERVAL_SEC", "").strip()
    poll_hot_iv = float(_poll_raw) if _poll_raw else float(chain_cfg.hot_interval)
    logger.info("[%s-HOT] Polling modu (hot_interval=%.1fs).", tag, poll_hot_iv)
    block_num = 0
    while True:
        try:
            block_num += 1
            if ctx.hot_list:
                await hot_scan_once(ctx, block_num)
                await asyncio.sleep(poll_hot_iv)
                continue
        except Exception as exc:
            logger.error("[%s-HOT] Polling hatası: %s", tag, exc)
        # Hot liste boşken agresif polling'i kapat; gerçek dinlenme uygula.
        await asyncio.sleep(idle_sleep_sec)


# ─────────────────────────────────────────────────────────────────────────────
# CLI + MAIN
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> Optional[str]:
    parser = argparse.ArgumentParser(description="Aave V3 Liquidation Watcher")
    parser.add_argument("chain", nargs="?", choices=["ARB", "BASE", "OP", "LINEA"], default=None)
    return parser.parse_args().chain


async def main() -> None:
    target_chain  = parse_args()
    all_chains    = cfg.load_chains()
    chain_configs = (
        [c for c in all_chains if c.tag == target_chain]
        if target_chain else all_chains
    )

    if not chain_configs:
        logger.error("Zincir bulunamadı: %s", target_chain)
        sys.exit(1)

    mode_str = target_chain or "MULTI-CHAIN"
    logger.info("=" * 65)
    logger.info("  AAVE V3 LIQUIDATION WATCHER — %s MODU (v13 OptiPair)", mode_str)
    logger.info("  Zincirler : %s", [c.tag for c in chain_configs])
    logger.info("  WSS       : %s", [c.tag for c in chain_configs if c.wss_url])
    logger.info("  JSON      : %s", TARGETS_FILE)
    logger.info("  Oracle    : %s",
                {c.tag: AAVE_ORACLE_ADDRESSES.get(c.tag, "—")
                 for c in chain_configs})
    logger.info("  Telegram  : %s", "AKTİF" if tg.enabled else "DEVRE DIŞI")
    logger.info("=" * 65)

    contexts = []
    for chain_cfg_item in chain_configs:
        ctx = await build_context(chain_cfg_item)
        if ctx:
            contexts.append(ctx)

    if not contexts:
        logger.error("Hiçbir zincire bağlanamadı.")
        sys.exit(1)

    # ── Her zincir için WSS-POOL oluştur ─────────────────────────────────────
    rot = get_rotator()
    wss_pools: dict = {}
    for ctx in contexts:
        tag = ctx.config.tag
        if ctx.config.wss_url:
            wss_list = build_wss_urls_for_watcher(ctx.config, rot)
            logger.info(
                "[WSS-POOL] %s: %d WSS endpoint (şarjör + %s_WSS)",
                tag, len(wss_list), tag,
            )
            pool = WssUrlPool(wss_list)
            warn_if_single_endpoint(pool.size, tag)
            wss_pools[tag] = pool
        else:
            logger.warning("[WSS-POOL] %s: WSS URL tanımsız — polling moduna düşecek.", tag)

    async with aiohttp.ClientSession() as session:
        tasks = []
        tasks.append(asyncio.create_task(
            targets_store.writer_loop(), name="targets-writer",
        ))
        for ctx in contexts:
            tasks.append(asyncio.create_task(
                cold_scan_loop(session, ctx), name=f"cold-{ctx.config.tag}",
            ))
            tasks.append(asyncio.create_task(
                wss_block_listener(ctx, wss_pool=wss_pools.get(ctx.config.tag)),
                name=f"wss-{ctx.config.tag}",
            ))

        logger.info("  %d paralel task:", len(tasks))
        for t in tasks:
            logger.info("    - %s", t.get_name())
        logger.info("=" * 65)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results):
            if isinstance(result, Exception):
                logger.error("Task '%s' sonlandı: %s", task.get_name(), result)


if __name__ == "__main__":
    asyncio.run(main())
