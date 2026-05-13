"""
aave_utils.py — Blockchain Matematik Motoru v6
-----------------------------------------------
Düzeltmeler:
  1. Tüm AsyncWeb3.to_checksum_address() çağrıları güvenli hale getirildi.
     Token adresleri de dahil — reserve cache, asset discovery her yerde.
  2. TKN_ fallback: bilinmeyen varlık ASSET_CLASS'ta "ALT" kabul edilir,
     bonus DEFAULT_BONUS olur. Asla atlanmaz.
  3. Kârsız sayacı: calculate_profit MIN_PROFIT_USD ile kesin bağlantı.
  4. log_hot_list_add: E-MODE/HEDGE etiketi + gerçek net kâr rakamı.
  5. discover_target: cache güncelleme mantığı sadeleştirildi.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from eth_abi import decode
from web3 import AsyncHTTPProvider, AsyncWeb3

from config import (
    POOL_ABI, MULTICALL3_ABI, DATA_PROVIDER_ABI, ERC20_ABI,
    ChainConfig,
    WAD, USD_DECIMALS, CLOSE_FACTOR,
    FLASH_LOAN_FEE, DEX_SLIPPAGE,
    LIQUIDATION_BONUS_MAP, DEFAULT_BONUS, EMODE_BONUS,
    ASSET_CLASS, WHITELIST_SYMBOLS,
    MIN_PROFIT_USD,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# VERİ YAPILARI
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TargetInfo:
    address:          str
    hf:               float
    total_debt_usd:   float
    debt_asset:       str
    debt_amount_usd:  float
    collateral_asset: str
    collateral_usd:   float
    bonus:            float
    position_type:    str   # "E-MODE" | "HEDGE" | "NORMAL"


@dataclass
class ProfitDetail:
    gross_profit:  float
    flash_fee:     float
    dex_slippage:  float
    gas_fee:       float
    net_profit:    float
    is_profitable: bool


@dataclass
class ChainContext:
    config:             ChainConfig
    w3:                 AsyncWeb3
    pool_contract:      object
    multicall_contract: object
    data_provider:      object
    hot_list:           Set[str]                   = field(default_factory=set)
    target_cache:       Dict[str, TargetInfo]      = field(default_factory=dict)
    reserve_cache:      Dict[str, Tuple[str, int]] = field(default_factory=dict)
    addr_to_symbol:     Dict[str, str]             = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# EIP-55 YARDIMCI
# ─────────────────────────────────────────────────────────────────────────────

def _cs(addr: str) -> str:
    """
    Token adreslerini güvenli checksum formatına dönüştürür.
    Hatalı adres gelirse orijinal string döner (en kötü ihtimal — multicall başarısız olur).
    """
    try:
        return AsyncWeb3.to_checksum_address(addr)
    except Exception:
        return addr


# ─────────────────────────────────────────────────────────────────────────────
# E-MODE / HEDGE TESPİTİ
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_class(symbol: str) -> str:
    """TKN_ prefix'li veya listede olmayan semboller → 'ALT' (güvenli varsayılan)."""
    if symbol.startswith("TKN_"):
        return "ALT"
    return ASSET_CLASS.get(symbol, "ALT")


def _resolve_bonus(symbol: str) -> float:
    """TKN_ veya listede olmayan → DEFAULT_BONUS."""
    if symbol.startswith("TKN_"):
        return DEFAULT_BONUS
    return LIQUIDATION_BONUS_MAP.get(symbol, DEFAULT_BONUS)


def classify_position(debt_symbol: str, coll_symbol: str) -> Tuple[str, float]:
    """
    Borç ve teminat varlık sınıflarını karşılaştırarak pozisyon tipini belirler.

    E-MODE  : Aynı sınıf (STABLE/STABLE, ETH/ETH) → EMODE_BONUS (%1)
    HEDGE   : Stable ↔ Volatile karışımı → standart bonus
    NORMAL  : Diğer çapraz sınıf kombinasyonlar → standart bonus
    """
    debt_class = _resolve_class(debt_symbol)
    coll_class = _resolve_class(coll_symbol)
    coll_bonus = _resolve_bonus(coll_symbol)

    if debt_class == coll_class:
        return "E-MODE", EMODE_BONUS

    if (debt_class == "STABLE" and coll_class in ("ETH", "BTC", "ALT")) or \
       (coll_class == "STABLE" and debt_class in ("ETH", "BTC", "ALT")):
        return "HEDGE", coll_bonus

    return "NORMAL", coll_bonus


# ─────────────────────────────────────────────────────────────────────────────
# BAĞLANTI KURMA
# ─────────────────────────────────────────────────────────────────────────────

async def build_context(cfg: ChainConfig) -> Optional[ChainContext]:
    """
    Bir zincir için AsyncWeb3 bağlantısı ve kontrat nesnelerini oluşturur.
    config.py'deki __post_init__ adres checksum'larını garantilemiş olmalı.
    """
    try:
        w3 = AsyncWeb3(AsyncHTTPProvider(cfg.rpc_url))
        if not await w3.is_connected():
            logger.error("[%s] RPC bağlantısı kurulamadı: %s", cfg.tag, cfg.rpc_url)
            return None

        chain_id = await w3.eth.chain_id
        logger.info("[%s] ✅ RPC bağlı. Chain ID: %d", cfg.tag, chain_id)

        return ChainContext(
            config             = cfg,
            w3                 = w3,
            pool_contract      = w3.eth.contract(
                address=cfg.pool_address, abi=POOL_ABI
            ),
            multicall_contract = w3.eth.contract(
                address=cfg.multicall_address, abi=MULTICALL3_ABI
            ),
            data_provider      = w3.eth.contract(
                address=cfg.data_provider_address, abi=DATA_PROVIDER_ABI
            ),
        )
    except Exception as exc:
        logger.error("[%s] ❌ Context oluşturulamadı: %s", cfg.tag, exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MULTICALL — TOPLU SORGULAMA
# ─────────────────────────────────────────────────────────────────────────────

async def multicall_account_data(
    ctx: ChainContext,
    addresses: List[str],
) -> List[Tuple[str, float, float]]:
    """
    getUserAccountData'yı Multicall3 ile toplu çeker.
    Returns: [(address, health_factor, total_debt_usd), ...]
    """
    calls = [
        (cfg_addr, ctx.pool_contract.encode_abi("getUserAccountData", args=[addr]))
        for addr in addresses
        if (cfg_addr := ctx.config.pool_address)  # walrus — her döngüde aynı adres
    ]

    # Walrus trick yerine sade liste anlayışı:
    calls = [
        (ctx.config.pool_address, ctx.pool_contract.encode_abi("getUserAccountData", args=[addr]))
        for addr in addresses
    ]

    try:
        results = await ctx.multicall_contract.functions.tryAggregate(False, calls).call()
    except Exception as exc:
        logger.warning("[%s] Multicall hatası: %s", ctx.config.tag, exc)
        return []

    parsed = []
    for i, (success, return_data) in enumerate(results):
        if not success or not return_data:
            continue
        try:
            decoded = decode(
                ["uint256","uint256","uint256","uint256","uint256","uint256"],
                return_data,
            )
            hf_wei = decoded[5]
            if hf_wei >= (2 ** 256 - 1):
                continue
            parsed.append((addresses[i], hf_wei / WAD, decoded[1] / USD_DECIMALS))
        except Exception:
            continue

    return parsed


# ─────────────────────────────────────────────────────────────────────────────
# RESERVE CACHE — Tüm Tokenlar + TKN_ Fallback
# ─────────────────────────────────────────────────────────────────────────────

async def load_reserve_cache(ctx: ChainContext) -> Dict[str, Tuple[str, int]]:
    """
    getAllReservesTokens() ile zincirdeki tüm reserve tokenlarını çeker.

    Beyaz listede olan  → kendi sembolü (WETH, USDC vb.)
    Beyaz listede olmayan → TKN_XXXXXX (adresin son 6 hex)
    Her iki durumda da cache'e alınır, "?" ile geçilmez.

    Token adresleri de _cs() ile checksum'a çevrilir.
    """
    if ctx.reserve_cache:
        return ctx.reserve_cache

    tag = ctx.config.tag
    try:
        tokens = await ctx.data_provider.functions.getAllReservesTokens().call()
    except Exception as exc:
        logger.error(
            "[%s] getAllReservesTokens başarısız "
            "(data_provider adresini kontrol et): %s",
            tag, exc,
        )
        return ctx.reserve_cache

    known_count = 0
    for symbol, token_addr in tokens:
        safe_addr = _cs(token_addr)  # ← EIP-55 checksum garantisi

        # Ters arama tablosu
        ctx.addr_to_symbol[safe_addr.lower()] = symbol

        # Sembol normalizasyonu
        if symbol in WHITELIST_SYMBOLS:
            effective_symbol = symbol
            known_count += 1
        else:
            # Bilinmeyen → adresin son 6 karakteri ile benzersiz isim
            effective_symbol = f"TKN_{safe_addr[-6:].upper()}"

        # Decimals — başarısız olursa 18 varsay
        try:
            erc20    = ctx.w3.eth.contract(address=safe_addr, abi=ERC20_ABI)
            decimals = await erc20.functions.decimals().call()
        except Exception:
            decimals = 18

        ctx.reserve_cache[effective_symbol] = (safe_addr, decimals)

    unknown_count = len(ctx.reserve_cache) - known_count
    logger.info(
        "[%s] Reserve cache yüklendi: %d token "
        "(%d bilinen, %d TKN_ fallback)",
        tag, len(ctx.reserve_cache), known_count, unknown_count,
    )
    return ctx.reserve_cache


# ─────────────────────────────────────────────────────────────────────────────
# ASSET DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

async def discover_target(
    ctx: ChainContext,
    address: str,
    hf: float,
    total_debt_usd: float,
) -> Optional[TargetInfo]:
    """
    Hedef cüzdanın en büyük borç ve teminat varlığını Multicall ile tespit eder.
    Sonuç target_cache'e kaydedilir.
    """
    # Cache hit → değişen alanları güncelle
    cached = ctx.target_cache.get(address)
    if cached:
        cached.hf              = hf
        cached.total_debt_usd  = total_debt_usd
        cached.debt_amount_usd = total_debt_usd * CLOSE_FACTOR
        cached.collateral_usd  = cached.debt_amount_usd * (1 + cached.bonus)
        return cached

    reserves = await load_reserve_cache(ctx)
    if not reserves:
        return None

    checksum_addr   = _cs(address)
    symbols_ordered = list(reserves.keys())

    calls = [
        (
            ctx.config.data_provider_address,
            ctx.data_provider.encode_abi(
                "getUserReserveData",
                args=[_cs(reserves[sym][0]), checksum_addr],
            ),
        )
        for sym in symbols_ordered
    ]

    try:
        results = await ctx.multicall_contract.functions.tryAggregate(False, calls).call()
    except Exception as exc:
        logger.warning("[%s] Asset discovery hatası | %s: %s", ctx.config.tag, address, exc)
        return None

    max_debt_symbol = ""
    max_debt_norm   = 0.0
    max_coll_symbol = ""
    max_coll_norm   = 0.0

    for i, (success, return_data) in enumerate(results):
        if not success or not return_data:
            continue
        try:
            symbol   = symbols_ordered[i]
            decimals = reserves[symbol][1]
            decoded  = decode(
                ["uint256","uint256","uint256","uint256","uint256",
                 "uint256","uint256","uint40","bool"],
                return_data,
            )
            stable_debt        = decoded[1]
            variable_debt      = decoded[2]
            a_balance          = decoded[0]
            collateral_enabled = decoded[8]

            debt_norm = (stable_debt + variable_debt) / (10 ** decimals)
            coll_norm = a_balance / (10 ** decimals) if collateral_enabled else 0.0

            if debt_norm > max_debt_norm:
                max_debt_norm, max_debt_symbol = debt_norm, symbol
            if coll_norm > max_coll_norm:
                max_coll_norm, max_coll_symbol = coll_norm, symbol

        except Exception:
            continue

    if not max_debt_symbol or not max_coll_symbol:
        return None

    position_type, effective_bonus = classify_position(max_debt_symbol, max_coll_symbol)
    debt_amount_usd = total_debt_usd * CLOSE_FACTOR

    info = TargetInfo(
        address          = address,
        hf               = hf,
        total_debt_usd   = total_debt_usd,
        debt_asset       = max_debt_symbol,
        debt_amount_usd  = debt_amount_usd,
        collateral_asset = max_coll_symbol,
        collateral_usd   = debt_amount_usd * (1 + effective_bonus),
        bonus            = effective_bonus,
        position_type    = position_type,
    )
    ctx.target_cache[address] = info
    return info


# ─────────────────────────────────────────────────────────────────────────────
# KÂR HESABI
# ─────────────────────────────────────────────────────────────────────────────

def calculate_profit(
    debt_amount_usd: float,
    bonus:           float,
    gas_fee_usd:     float,
) -> ProfitDetail:
    """
    Net Kâr = (Borç × Bonus) − Flash Fee − DEX Slippage − Gas
    is_profitable = net_profit > MIN_PROFIT_USD  (kesin bağlantı)
    """
    gross = debt_amount_usd * bonus
    flash = debt_amount_usd * FLASH_LOAN_FEE
    slip  = debt_amount_usd * DEX_SLIPPAGE
    net   = gross - flash - slip - gas_fee_usd

    return ProfitDetail(
        gross_profit  = gross,
        flash_fee     = flash,
        dex_slippage  = slip,
        gas_fee       = gas_fee_usd,
        net_profit    = net,
        is_profitable = net > MIN_PROFIT_USD,
    )


def build_profit_from_info(info: TargetInfo, gas_fee_usd: float) -> ProfitDetail:
    return calculate_profit(info.debt_amount_usd, info.bonus, gas_fee_usd)


def build_profit_simple(total_debt_usd: float, gas_fee_usd: float) -> ProfitDetail:
    """Asset Discovery başarısız olduğunda DEFAULT_BONUS ile fallback."""
    return calculate_profit(total_debt_usd * CLOSE_FACTOR, DEFAULT_BONUS, gas_fee_usd)


# ─────────────────────────────────────────────────────────────────────────────
# LOG FORMATLAMA
# ─────────────────────────────────────────────────────────────────────────────

_EMOJI = {"E-MODE": "⚠️ ", "HEDGE": "🔀", "NORMAL": "🔴"}


def log_hot_list_add(
    tag:      str,
    address:  str,
    hf:       float,
    debt_usd: float,
    info:     Optional[TargetInfo],
    profit:   Optional[ProfitDetail],
) -> None:
    """Hot_list'e eklenirken detaylı log: E-MODE/HEDGE etiketi + net kâr."""
    if info and profit:
        emoji   = _EMOJI.get(info.position_type, "🔴")
        kar_str = f"$+{profit.net_profit:.2f}" if profit.net_profit >= 0 \
                  else f"$-{abs(profit.net_profit):.2f}"
        logger.info(
            "[%s-COLD] %s HOT_LIST | %s | HF: %.4f | Borç: $%.2f | "
            "%s→%s [%s] | Net Kâr: %s",
            tag, emoji, address, hf, debt_usd,
            info.debt_asset, info.collateral_asset,
            info.position_type, kar_str,
        )
    else:
        logger.info(
            "[%s-COLD] 🔴 HOT_LIST | %s | HF: %.4f | Borç: $%.2f",
            tag, address, hf, debt_usd,
        )


def log_opportunity(tag: str, info: TargetInfo, profit: ProfitDetail) -> None:
    """Hedef kilitleme — suikastçı formatı."""
    bonus_pct  = info.bonus * 100
    type_label = f"[{info.position_type}] " if info.position_type != "NORMAL" else ""
    logger.warning("=" * 65)
    logger.warning("🚨 [%s] KÂRLI FIRSAT! %sHEDEFE KİLİTLENİLDİ!", tag, type_label)
    logger.warning(" ├─ Hedef Cüzdan    : %s", info.address)
    logger.warning(" ├─ Health Factor   : %.6f",  info.hf)
    logger.warning(" ├─ Pozisyon Tipi   : %s",    info.position_type)
    logger.warning(" ├─ Kapatılacak Borç: $%.2f  (Varlık: %s)",
                   info.debt_amount_usd, info.debt_asset)
    logger.warning(" ├─ Alınacak Teminat: $%.2f  (Varlık: %s) [Bonus: %%%g]",
                   info.collateral_usd, info.collateral_asset, bonus_pct)
    logger.warning(" ├─ Flash Loan Fee  : -$%.4f", profit.flash_fee)
    logger.warning(" ├─ DEX Slippage    : -$%.4f", profit.dex_slippage)
    logger.warning(" ├─ Gas Maliyeti    : -$%.4f", profit.gas_fee)
    logger.warning(" └─ BEKLENEN NET KÂR: $%.4f",  profit.net_profit)
    logger.warning("=" * 65)
    # ── İLERİDE: trigger_liquidation(ctx, info) ───────────────────────────────