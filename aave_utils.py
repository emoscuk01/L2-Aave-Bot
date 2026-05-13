"""
aave_utils.py — Blockchain Matematik Motoru v11 (WSS + Ghost Recovery)
-----------------------------------------------------------------------
v10 → v11 Değişiklik Logu:

1. [YENİ] build_context() — WSS Öncelikli Bağlantı
   - ChainConfig.wss_url doluysa WebsocketProviderV2 ile bağlanır.
   - WSS başarısız olursa AsyncHTTPProvider'a sessizce düşer (fallback).
   - Her iki durumda da ChainContext.w3 tam olarak çalışır;
     multicall, contract.functions.foo().call() vb. değişmez.

2. [YENİ] WalletSnapshot.nonce alanı eklendi.
   - Hot_list'e yeni adres eklenirken cüzdanın o anki nonce'u kaydedilir.
   - Ghost Recovery tespiti için watcher.py bu alanı kullanır.
   - Nonce alınamazsa 0 olarak kalır (güvenli taraf: aktif recovery sayılır).

3. [YENİ] ChainContext.hot_scan_lock (asyncio.Lock) eklendi.
   - wss_block_listener her blokta hot_scan_once() task'ı oluşturur.
   - Önceki scan bitmeden yenisi başlamasın diye kilit mekanizması.

4. [KORUNDU] Tüm v10 matematik değişmedi:
   - Decimal Normalize (_usd_estimate)
   - Collateral Bottleneck downsize
   - Dinamik Close Factor (Zombie: %100, Normal: %50)
   - Cache hit'te taze CF + Bottleneck yeniden hesabı
"""

import asyncio
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
    HF_ZOMBIE_MIN,
)
from alchemy_cu_meter import cu_try_aggregate, record_rpc_cu
from rpc_rotator import (
    RPCRateLimited429,
    is_rate_limit_429 as _is_rate_limit_429,
    call_with_retry,
    get_rotator,
)
from wss_url_pool import build_wss_urls_for_watcher

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# VERİ YAPILARI
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AssetPosition:
    """Tek bir rezerv token'ındaki pozisyon (borç veya teminat)."""
    symbol:          str     # "USDC", "WETH", "TKN_A3F2B1"
    underlying_addr: str     # Dayanak varlık adresi (liquidationCall'a giden)
    decimals:        int
    amount_raw:      int     # Wei cinsinden ham miktar (on-chain değer)
    amount_usd:      float   # USD karşılığı (gösterim için)


@dataclass
class TargetInfo:
    """
    Bir hedef cüzdanın tam analizi.

    Liquidation payload için gereken alanlar:
      debt_asset_address       → Aave liquidationCall'ın debtAsset parametresi
      collateral_asset_address → Aave liquidationCall'ın collateralAsset parametresi
      debt_to_cover_wei        → Aave liquidationCall'ın debtToCover parametresi
                                 (SEÇİLEN borç tokeninin %50 veya %100'ü, Wei cinsinden)
    """
    address:                  str
    hf:                       float
    total_debt_usd:           float
    total_coll_usd:           float

    # Optimal pair — seçilen borç tokeni
    debt_asset:               str    # Sembol (log için)
    debt_asset_address:       str    # Underlying adres (payload için)
    debt_token_total_usd:     float  # Seçilen tokenin toplam borcu (USD)
    debt_amount_usd:          float  # Close factor uygulanmış (USD)
    debt_to_cover_wei:        int    # Close factor uygulanmış (Wei) ← payload

    # Optimal pair — seçilen teminat tokeni
    collateral_asset:         str    # Sembol (log için)
    collateral_asset_address: str    # Underlying adres (payload için)
    collateral_token_total_usd: float  # Seçilen tokenin toplam teminatı (USD)
    collateral_usd:           float  # Alınacak teminat tahmini (USD)

    bonus:                    float
    position_type:            str   # "E-MODE" | "HEDGE" | "NORMAL"
    effective_close_factor:   float = CLOSE_FACTOR  # 0.5 veya 1.0 (zombie)

    # Tüm pozisyonlar (log'da detay göstermek için)
    all_debts:       List[AssetPosition] = field(default_factory=list)
    all_collaterals: List[AssetPosition] = field(default_factory=list)


@dataclass
class ProfitDetail:
    gross_profit:  float
    flash_fee:     float
    dex_slippage:  float
    gas_fee:       float
    net_profit:    float
    is_profitable: bool


@dataclass
class WalletSnapshot:
    """
    Derin Otopsi + Ghost Recovery için cüzdan anlık görüntüsü.

    [v11 YENİ] nonce:
      Cüzdan hot_list'e alınırken kaydedilen Ethereum nonce değeri.
      Ghost Recovery tespitinde kullanılır:
        mevcut_nonce == snapshot.nonce  →  kullanıcı işlem yapmadı (pasif recovery)
        mevcut_nonce  > snapshot.nonce  →  kullanıcı gerçekten işlem yaptı (aktif)
    """
    hf:       float
    debt_usd: float
    coll_usd: float
    nonce:    int = 0   # [v11] Ghost Recovery için Ethereum nonce


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
    # token_address.lower() → sembol (ters arama — TKN_ çözümü için)
    addr_to_symbol:     Dict[str, str]             = field(default_factory=dict)
    wallet_snapshot:    Dict[str, WalletSnapshot]  = field(default_factory=dict)

    # [v11 YENİ] Hot scan eşzamanlılık kilidi.
    # wss_block_listener her yeni blokta hot_scan_once() task'ı açar.
    # Önceki scan bitmediyse yeni task kilit görerek atlar.
    hot_scan_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # newHeads gürültüsünü seyreltmek için WSS tetikleyicide kullanılır (watcher.py).
    hot_scan_last_dispatch_mono: float = 0.0
    hot_scan_next_allowed_block: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# YARDIMCI FONKSİYONLAR
# ─────────────────────────────────────────────────────────────────────────────

def _cs(addr: str) -> str:
    """EIP-55 checksum dönüşümü."""
    try:
        return AsyncWeb3.to_checksum_address(addr)
    except Exception:
        return addr


def _resolve_class(symbol: str) -> str:
    if symbol.startswith("TKN_"):
        return "ALT"
    return ASSET_CLASS.get(symbol, "ALT")


def _resolve_bonus(symbol: str) -> float:
    if symbol.startswith("TKN_"):
        return DEFAULT_BONUS
    return LIQUIDATION_BONUS_MAP.get(symbol, DEFAULT_BONUS)


def classify_position(debt_symbol: str, coll_symbol: str) -> Tuple[str, float]:
    """E-MODE / HEDGE / NORMAL sınıflandırması ve efektif bonus."""
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
# BAĞLANTI KURMA — WSS ÖNCELİKLİ
# ─────────────────────────────────────────────────────────────────────────────

def _http_fallback_candidates(cfg: ChainConfig, rot) -> List[str]:
    """
    WSS düştüğünde denenecek HTTP URL sırası.

    - Zincirin cfg.rpc_url host'u (örn. base-mainnet) ile şarjördeki URL'ler eşlenir;
      Arbitrum şarjör key'leri Base'e uygulanmaz.
    - Aynı host'ta birden fazla key varsa şarjör sırası (dosya öncelikli) korunur;
      cfg.rpc_url genelde en sonda denenir (sıklıkla tükenmiş anahtar).
    """
    from urllib.parse import urlparse

    primary = (cfg.rpc_url or "").strip()
    if not primary:
        return rot.all_urls
    want_host = urlparse(primary).hostname
    same = [u for u in rot.all_urls if urlparse(u).hostname == want_host]
    if not same:
        return [primary]
    seen: Set[str] = set()
    dedup: List[str] = []
    for u in same:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    if primary in dedup:
        dedup = [u for u in dedup if u != primary] + [primary]
    return dedup


async def build_context(cfg: ChainConfig) -> Optional["ChainContext"]:
    """
    [v11] WSS öncelikli bağlantı.

    Strateji:
      1. cfg.wss_url doluysa WebsocketProviderV2 ile bağlan.
         Bağlantıyı doğrulamak için chain_id çek (lazy connect tetikler).
      2. WSS başarısız olursa AsyncHTTPProvider fallback.
      3. İkisi de başarısız olursa None döndür (chain devre dışı kalır).

    Dönen ChainContext.w3 her iki durumda da aynı API'yi sunar;
    multicall ve contract.functions.foo().call() için WSS/HTTP farkı yoktur.
    """
    tag = cfg.tag
    w3: Optional[AsyncWeb3] = None

    # ── 1. WSS Denemesi (Web3 v7.x Uyumlu) — rotator key'leri ile ─────────────
    if cfg.wss_url:
        try:
            from web3 import AsyncWeb3, WebSocketProvider

            rot = get_rotator()
            wss_candidates = build_wss_urls_for_watcher(cfg, rot)
            if cfg.wss_url not in wss_candidates:
                wss_candidates.append(cfg.wss_url)

            for wss_url in wss_candidates:
                try:
                    w3 = AsyncWeb3(WebSocketProvider(wss_url))
                    await w3.provider.connect()
                    chain_id = await asyncio.wait_for(w3.eth.chain_id, timeout=15)
                    logger.info(
                        "[%s] WSS bağlı (...%s). Chain ID: %d",
                        tag, wss_url[-24:], chain_id,
                    )
                    break
                except Exception as wss_exc:
                    logger.debug(
                        "[%s] WSS denemesi başarısız (...%s): %s",
                        tag, wss_url[-24:], wss_exc,
                    )
                    w3 = None
                    continue

            if w3 is None:
                logger.warning(
                    "[%s] Tüm WSS adayları (%d) başarısız. HTTP fallback...",
                    tag, len(wss_candidates),
                )
        except ImportError:
            logger.warning(
                "[%s] WebSocketProvider import edilemedi. HTTP'ye düşülüyor...", tag,
            )
            w3 = None

    # ── 2. HTTP Fallback ──────────────────────────────────────────────────────
    if w3 is None:
        if not cfg.rpc_url:
            logger.error("[%s] WSS ve HTTP RPC ikisi de tanımsız!", tag)
            return None
        try:
            rot = get_rotator()
            candidates = _http_fallback_candidates(cfg, rot)
            last_exc: Optional[Exception] = None
            w3 = None
            http_url = ""
            for http_url in candidates:
                try:
                    try:
                        http_provider = AsyncHTTPProvider(
                            http_url,
                            request_information_cache_size=5000,
                        )
                    except TypeError:
                        http_provider = AsyncHTTPProvider(http_url)
                    w3_try = AsyncWeb3(http_provider)
                    if not await asyncio.wait_for(w3_try.is_connected(), timeout=10):
                        raise ConnectionError("is_connected() → False")
                    chain_id = await w3_try.eth.chain_id
                    rot.sync_index_to_url(http_url)
                    w3 = w3_try
                    logger.debug(
                        "[%s] ⚠️  HTTP fallback bağlı (%s). Chain ID: %d",
                        tag, http_url[:40] + "...", chain_id,
                    )
                    break
                except Exception as exc:
                    last_exc = exc
                    logger.warning(
                        "[%s] HTTP fallback denemesi başarısız (...%s): %s",
                        tag, http_url[-14:], exc,
                    )
                    continue
            if w3 is None:
                raise last_exc or ConnectionError("Tüm HTTP adayları başarısız")
        except Exception as exc:
            logger.error("[%s] Context oluşturulamadı (WSS+HTTP başarısız): %s", tag, exc)
            return None

    # ── 3. Kontrat objeleri ───────────────────────────────────────────────────
    try:
        return ChainContext(
            config             = cfg,
            w3                 = w3,
            pool_contract      = w3.eth.contract(address=cfg.pool_address,          abi=POOL_ABI),
            multicall_contract = w3.eth.contract(address=cfg.multicall_address,     abi=MULTICALL3_ABI),
            data_provider      = w3.eth.contract(address=cfg.data_provider_address, abi=DATA_PROVIDER_ABI),
        )
    except Exception as exc:
        logger.error("[%s] Kontrat nesneleri oluşturulamadı: %s", tag, exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MULTICALL — TOPLU HF TARAMASI (Adaptive Retry)
# ─────────────────────────────────────────────────────────────────────────────

async def _multicall_raw(ctx: ChainContext, addresses: List[str]) -> Optional[list]:
    calls = [
        (ctx.config.pool_address,
         ctx.pool_contract.encode_abi("getUserAccountData", args=[addr]))
        for addr in addresses
    ]
    rot = get_rotator()
    return await call_with_retry(
        lambda: ctx.multicall_contract.functions.tryAggregate(False, calls).call(),
        rot, ctx.w3,
        context_label="MULTICALL-HF",
        estimated_cu=cu_try_aggregate(len(calls)),
    )


async def multicall_account_data(
    ctx: ChainContext,
    addresses: List[str],
) -> List[Tuple[str, float, float, float]]:
    """
    getUserAccountData toplu sorgusu.
    Returns: [(address, hf, total_debt_usd, total_coll_usd), ...]

    Adaptive Retry: Hata alınırsa chunk ikiye bölünür, her yarı ayrı denenir.

    NOT: Return signature v10 ile aynı tutuldu.
    Ghost Recovery nonce üzerinden yapıldığından raw USD karşılaştırması
    gerekmedi (totalDebtBase zaten fiyat-bağımlı USD değeri taşıdığından
    raw karşılaştırma yanlış pozitif verebilir).
    """
    tag = ctx.config.tag

    try:
        results = await _multicall_raw(ctx, addresses)
    except Exception as exc:
        if _is_rate_limit_429(exc):
            logger.warning(
                "[%s] Multicall 429 / rate limit (chunk=%d) — tarama turu iptal, chunk bölünmüyor.",
                tag, len(addresses),
            )
            raise RPCRateLimited429(str(exc)) from exc
        logger.error(
            "[%s] Multicall HATA (chunk: %d) — ikiye bölünüyor. Hata: %s",
            tag, len(addresses), exc,
        )
        if len(addresses) <= 1:
            logger.error("[%s] Tek adres bile başarısız — RPC/kontrat sorunu!", tag)
            return []
        mid = len(addresses) // 2
        return (await multicall_account_data(ctx, addresses[:mid]) +
                await multicall_account_data(ctx, addresses[mid:]))

    if not results:
        logger.error(
            "[%s] Multicall boş dönüş (chunk: %d) — COLD_CHUNK_SIZE küçültün.",
            tag, len(addresses),
        )
        return []

    parsed = []
    for i, (success, return_data) in enumerate(results):
        if not success or not return_data:
            continue
        try:
            d      = decode(["uint256","uint256","uint256","uint256","uint256","uint256"], return_data)
            hf_wei = d[5]
            if hf_wei >= (2 ** 256 - 1):
                continue
            parsed.append((
                addresses[i],
                hf_wei / WAD,
                d[1] / USD_DECIMALS,   # totalDebtBase
                d[0] / USD_DECIMALS,   # totalCollateralBase
            ))
        except Exception:
            continue
    return parsed


# ─────────────────────────────────────────────────────────────────────────────
# RESERVE CACHE — Token Sembol + Adres Eşleştirmesi
# ─────────────────────────────────────────────────────────────────────────────

async def load_reserve_cache(ctx: ChainContext) -> Dict[str, Tuple[str, int]]:
    """
    Zincirdeki tüm reserve tokenlarını çekip cache'e alır.
    addr_to_symbol ters tablosunu da doldurur (TKN_ sembol çözümü için).
    """
    if ctx.reserve_cache:
        return ctx.reserve_cache

    tag = ctx.config.tag
    try:
        rot = get_rotator()
        tokens = await call_with_retry(
            lambda: ctx.data_provider.functions.getAllReservesTokens().call(),
            rot, ctx.w3,
            context_label=f"{tag}-RESERVES",
        )
    except Exception as exc:
        logger.error("[%s] getAllReservesTokens başarısız: %s", tag, exc)
        return ctx.reserve_cache

    known = 0
    for symbol, token_addr in tokens:
        safe = _cs(token_addr)
        # Ters arama — adres → sembol (TKN_ çözümü için kritik)
        ctx.addr_to_symbol[safe.lower()] = symbol

        eff_symbol = symbol if symbol in WHITELIST_SYMBOLS else f"TKN_{safe[-6:].upper()}"
        if symbol in WHITELIST_SYMBOLS:
            known += 1

        try:
            dec = await ctx.w3.eth.contract(address=safe, abi=ERC20_ABI).functions.decimals().call()
            record_rpc_cu(rpc_method="eth_call", context_label=f"{tag}-ERC20.decimals")
        except Exception:
            dec = 18

        ctx.reserve_cache[eff_symbol] = (safe, dec)

    logger.info(
        "[%s] Reserve cache: %d token (%d bilinen, %d TKN_ fallback)",
        tag, len(ctx.reserve_cache), known, len(ctx.reserve_cache) - known,
    )
    return ctx.reserve_cache


# ─────────────────────────────────────────────────────────────────────────────
# [v10 DÜZELTME #1] USD TAHMİNİ — DECIMAL NORMALIZE (KORUNDU)
# ─────────────────────────────────────────────────────────────────────────────

def _usd_estimate(
    amount_raw:       int,
    decimals:         int,
    total_usd:        float,
    total_normalized: float,   # sum(raw_i / 10**dec_i) — decimal-doğru normalize toplam
) -> float:
    """
    Ham token miktarını USD'ye çevirir. Decimal normalize edilmiş orantısal dağılım.

    Eski yöntemin sorunu:
      total_raw = WBTC_raw + WETH_raw
      WBTC (8 dec):  1 BTC  → 100_000_000
      WETH (18 dec): 1 ETH  → 1_000_000_000_000_000_000
      Toplam: ~1e18 → WBTC ağırlığı % 0.00000001 çıkıyordu (YANLIŞ!)

    Yeni yöntem:
      WBTC normalize: 100_000_000 / 10^8  = 1.0
      WETH normalize: 1e18       / 10^18  = 1.0
      Toplam: 2.0 → her biri %50 (DOĞRU)
    """
    if total_normalized <= 0 or total_usd <= 0:
        return 0.0
    normalized = amount_raw / (10 ** decimals)
    return (normalized / total_normalized) * total_usd


# ─────────────────────────────────────────────────────────────────────────────
# ASSET DISCOVERY v3 — Multi-Asset Parsing + Optimal Pair + Bottleneck Check
# ─────────────────────────────────────────────────────────────────────────────

async def discover_target(
    ctx: ChainContext,
    address: str,
    hf: float,
    total_debt_usd: float,
    total_coll_usd: float = 0.0,
) -> Optional[TargetInfo]:
    """
    Hedef cüzdanın tam varlık röntgeni:
      1. Tüm rezervler için getUserReserveData → Multicall ile tek çağrı
      2. Her token'ın borç/teminat miktarı ayrı ayrı hesaplanır
      3. [v10] Decimal normalize toplam üzerinden USD dağılımı
      4. Borçlar USD'ye göre sıralanır → en büyük borç = debt_asset
      5. Teminatlar USD'ye göre sıralanır → en büyük teminat = collateral_asset
      6. [v10] Dinamik Close Factor: HF < 0.95 → %100, aksi → %50
      7. [v10] Collateral Bottleneck Kontrolü + Downsize:
         debt_cover * (1+bonus) > max_coll → borcu küçült
      8. debt_to_cover_wei = ham borç × close_factor (on-chain payload için)
      9. TKN_ sembolleri addr_to_symbol üzerinden gerçek isme çevrilir

    Cache hit: hf/borç/teminat güncellenir, discovery tekrar yapılmaz.
    NOT: hot_scan tetik anında (HF < 1.00) cache dışarıdan pop edilir (watcher.py).
    """
    # ── [v10] Dinamik Close Factor ────────────────────────────────────────────
    # Zombie (HF < 0.95): Aave V3 tam tasfiyeye izin verir (CLOSE_FACTOR = 1.0)
    # Normal  (HF >= 0.95): %50 kural
    effective_close_factor = 1.0 if hf < HF_ZOMBIE_MIN else CLOSE_FACTOR

    # ── Cache hit ─────────────────────────────────────────────────────────────
    cached = ctx.target_cache.get(address)
    if cached:
        cached.hf                     = hf
        cached.total_debt_usd         = total_debt_usd
        cached.total_coll_usd         = total_coll_usd
        cached.effective_close_factor = effective_close_factor

        # Close factor ve bottleneck'i taze hf ile yeniden hesapla
        raw_debt_cf = int(cached.debt_to_cover_wei / (cached.effective_close_factor or CLOSE_FACTOR)
                          * effective_close_factor) \
                      if cached.effective_close_factor else cached.debt_to_cover_wei
        debt_usd_cf = cached.debt_token_total_usd * effective_close_factor

        # Bottleneck kontrolü (cache hit'te de geçerli)
        max_coll_usd      = cached.collateral_token_total_usd
        required_coll_usd = debt_usd_cf * (1 + cached.bonus)
        if required_coll_usd > max_coll_usd and max_coll_usd > 0:
            debt_usd_cf = max_coll_usd / (1 + cached.bonus)
            if cached.debt_token_total_usd > 0:
                ratio       = debt_usd_cf / cached.debt_token_total_usd
                raw_debt_cf = int(cached.debt_to_cover_wei
                                  / effective_close_factor * ratio)
            logger.debug(
                "[CACHE-DOWNSIZE] %s | Teminat: $%.2f → Borç downsize: $%.2f",
                address, max_coll_usd, debt_usd_cf,
            )

        cached.debt_amount_usd   = debt_usd_cf
        cached.debt_to_cover_wei = raw_debt_cf
        cached.collateral_usd    = debt_usd_cf * (1 + cached.bonus)
        return cached

    # ── Rezerv cache ──────────────────────────────────────────────────────────
    reserves = await load_reserve_cache(ctx)
    if not reserves:
        return None

    checksum_addr   = _cs(address)
    symbols_ordered = list(reserves.keys())

    # ── Tek Multicall: tüm tokenlar için getUserReserveData ───────────────────
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
        rot = get_rotator()
        results = await call_with_retry(
            lambda: ctx.multicall_contract.functions.tryAggregate(False, calls).call(),
            rot, ctx.w3,
            context_label=f"{ctx.config.tag}-DISCOVERY",
            estimated_cu=cu_try_aggregate(len(calls)),
        )
    except Exception as exc:
        logger.warning("[%s] Asset discovery Multicall hatası | %s: %s",
                       ctx.config.tag, address, exc)
        return None

    # ── Her token için pozisyonu ayrıştır ─────────────────────────────────────
    debt_positions: List[AssetPosition] = []
    coll_positions: List[AssetPosition] = []

    # [v10] Normalize toplamlar (raw yerine insan-birimine çevrilmiş)
    total_debt_normalized: float = 0.0
    total_coll_normalized: float = 0.0

    for i, (success, return_data) in enumerate(results):
        if not success or not return_data:
            continue
        try:
            sym      = symbols_ordered[i]
            decimals = reserves[sym][1]
            addr     = reserves[sym][0]

            # TKN_ sembollerini gerçek isme çevir
            if sym.startswith("TKN_"):
                real_sym    = ctx.addr_to_symbol.get(addr.lower(), sym)
                display_sym = real_sym if real_sym != sym else sym
            else:
                display_sym = sym

            d = decode(
                ["uint256","uint256","uint256","uint256","uint256",
                 "uint256","uint256","uint40","bool"],
                return_data,
            )
            a_balance          = d[0]   # currentATokenBalance (teminat)
            stable_debt        = d[1]   # currentStableDebt
            variable_debt      = d[2]   # currentVariableDebt
            collateral_enabled = d[8]   # usageAsCollateralEnabled

            total_debt_raw_token = stable_debt + variable_debt
            total_coll_raw_token = a_balance if collateral_enabled else 0

            divisor = 10 ** decimals

            if total_debt_raw_token > 0:
                total_debt_normalized += total_debt_raw_token / divisor
                debt_positions.append(AssetPosition(
                    symbol          = display_sym,
                    underlying_addr = addr,
                    decimals        = decimals,
                    amount_raw      = total_debt_raw_token,
                    amount_usd      = 0.0,  # Sonra hesaplanacak
                ))

            if total_coll_raw_token > 0:
                total_coll_normalized += total_coll_raw_token / divisor
                coll_positions.append(AssetPosition(
                    symbol          = display_sym,
                    underlying_addr = addr,
                    decimals        = decimals,
                    amount_raw      = total_coll_raw_token,
                    amount_usd      = 0.0,
                ))

        except Exception:
            continue

    if not debt_positions or not coll_positions:
        return None

    # ── [v10] USD değerlerini decimal-normalize orantıyla dağıt ──────────────
    for pos in debt_positions:
        pos.amount_usd = _usd_estimate(
            pos.amount_raw, pos.decimals,
            total_debt_usd, total_debt_normalized,
        )
    for pos in coll_positions:
        pos.amount_usd = _usd_estimate(
            pos.amount_raw, pos.decimals,
            total_coll_usd, total_coll_normalized,
        )

    # ── Optimal Pair: En büyük borç + En büyük teminat ───────────────────────
    debt_positions.sort(key=lambda p: p.amount_usd, reverse=True)
    coll_positions.sort(key=lambda p: p.amount_usd, reverse=True)

    best_debt = debt_positions[0]
    best_coll = coll_positions[0]

    # ── [v10] Dinamik Close Factor ────────────────────────────────────────────
    # Zombie (HF < 0.95): tüm borcu kapat (%100)
    # Normal  (HF >= 0.95): %50 close factor
    debt_to_cover_raw     = int(best_debt.amount_raw * effective_close_factor)
    debt_amount_usd_cover = best_debt.amount_usd * effective_close_factor

    # ── E-Mode / Hedge / Normal sınıflandırması ───────────────────────────────
    position_type, effective_bonus = classify_position(best_debt.symbol, best_coll.symbol)

    # ── [v10] Collateral Bottleneck Kontrolü + Downsize ───────────────────────
    #
    # Kural: liquidationCall() çağrısında alınacak teminat,
    #        hedefin elindeki spesifik teminat bakiyesini geçemez.
    # Formül:
    #   alınacak_teminat = debt_cover × (1 + bonus)
    #   eğer alınacak_teminat > max_coll → revert
    #
    # Çözüm (Downsize):
    #   debt_cover = max_coll / (1 + bonus)   (teminat sınırından geriye dön)
    max_coll_usd      = best_coll.amount_usd
    required_coll_usd = debt_amount_usd_cover * (1 + effective_bonus)

    if required_coll_usd > max_coll_usd and max_coll_usd > 0:
        # Teminat yetersiz — borcu downsize et
        debt_amount_usd_cover = max_coll_usd / (1 + effective_bonus)

        # Wei'yi orantılı küçült
        ratio             = debt_amount_usd_cover / best_debt.amount_usd \
                            if best_debt.amount_usd > 0 else 0.0
        debt_to_cover_raw = int(best_debt.amount_raw * ratio)

        logger.debug(
            "[DOWNSIZE] %s | %s borcu $%.2f'den $%.2f'e indirildi "
            "(Teminat tavanı: $%.2f, Bonus: %.1f%%)",
            address,
            best_debt.symbol,
            best_debt.amount_usd * effective_close_factor,
            debt_amount_usd_cover,
            max_coll_usd,
            effective_bonus * 100,
        )

    collateral_to_receive_usd = debt_amount_usd_cover * (1 + effective_bonus)

    info = TargetInfo(
        address                    = address,
        hf                         = hf,
        total_debt_usd             = total_debt_usd,
        total_coll_usd             = total_coll_usd,
        debt_asset                 = best_debt.symbol,
        debt_asset_address         = best_debt.underlying_addr,
        debt_token_total_usd       = best_debt.amount_usd,
        debt_amount_usd            = debt_amount_usd_cover,
        debt_to_cover_wei          = debt_to_cover_raw,
        collateral_asset           = best_coll.symbol,
        collateral_asset_address   = best_coll.underlying_addr,
        collateral_token_total_usd = best_coll.amount_usd,
        collateral_usd             = collateral_to_receive_usd,
        bonus                      = effective_bonus,
        position_type              = position_type,
        effective_close_factor     = effective_close_factor,
        all_debts                  = debt_positions,
        all_collaterals            = coll_positions,
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
    debt_amount_usd: Close factor + Bottleneck downsize uygulanmış miktar
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
    """Asset Discovery başarısız olduğunda fallback."""
    return calculate_profit(total_debt_usd * CLOSE_FACTOR, DEFAULT_BONUS, gas_fee_usd)


# ─────────────────────────────────────────────────────────────────────────────
# LOG FORMATLAMA
# ─────────────────────────────────────────────────────────────────────────────

def log_hot_list_add(tag: str, address: str, hf: float, debt_usd: float,
                     info: Optional[TargetInfo], profit: Optional[ProfitDetail]) -> None:
    if info and profit:
        kar = f"+${profit.net_profit:.2f}" if profit.net_profit >= 0 \
              else f"-${abs(profit.net_profit):.2f}"
        cf_pct = int(info.effective_close_factor * 100)
        logger.info(
            "[%s-COLD] HOT_LIST | %s | HF: %.4f | "
            "ToplamBorc: $%.2f | ToplamTeminat: $%.2f | "
            "%s($%.2f)->%s($%.2f) [%s] | CF: %%%d | Net: %s",
            tag, address, hf, debt_usd, info.total_coll_usd,
            info.debt_asset, info.debt_token_total_usd,
            info.collateral_asset, info.collateral_token_total_usd,
            info.position_type, cf_pct, kar,
        )
    else:
        logger.info("[%s-COLD] HOT_LIST | %s | HF: %.4f | Borç: $%.2f",
                    tag, address, hf, debt_usd)


def log_opportunity(tag: str, info: TargetInfo, profit: ProfitDetail) -> None:
    """
    Terminale detaylı fırsat logu — Optimal Pair + Close Factor + Bottleneck gösterir.
    """
    bonus_pct  = info.bonus * 100
    type_label = f"[{info.position_type}] " if info.position_type != "NORMAL" else ""
    cf_label   = "TAM TASFIYE (Zombie)" if info.effective_close_factor == 1.0 else "%50 Kural"

    logger.warning("=" * 68)
    logger.warning("KÂRLI FIRSAT! %s[%s] HEDEFE KİLİTLENİLDİ!", type_label, tag)
    logger.warning(" Cüzdan          : %s", info.address)
    logger.warning(" Health Factor   : %.6f", info.hf)
    logger.warning(" Pozisyon Tipi   : %s",   info.position_type)
    logger.warning(" Close Factor    : %.0f%% (%s)", info.effective_close_factor * 100, cf_label)
    logger.warning("─" * 68)
    logger.warning(" GENEL DURUM:")
    logger.warning("   Total Teminat : $%.2f", info.total_coll_usd)
    logger.warning("   Total Borç    : $%.2f", info.total_debt_usd)

    if info.all_debts:
        logger.warning(" TÜM BORÇLAR (büyükten küçüğe):")
        for p in info.all_debts:
            marker = " <-- SEÇİLDİ" if p.symbol == info.debt_asset else ""
            logger.warning("   %s: $%.2f%s", p.symbol, p.amount_usd, marker)

    if info.all_collaterals:
        logger.warning(" TÜM TEMİNATLAR (büyükten küçüğe):")
        for p in info.all_collaterals:
            marker = " <-- SEÇİLDİ" if p.symbol == info.collateral_asset else ""
            logger.warning("   %s: $%.2f%s", p.symbol, p.amount_usd, marker)

    logger.warning("─" * 68)
    logger.warning(" TASFİYE STRATEJİSİ (Optimal Pair):")
    logger.warning("   Kapatılacak Borç    : %s (Toplam: $%.2f)",
                   info.debt_asset, info.debt_token_total_usd)
    logger.warning("   Alınacak Teminat    : %s (Toplam: $%.2f) [Bonus: %%%g]",
                   info.collateral_asset, info.collateral_token_total_usd, bonus_pct)
    logger.warning("   %s Miktarı: $%.2f değeri %s",
                   cf_label, info.debt_amount_usd, info.debt_asset)
    logger.warning("   debtToCover (Wei)   : %d", info.debt_to_cover_wei)
    logger.warning("   collateralAsset ($) : $%.2f (sınır içinde ✓)", info.collateral_usd)
    logger.warning("─" * 68)
    logger.warning(" KÂR ANALİZİ:")
    logger.warning("   Flash Loan Fee   : -$%.4f", profit.flash_fee)
    logger.warning("   DEX Slippage     : -$%.4f", profit.dex_slippage)
    logger.warning("   Gas Maliyeti     : -$%.4f", profit.gas_fee)
    logger.warning("   NET KÂR          : $%.4f",  profit.net_profit)
    logger.warning("=" * 68)
    # ── PAYLOAD (ileride Solidity kontratına gidecek) ────────────────────────
    # {
    #   collateralAsset: info.collateral_asset_address,
    #   debtAsset:       info.debt_asset_address,
    #   user:            info.address,
    #   debtToCover:     info.debt_to_cover_wei,
    #   receiveAToken:   False,
    # }