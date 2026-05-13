"""
watcher.py — Ana Orkestra Şefi v5
-----------------------------------
Değişiklikler:
  1. Kârsız sayacı düzeltildi: 1.00–1.05 arasındaki cüzdanlar da
     kâr testinden geçirilir. Kârsız ise hot_list'e ALINMAZ.
  2. OP null account hataları için geliştirilmiş tolerans.
  3. Cold scan log formatı E-MODE/HEDGE etiketlerini gösterir.

Blockchain matematiği  → aave_utils.py
Konfigürasyon/adresler → config.py
"""

import asyncio
import logging
import time
from typing import List, Set

import aiohttp
from dotenv import load_dotenv

import config as cfg
from aave_utils import (
    ChainContext,
    build_context,
    multicall_account_data,
    discover_target,
    build_profit_from_info,
    build_profit_simple,
    load_reserve_cache,
    log_opportunity,
    log_hot_list_add,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# THE GRAPH — Borçlu Listesi
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


async def fetch_borrowers(session: aiohttp.ClientSession, ctx: ChainContext) -> List[str]:
    """
    The Graph'tan zincire ait aktif borçlu adresleri çeker.

    Şema seçimi (config.py'den):
      "positions"    → ARB, OP
      "userReserves" → BASE (ve diğer eski Aave schema kullanan subgraph'lar)

    Tolerans:
      - Kısmi GraphQL hataları (OP'taki null account kayıtları) taramayı durdurmaz.
      - data alanı tamamen boşsa döngüden çıkar.
    """
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
    skip = 0
    logger.info("[%s-COLD] Borçlu listesi çekiliyor (schema: %s)...", tag, schema)

    while True:
        try:
            async with session.post(
                ctx.config.subgraph_url,
                json={"query": query, "variables": {"first": cfg.GRAPH_BATCH_SIZE, "skip": skip}},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()

            # Kısmi hata toleransı: hata varsa uyar ama data içindeki geçerli
            # kayıtları işlemeye devam et (OP'un null account sorunu için kritik)
            if "errors" in data:
                err_count = len(data["errors"])
                logger.warning(
                    "[%s-COLD] GraphQL kısmi hata (%d kayıt atlandı), devam...",
                    tag, err_count,
                )
                if not data.get("data"):
                    break  # Hiç data yok → dur

            positions = (data.get("data") or {}).get(data_key) or []
            if not positions:
                break

            for p in positions:
                try:
                    addresses.add(AsyncWeb3.to_checksum_address(addr_fn(p)))
                except Exception:
                    continue  # null account vb. bozuk kayıt → atla

            if len(positions) < cfg.GRAPH_BATCH_SIZE:
                break
            skip += cfg.GRAPH_BATCH_SIZE

        except Exception as exc:
            logger.error("[%s-COLD] Graph isteği hatası: %s", tag, exc)
            break

    logger.info("[%s-COLD] %d tekil borçlu adres bulundu.", tag, len(addresses))
    return list(addresses)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — COLD SCAN (Geniş Radar)
# ─────────────────────────────────────────────────────────────────────────────

async def cold_scan_loop(session: aiohttp.ClientSession, ctx: ChainContext) -> None:
    """
    Her cold_interval saniyede bir tüm borçluları Multicall ile tarar.

    Sayaç mantığı (düzeltilmiş):
      zombi   → HF < 0.95
      kucuk   → borç < MIN_DEBT_USD
      karsiz  → net kâr < MIN_PROFIT_USD  (hem 1.00–1.05 hem de <1.00 için)
      firsat  → kârlı fırsat (HF < 1.00 ve kârlı)
      hot_add → hot_list'e eklenen (kârlı olma potansiyeli olan 1.00–1.05)

    ÖNEMLİ: 1.00–1.05 arasındaki cüzdan kârsızsa hot_list'e ALINMAZ.
    Bu sayede hot_list temiz kalır ve "Kârsız: 0" sorunu ortadan kalkar.
    """
    tag       = ctx.config.tag
    chain_cfg = ctx.config

    # İlk döngüde reserve cache yükle
    await load_reserve_cache(ctx)

    while True:
        try:
            logger.info("━" * 65)
            logger.info("[%s-COLD] Geniş Radar Taraması başlıyor...", tag)

            borrowers = await fetch_borrowers(session, ctx)
            if not borrowers:
                logger.warning(
                    "[%s-COLD] Borçlu listesi boş, %ds sonra tekrar...",
                    tag, chain_cfg.cold_interval,
                )
                await asyncio.sleep(chain_cfg.cold_interval)
                continue

            total    = len(borrowers)
            chunks   = [borrowers[i:i + cfg.COLD_CHUNK_SIZE] for i in range(0, total, cfg.COLD_CHUNK_SIZE)]
            scanned  = 0
            stats    = {"zombi": 0, "kucuk": 0, "karsiz": 0, "firsat": 0, "hot_add": 0}
            t0       = time.time()

            for chunk in chunks:
                results = await multicall_account_data(ctx, chunk)

                for address, hf, debt_usd in results:

                    # ── Güvenli → atla ───────────────────────────────────────
                    if hf >= cfg.HF_HOT_UPPER:
                        continue

                    # ── Zombi → atla ─────────────────────────────────────────
                    if hf < cfg.HF_ZOMBIE_MIN:
                        stats["zombi"] += 1
                        continue

                    # ── Küçük borç → gas karşılamaz, atla ────────────────────
                    if debt_usd < cfg.MIN_DEBT_USD:
                        stats["kucuk"] += 1
                        continue

                    # ── HF 1.00–1.05: Hot_list adayı ─────────────────────────
                    # Asset Discovery + kâr testi BURADA yapılır.
                    # Kârsız ise hot_list'e ALINMAZ → "Kârsız" sayacı artar.
                    if cfg.HF_LIQUIDATABLE <= hf < cfg.HF_HOT_UPPER:
                        if address not in ctx.hot_list:
                            info   = await discover_target(ctx, address, hf, debt_usd)
                            profit = (
                                build_profit_from_info(info, chain_cfg.gas_fee_usd)
                                if info else
                                build_profit_simple(debt_usd, chain_cfg.gas_fee_usd)
                            )

                            if not profit.is_profitable:
                                # Kârsız (E-Mode veya çok küçük borç) → hot_list'e alma
                                stats["karsiz"] += 1
                                logger.debug(
                                    "[%s-COLD] ⛔ Kârsız hot_list adayı atlandı | %s | "
                                    "HF: %.4f | %s→%s [%s] | Net: $%.2f",
                                    tag, address, hf,
                                    info.debt_asset if info else "?",
                                    info.collateral_asset if info else "?",
                                    info.position_type if info else "?",
                                    profit.net_profit,
                                )
                                continue

                            ctx.hot_list.add(address)
                            stats["hot_add"] += 1
                            log_hot_list_add(tag, address, hf, debt_usd, info, profit)
                        continue

                    # ── HF < 1.00: Direkt tasfiye fırsatı ────────────────────
                    info   = await discover_target(ctx, address, hf, debt_usd)
                    profit = (
                        build_profit_from_info(info, chain_cfg.gas_fee_usd)
                        if info else
                        build_profit_simple(debt_usd, chain_cfg.gas_fee_usd)
                    )

                    if not profit.is_profitable:
                        stats["karsiz"] += 1
                        continue

                    stats["firsat"] += 1
                    if info:
                        log_opportunity(f"{tag}-COLD", info, profit)
                    else:
                        logger.warning(
                            "🚨 [%s-COLD] FIRSAT | %s | HF: %.4f | Borç: $%.2f | Net: $%.2f",
                            tag, address, hf, debt_usd, profit.net_profit,
                        )

                scanned += len(chunk)
                logger.info(
                    "[%s-COLD] %d/%d | Hot: +%d | Zombi: %d | "
                    "Küçük: %d | Kârsız: %d | Fırsat: %d",
                    tag, scanned, total,
                    stats["hot_add"], stats["zombi"],
                    stats["kucuk"], stats["karsiz"], stats["firsat"],
                )

            elapsed = time.time() - t0
            logger.info(
                "[%s-COLD] ✅ Tamamlandı. Süre: %.1fs | Hız: %.0f/sn | Hot_list: %d",
                tag, elapsed, total / elapsed if elapsed > 0 else 0, len(ctx.hot_list),
            )

        except Exception as exc:
            logger.error("[%s-COLD] ❌ Döngü hatası: %s", tag, exc)

        logger.info("[%s-COLD] Sonraki tarama %ds sonra...", tag, chain_cfg.cold_interval)
        await asyncio.sleep(chain_cfg.cold_interval)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — HOT SCAN (Keskin Nişancı)
# ─────────────────────────────────────────────────────────────────────────────

async def hot_scan_loop(ctx: ChainContext) -> None:
    """
    Her hot_interval saniyede bir sadece hot_list'i Multicall ile tarar.
    hot_list zaten kâr testinden geçmiş adresler içerir (cold scan garantisi).
    """
    tag       = ctx.config.tag
    chain_cfg = ctx.config
    logger.info("[%s-HOT] Keskin Nişancı hazır. Hot_list dolmayı bekliyor...", tag)

    while True:
        try:
            if not ctx.hot_list:
                await asyncio.sleep(chain_cfg.hot_interval)
                continue

            addresses  = list(ctx.hot_list)
            chunks     = [
                addresses[i:i + cfg.HOT_CHUNK_SIZE]
                for i in range(0, len(addresses), cfg.HOT_CHUNK_SIZE)
            ]
            to_remove: Set[str] = set()

            for chunk in chunks:
                results = await multicall_account_data(ctx, chunk)

                for address, hf, debt_usd in results:

                    # ── Kurtuldu: HF güvenli bölgeye çıktı ───────────────────
                    if hf >= cfg.HF_HOT_REMOVE:
                        to_remove.add(address)
                        ctx.target_cache.pop(address, None)
                        logger.info(
                            "[%s-HOT] ✅ KURTULDU (HF güvenli) | %s | HF: %.4f",
                            tag, address, hf,
                        )
                        continue

                    # ── Kurtuldu: Borç ödendi ─────────────────────────────────
                    if debt_usd < cfg.MIN_DEBT_USD:
                        to_remove.add(address)
                        ctx.target_cache.pop(address, None)
                        logger.info(
                            "[%s-HOT] ✅ KURTULDU (borç ödendi) | %s | $%.2f",
                            tag, address, debt_usd,
                        )
                        continue

                    # ── Hâlâ tehlike bölgesinde (1.00–1.05) → izle ───────────
                    if hf >= cfg.HF_LIQUIDATABLE:
                        logger.debug(
                            "[%s-HOT] İzleniyor | %s | HF: %.4f | Borç: $%.2f",
                            tag, address, hf, debt_usd,
                        )
                        continue

                    # ── TETİK NOKTASI: HF < 1.00 ─────────────────────────────
                    # Cache'den gelir (cold scan'de zaten discover_target yapıldı)
                    info   = await discover_target(ctx, address, hf, debt_usd)
                    profit = (
                        build_profit_from_info(info, chain_cfg.gas_fee_usd)
                        if info else
                        build_profit_simple(debt_usd, chain_cfg.gas_fee_usd)
                    )

                    if not profit.is_profitable:
                        logger.debug(
                            "[%s-HOT] Kârsız | %s | Net: $%.4f [%s]",
                            tag, address, profit.net_profit,
                            info.position_type if info else "?",
                        )
                        continue

                    # 🚨 HEDEFE KİLİTLENİLDİ
                    if info:
                        log_opportunity(f"{tag}-HOT", info, profit)
                    else:
                        logger.warning("=" * 65)
                        logger.warning("🚨 [%s-HOT] KÂRLI FIRSAT! TETİK NOKTASI!", tag)
                        logger.warning(" ├─ Adres  : %s", address)
                        logger.warning(" ├─ HF     : %.6f", hf)
                        logger.warning(" ├─ Borç   : $%.2f", debt_usd)
                        logger.warning(" └─ Net Kâr: $%.4f", profit.net_profit)
                        logger.warning("=" * 65)

            if to_remove:
                ctx.hot_list -= to_remove
                logger.info(
                    "[%s-HOT] %d adres temizlendi. Kalan: %d",
                    tag, len(to_remove), len(ctx.hot_list),
                )

        except Exception as exc:
            logger.error("[%s-HOT] ❌ Döngü hatası: %s", tag, exc)

        await asyncio.sleep(chain_cfg.hot_interval)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    chain_configs = cfg.load_chains()

    if not chain_configs:
        logger.error("Hiçbir zincir aktif! .env dosyasına RPC URL ekle:")
        logger.error("  ARB_RPC=https://arb-mainnet.g.alchemy.com/v2/KEY")
        logger.error("  BASE_RPC=https://base-mainnet.g.alchemy.com/v2/KEY")
        logger.error("  OP_RPC=https://opt-mainnet.g.alchemy.com/v2/KEY")
        return

    logger.info("=" * 65)
    logger.info("  MULTI-CHAIN GOD MODE + TARGET LOCK v6")
    logger.info("  Aktif zincirler: %s", [c.tag for c in chain_configs])
    logger.info("=" * 65)

    contexts = []
    for chain_cfg_item in chain_configs:
        ctx = await build_context(chain_cfg_item)
        if ctx:
            contexts.append(ctx)

    if not contexts:
        logger.error("Hiçbir zincire bağlanılamadı.")
        return

    async with aiohttp.ClientSession() as session:
        tasks = []
        for ctx in contexts:
            tasks.append(asyncio.create_task(
                cold_scan_loop(session, ctx),
                name=f"cold-{ctx.config.tag}",
            ))
            tasks.append(asyncio.create_task(
                hot_scan_loop(ctx),
                name=f"hot-{ctx.config.tag}",
            ))

        logger.info("=" * 65)
        logger.info("  %d paralel task başlatıldı:", len(tasks))
        for t in tasks:
            logger.info("    • %s", t.get_name())
        logger.info("=" * 65)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results):
            if isinstance(result, Exception):
                logger.error(
                    "Task '%s' beklenmedik şekilde sonlandı: %s",
                    task.get_name(), result,
                )


if __name__ == "__main__":
    asyncio.run(main())