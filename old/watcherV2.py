"""
watcher.py — WSS Pub/Sub + Ghost Recovery + JSON Export Bridge v12
-------------------------------------------------------------------
Kullanım:
    python watcher.py ARB
    python watcher.py BASE
    python watcher.py OP
    python watcher.py           # Tüm zincirler

.env dosyasına ekle:
    TELEGRAM_CHAT_ID=-100xxxxxxxxxx   (grup ID'si)

──────────────────────────────────────────────────────────────────────────────
v11 → v12 Değişiklik Logu:

[YENİ] TargetsStore — Streamlit için JSON Export Köprüsü
  Problem: Streamlit arayüzü watcher'ın bellekteki hot_list'ini göremez.
  Çözüm: targets.json köprü dosyası — watcher yazar, Streamlit okur.

  Mimari (Producer-Consumer, ana döngüyü yavaşlatmaz):
    • upsert() / remove(): ANINDA döner — sadece asyncio.Queue'ya yazar.
    • targets_writer_task: tek arka plan worker. Kuyruğu boşaltır,
      bellekte birleştirir (coalesce), ardından asyncio.to_thread ile
      json.dump'ı thread pool'a iter. Ana event loop ASLA bloke olmaz.
    • Tek consumer → Lock gerekmez.
    • Atomik yazım: .tmp → os.replace() (yarım dosya riski yok).
    • Kuyruk doygunluk koruması: TARGETS_MAX_QUEUE (2048). Dolunca
      en eski girdi düşürülür (circuit breaker).

  Dosya formatı (her kayıt):
    {
      "chain":          "ARB",
      "address":        "0x...",
      "hf":             1.0231,
      "debt_usd":       1500.50,
      "collateral_usd": 1601.20,
      "updated_at":     "14:30:00"
    }

  Dokunulan noktalar:
    cold_scan_loop   → hot_list.add sonrası   : upsert (ilk ekleme)
    hot_scan_once    → snap is None (yeni)    : upsert (ilk görüş)
    hot_scan_once    → Ghost kendi_odedi pasif: upsert (taze HF, listede kal)
    hot_scan_once    → İzle adımı (7)         : upsert (HF güncellemesi)
    hot_scan_once    → Tetik adımı (8)        : upsert (HF < 1.00, taze)
    hot_scan_once    → Tüm to_remove yolları  : remove (listeden çıkarma)
──────────────────────────────────────────────────────────────────────────────
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import websockets
from dotenv import load_dotenv

import config as cfg
from aave_utils import (
    ChainContext, WalletSnapshot,
    build_context,
    multicall_account_data,
    discover_target,
    build_profit_from_info,
    build_profit_simple,
    load_reserve_cache,
    log_opportunity,
    log_hot_list_add,
)
from telegram_utils import (
    TelegramNotifier,
    fmt_scan_start, fmt_scan_done,
    fmt_hot_list_add, fmt_opportunity,
    fmt_autopsy_liquidated, fmt_autopsy_repaid,
)

load_dotenv()

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

# ── Telegram ──────────────────────────────────────────────────────────────────
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
tg = TelegramNotifier(chat_id=_CHAT_ID)

# ── API Kalkanı sabitleri ─────────────────────────────────────────────────────
NULL_STREAK_LIMIT = 3
MAX_SKIP_LIMIT    = 150_000
AUTOPSY_DEBT_DROP = 0.40
AUTOPSY_COLL_DROP = 0.30

# ── WSS sabitleri ─────────────────────────────────────────────────────────────
WSS_PING_INTERVAL   = 30
WSS_PING_TIMEOUT    = 15
WSS_CLOSE_TIMEOUT   = 5
WSS_MAX_MSG_SIZE    = 2**20
WSS_INITIAL_BACKOFF = 2
WSS_MAX_BACKOFF     = 60

# ── JSON Export sabitleri ─────────────────────────────────────────────────────
TARGETS_FILE      = "targets.json"
TARGETS_MAX_QUEUE = 2048   # Circuit breaker — dolunca en eski düşürülür


# ─────────────────────────────────────────────────────────────────────────────
# TARGETS STORE — Streamlit JSON Export Köprüsü
# ─────────────────────────────────────────────────────────────────────────────

class TargetsStore:
    """
    Bellekteki hot_list verilerini targets.json'a yansıtan köprü.

    Kullanım:
        targets_store.upsert("ARB", address, hf, debt_usd, coll_usd)
        targets_store.remove(address)
        # main() içinde bir kez:
        asyncio.create_task(targets_store.writer_loop())
    """

    # Kuyruk öğesi tipi: (op, address, payload_or_None)
    _Op = Tuple[str, str, Optional[Dict[str, Any]]]

    def __init__(self, filepath: str = TARGETS_FILE) -> None:
        self._filepath = filepath
        self._data: Dict[str, Dict[str, Any]] = {}  # address → kayıt
        self._queue: asyncio.Queue = asyncio.Queue()

    # ── Public API (tamamen non-blocking) ─────────────────────────────────────

    def upsert(
        self,
        chain:          str,
        address:        str,
        hf:             float,
        debt_usd:       float,
        collateral_usd: float,
    ) -> None:
        """
        Cüzdanı targets.json'a ekle veya güncelle.
        Anında döner. Event loop'u asla bloke etmez.
        """
        record: Dict[str, Any] = {
            "chain":          chain,
            "address":        address,
            "hf":             round(hf, 6),
            "debt_usd":       round(debt_usd, 2),
            "collateral_usd": round(collateral_usd, 2),
            "updated_at":     datetime.now().strftime("%H:%M:%S"),
        }
        self._enqueue(("upsert", address, record))

    def remove(self, address: str) -> None:
        """
        Cüzdanı targets.json'dan sil.
        Anında döner. Event loop'u asla bloke etmez.
        """
        self._enqueue(("remove", address, None))

    # ── Writer (arka plan task) ───────────────────────────────────────────────

    async def writer_loop(self) -> None:
        """
        Kuyruğu sürekli dinleyen tek arka plan worker.
        main() içinde asyncio.create_task() ile bir kez başlatılır.

        Algoritma — coalesce + to_thread:
          1. İlk öğeyi await ile bekle (CPU harcanmaz).
          2. Kuyruktaki tüm öğeleri senkron drainle (burst coalesce).
          3. _data tablosunu güncelle.
          4. json.dump → asyncio.to_thread → thread pool'da çalışır,
             event loop bloke olmaz.
        500 adres aynı anda gelirse → 1 disk yazımı, 500 güncelleme.
        """
        logger.info("[TargetsStore] Writer task başladı → %s", self._filepath)

        while True:
            try:
                # ── 1. İlk öğeyi bekle ────────────────────────────────────────
                first = await self._queue.get()
                ops: List[TargetsStore._Op] = [first]

                # ── 2. Geri kalanları senkron drainle (coalesce) ──────────────
                while not self._queue.empty():
                    try:
                        ops.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                # ── 3. Bellek tablosunu güncelle ──────────────────────────────
                for op, address, payload in ops:
                    if op == "upsert" and payload is not None:
                        self._data[address] = payload
                    elif op == "remove":
                        self._data.pop(address, None)

                # ── 4. Diske yaz — thread pool'da, event loop bloke olmaz ─────
                snapshot = list(self._data.values())
                await asyncio.to_thread(self._write_file, snapshot)

                logger.debug(
                    "[TargetsStore] %d işlem → %d upsert / %d remove | "
                    "Toplam aktif: %d",
                    len(ops),
                    sum(1 for o, _, __ in ops if o == "upsert"),
                    sum(1 for o, _, __ in ops if o == "remove"),
                    len(self._data),
                )

            except asyncio.CancelledError:
                logger.info("[TargetsStore] Writer task iptal edildi.")
                return
            except Exception as exc:
                logger.error("[TargetsStore] Writer hata: %s", exc)
                await asyncio.sleep(0.5)   # CPU spin engellemesi

    # ── Özel yardımcılar ─────────────────────────────────────────────────────

    def _enqueue(self, op: _Op) -> None:
        """
        Kuyruğa yaz. Kuyruk doluysa en eski girdi düşürülür (circuit breaker).
        Sadece son durum önemli olduğu için veri bütünlüğü korunur.
        """
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
        """
        JSON dosyasına atomik yazım (asyncio.to_thread içinde çalışır).
        .tmp → os.replace(): yarım dosya riski sıfır.
        """
        tmp = self._filepath + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(records, fh, ensure_ascii=False, indent=2)
            os.replace(tmp, self._filepath)
        except Exception as exc:
            logger.error("[TargetsStore] Dosya yazım hatası: %s", exc)
            try:
                os.remove(tmp)
            except OSError:
                pass


# ── Global singleton ─ tüm zincirler aynı targets.json'a yazar ───────────────
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

async def fetch_borrowers(session: aiohttp.ClientSession, ctx: ChainContext) -> List[str]:
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

    logger.info("[%s-COLD] Borçlu listesi çekiliyor (schema: %s)...", tag, schema)

    while True:
        if skip >= MAX_SKIP_LIMIT:
            logger.warning("[%s-COLD] API KALKANI: skip=%d sınırı. (%d adres)", tag, skip, len(addresses))
            break

        try:
            async with session.post(
                ctx.config.subgraph_url,
                json={"query": query, "variables": {"first": cfg.GRAPH_BATCH_SIZE, "skip": skip}},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
        except Exception as exc:
            logger.error("[%s-COLD] Graph isteği hatası: %s", tag, exc)
            null_streak += 1
            if null_streak >= NULL_STREAK_LIMIT:
                logger.warning("[%s-COLD] API KALKANI: %d ardışık hata.", tag, null_streak)
                break
            await asyncio.sleep(2)
            continue

        if "errors" in data:
            logger.debug("[%s-COLD] GraphQL kısmi hata (%d kayıt), devam...", tag, len(data["errors"]))
            if data.get("data") is None:
                null_streak += 1
                if null_streak >= NULL_STREAK_LIMIT:
                    logger.warning("[%s-COLD] API KALKANI: %d null. Döngü bitti.", tag, null_streak)
                    break
                await asyncio.sleep(1)
                continue

        positions = (data.get("data") or {}).get(data_key) or []

        if not positions:
            null_streak += 1
            if null_streak >= NULL_STREAK_LIMIT:
                logger.info("[%s-COLD] API KALKANI: %d boş sayfa, tamamlandı. (%d adres)", tag, null_streak, len(addresses))
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

    logger.info("[%s-COLD] %d tekil borçlu adres bulundu.", tag, len(addresses))
    return list(addresses)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — COLD SCAN
# ─────────────────────────────────────────────────────────────────────────────

async def cold_scan_loop(session: aiohttp.ClientSession, ctx: ChainContext) -> None:
    tag       = ctx.config.tag
    chain_cfg = ctx.config

    await load_reserve_cache(ctx)

    while True:
        try:
            logger.info("━" * 65)
            logger.info("[%s-COLD] Geniş Radar Taraması başlıyor...", tag)

            borrowers = await fetch_borrowers(session, ctx)
            if not borrowers:
                logger.warning("[%s-COLD] Borçlu listesi boş, %ds sonra...", tag, chain_cfg.cold_interval)
                await asyncio.sleep(chain_cfg.cold_interval)
                continue

            tg.send(fmt_scan_start(tag, len(borrowers)))

            total   = len(borrowers)
            chunks  = [borrowers[i:i + cfg.COLD_CHUNK_SIZE] for i in range(0, total, cfg.COLD_CHUNK_SIZE)]
            scanned = 0
            stats   = {"guvenli": 0, "kucuk": 0, "karsiz": 0, "hot_add": 0, "firsat": 0}
            t0      = time.time()

            for chunk in chunks:
                results = await multicall_account_data(ctx, chunk)

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
                            info   = await discover_target(ctx, address, hf, debt_usd, coll_usd)
                            profit = (
                                build_profit_from_info(info, chain_cfg.gas_fee_usd)
                                if info else
                                build_profit_simple(debt_usd, chain_cfg.gas_fee_usd)
                            )
                            if not profit.is_profitable:
                                stats["karsiz"] += 1
                                continue

                            try:
                                _nonce = await ctx.w3.eth.get_transaction_count(address)
                            except Exception:
                                _nonce = 0

                            ctx.hot_list.add(address)
                            ctx.wallet_snapshot[address] = WalletSnapshot(
                                hf, debt_usd, coll_usd, nonce=_nonce,
                            )
                            stats["hot_add"] += 1
                            log_hot_list_add(tag, address, hf, debt_usd, info, profit)

                            # [v12] JSON Export: hot_list'e ilk ekleme
                            targets_store.upsert(tag, address, hf, debt_usd, coll_usd)

                            if info:
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

                    # ── HF < 1.00: Direkt fırsat ──────────────────────────────
                    info   = await discover_target(ctx, address, hf, debt_usd, coll_usd)
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
                        tg.send(fmt_opportunity(
                            f"{tag}-COLD", address, hf,
                            info.debt_amount_usd, info.debt_asset,
                            info.collateral_usd, info.collateral_asset,
                            info.bonus * 100,
                            profit.flash_fee, profit.dex_slippage,
                            profit.gas_fee, profit.net_profit,
                        ))

                scanned += len(chunk)
                logger.info(
                    "[%s-COLD] %d/%d | Güvenli: %d | Küçük: %d | "
                    "Kârsız: %d | Hot: +%d | Fırsat: %d",
                    tag, scanned, total,
                    stats["guvenli"], stats["kucuk"],
                    stats["karsiz"], stats["hot_add"], stats["firsat"],
                )

            elapsed = time.time() - t0
            hiz     = total / elapsed if elapsed > 0 else 0
            logger.info(
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

        logger.info("[%s-COLD] Sonraki tarama %ds sonra...", tag, chain_cfg.cold_interval)
        await asyncio.sleep(chain_cfg.cold_interval)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2a — HOT SCAN (TEK SEFERLİK)
# ─────────────────────────────────────────────────────────────────────────────

async def hot_scan_once(ctx: ChainContext, block_num: int) -> None:
    """
    Hot_list'teki tüm adresleri tek seferlik tarar.
    wss_block_listener() her yeni blokta bu fonksiyonu Task olarak açar.

    Karar ağacı + JSON Export touch points:
      1. Snapshot al (snap is None → upsert: ilk görüş)
      2. BAŞKASI VURDU                → remove
      3. KENDİ ODEDİ — Ghost (pasif) → upsert (sessiz güncelleme, listede kal)
      3. KENDİ ODEDİ — Aktif         → remove
      4. TOZ                          → remove
      5. KURTULDU (Ghost veya aktif)  → remove
      6. İZLE (HF 1.00–1.05)          → upsert (taze HF)
      7. TETİK (HF < 1.00)            → upsert (taze HF) + fırsat kontrolü
    """

    if ctx.hot_scan_lock.locked():
        logger.debug("[%s-HOT] Blok #%d atlandı — önceki scan devam ediyor.", ctx.config.tag, block_num)
        return

    async with ctx.hot_scan_lock:
        tag       = ctx.config.tag
        chain_cfg = ctx.config

        try:
            if not ctx.hot_list:
                return

            addresses  = list(ctx.hot_list)
            chunks     = [
                addresses[i:i + cfg.HOT_CHUNK_SIZE]
                for i in range(0, len(addresses), cfg.HOT_CHUNK_SIZE)
            ]
            to_remove: Set[str] = set()

            for chunk in chunks:
                results = await multicall_account_data(ctx, chunk)

                import time
                start_time = time.perf_counter()

                for address, hf, debt_usd, coll_usd in results:

                    # ── 1. Snapshot al / güncelle ─────────────────────────────
                    snap = ctx.wallet_snapshot.get(address)
                    if snap is None:
                        try:
                            _nonce = await ctx.w3.eth.get_transaction_count(address)
                        except Exception:
                            _nonce = 0
                        ctx.wallet_snapshot[address] = WalletSnapshot(
                            hf, debt_usd, coll_usd, nonce=_nonce,
                        )
                        # [v12] İlk kez hot_scan'da görüldü → JSON'a yaz
                        targets_store.upsert(tag, address, hf, debt_usd, coll_usd)
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
                    baskasin_vurdu_a = (
                        debt_drop >= 0.50 and
                        coll_drop >= AUTOPSY_COLL_DROP and
                        prev_debt > cfg.MIN_DEBT_USD
                    )
                    baskasin_vurdu_b = (
                        prev_debt >= cfg.MIN_DEBT_USD and
                        debt_usd < cfg.MIN_DEBT_USD and
                        debt_drop >= 0.50
                    )

                    if baskasin_vurdu_a or baskasin_vurdu_b:
                        logger.warning(
                            "[%s-HOT] BAŞKASI VURDU! | %s | "
                            "Borç: $%.2f→$%.2f (-%.0f%%) | "
                            "Teminat: $%.2f→$%.2f (-%.0f%%) | "
                            "HF: %.4f→%.4f",
                            tag, address,
                            prev_debt, debt_usd, debt_drop * 100,
                            prev_coll, coll_usd, coll_drop * 100,
                            prev_hf, hf,
                        )
                        try:
                            tg.send(fmt_autopsy_liquidated(
                                tag, address, prev_hf, hf, prev_debt, debt_usd, prev_coll, coll_usd,
                            ))
                        except Exception as tg_exc:
                            logger.debug("Telegram hata (baskasin_vurdu): %s", tg_exc)

                        targets_store.remove(address)      # [v12]
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
                        except Exception:
                            current_nonce = prev_nonce + 1

                        is_passive = (prev_nonce > 0 and current_nonce == prev_nonce)

                        if is_passive:
                            # GHOST — sadece fiyat hareketi, listede kal
                            logger.debug(
                                "[%s-HOT] GHOST (kendi_odedi pasif) | %s | "
                                "HF: %.4f | debt_drop: %.1f%% | Nonce: %d (değişmedi)",
                                tag, address, hf, debt_drop * 100, current_nonce,
                            )
                            ctx.wallet_snapshot[address] = WalletSnapshot(
                                hf, debt_usd, coll_usd, nonce=prev_nonce,
                            )
                            # [v12] Ghost'ta taze değerleri yaz, listede kal
                            targets_store.upsert(tag, address, hf, debt_usd, coll_usd)
                            continue

                        # Gerçek ödeme
                        logger.info(
                            "[%s-HOT] KENDİ ODEDİ | %s | "
                            "Borç: $%.2f→$%.2f (-%.0f%%) | "
                            "HF: %.4f→%.4f | Nonce: %d→%d",
                            tag, address, prev_debt, debt_usd,
                            debt_drop * 100, prev_hf, hf, prev_nonce, current_nonce,
                        )
                        try:
                            tg.send(fmt_autopsy_repaid(
                                tag, address, prev_hf, hf, prev_debt, debt_usd, prev_coll, coll_usd,
                            ))
                        except Exception as tg_exc:
                            logger.debug("Telegram hata (kendi_odedi): %s", tg_exc)

                        targets_store.remove(address)      # [v12]
                        to_remove.add(address)
                        ctx.target_cache.pop(address, None)
                        ctx.wallet_snapshot.pop(address, None)
                        continue

                    # ── 4. TOZ HESAP ──────────────────────────────────────────
                    if debt_usd < cfg.MIN_DEBT_USD:
                        logger.info(
                            "[%s-HOT] TOZ HESAP | %s | Borç: $%.4f < $%.0f",
                            tag, address, debt_usd, cfg.MIN_DEBT_USD,
                        )
                        if prev_debt >= cfg.MIN_DEBT_USD:
                            try:
                                tg.send(fmt_autopsy_liquidated(
                                    tag, address, prev_hf, hf, prev_debt, debt_usd, prev_coll, coll_usd,
                                ))
                            except Exception as tg_exc:
                                logger.debug("Telegram hata (toz): %s", tg_exc)

                        targets_store.remove(address)      # [v12]
                        to_remove.add(address)
                        ctx.target_cache.pop(address, None)
                        ctx.wallet_snapshot.pop(address, None)
                        continue

                    # ── 5. KURTULDU — Ghost Recovery korumalı ─────────────────
                    if hf >= cfg.HF_HOT_REMOVE:
                        try:
                            current_nonce = await ctx.w3.eth.get_transaction_count(address)
                        except Exception:
                            current_nonce = prev_nonce + 1

                        is_passive = (prev_nonce > 0 and current_nonce == prev_nonce)

                        if is_passive:
                            logger.info(
                                "[%s-HOT] 👻 GHOST RECOVERY (pasif) | %s | "
                                "HF: %.4f→%.4f | Nonce: %d (değişmedi) | Sessiz çıkış",
                                tag, address, prev_hf, hf, current_nonce,
                            )
                        else:
                            logger.info(
                                "[%s-HOT] 🟢 KURTULDU | %s | "
                                "HF: %.4f→%.4f | Borç: $%.2f | Nonce: %d→%d",
                                tag, address, prev_hf, hf, debt_usd,
                                prev_nonce, current_nonce,
                            )
                            try:
                                tg.send(fmt_autopsy_repaid(
                                    tag, address, prev_hf, hf, prev_debt, debt_usd, prev_coll, coll_usd,
                                ))
                            except Exception as tg_exc:
                                logger.debug("Telegram hata (kurtuldu): %s", tg_exc)

                        # [v12] Her iki durumda da JSON'dan sil (HF güvenli)
                        targets_store.remove(address)
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
                        # [v12] HF değişmiş olabilir — taze değerleri yaz
                        targets_store.upsert(tag, address, hf, debt_usd, coll_usd)
                        continue

                    # ── 7. TETİK: HF < 1.00 ──────────────────────────────────
                    ctx.target_cache.pop(address, None)   # Cache temizle — taze payload zorunlu

                    info   = await discover_target(ctx, address, hf, debt_usd, coll_usd)
                    profit = (
                        build_profit_from_info(info, chain_cfg.gas_fee_usd)
                        if info else
                        build_profit_simple(debt_usd, chain_cfg.gas_fee_usd)
                    )

                    # [v12] Tetik anında taze HF < 1.00 değerini yaz
                    targets_store.upsert(tag, address, hf, debt_usd, coll_usd)

                    if not profit.is_profitable:
                        logger.debug(
                            "[%s-HOT] Kârsız | %s | Net: $%.4f [%s]",
                            tag, address, profit.net_profit,
                            info.position_type if info else "?",
                        )
                        continue

                    # FIRSAT!
                    if info:
                        log_opportunity(f"{tag}-HOT", info, profit)
                        try:
                            tg.send(fmt_opportunity(
                                f"{tag}-HOT", address, hf,
                                info.debt_amount_usd, info.debt_asset,
                                info.collateral_usd,  info.collateral_asset,
                                info.bonus * 100,
                                profit.flash_fee, profit.dex_slippage,
                                profit.gas_fee, profit.net_profit,
                            ))
                        except Exception as tg_exc:
                            logger.debug("Telegram hata (fırsat): %s", tg_exc)
                    else:
                        logger.warning(
                            "KÂRLI FIRSAT! [%s-HOT] | %s | HF: %.4f | Net: $%.2f",
                            tag, address, hf, profit.net_profit,
                        )
                # ⏱️ --- SAF PYTHON KRONOMETRESİ BİTİYOR ---
                # (for address... döngüsü bittikten hemen sonra, chunk döngüsünün içinde)
                end_time = time.perf_counter()
                pure_python_ms = (end_time - start_time) * 1000
                logger.info("[%s-WSS] 🧠 Saf Python İşlemcisi: %.3f ms (İncelenen: %d cüzdan)", 
                            tag, pure_python_ms, len(chunk))

            if to_remove:
                ctx.hot_list -= to_remove
                logger.info(
                    "[%s-HOT] Blok #%d | %d adres temizlendi. Kalan: %d",
                    tag, block_num, len(to_remove), len(ctx.hot_list),
                )

        except Exception as exc:
            logger.error("[%s-HOT] hot_scan_once hatası (blok #%d): %s", tag, block_num, exc)



# ─────────────────────────────────────────────────────────────────────────────
# TASK 2b — WSS BLOCK LİSTENER
# ─────────────────────────────────────────────────────────────────────────────

async def wss_block_listener(ctx: ChainContext) -> None:
    """
    WebSocket Pub/Sub dinleyici — eth_subscribe("newHeads").
    Her yeni blok → hot_scan_once() asyncio.Task olarak ateşlenir.
    Auto-Reconnect: exponential backoff (2s → 60s).
    wss_url boşsa _fallback_hot_poll() devreye girer.
    """
    tag     = ctx.config.tag
    wss_url = ctx.config.wss_url

    if not wss_url:
        logger.warning(
            "[%s-WSS] WSS URL tanımlı değil — polling moduna geçiliyor "
            "(hot_interval=%.1fs)", tag, ctx.config.hot_interval,
        )
        await _fallback_hot_poll(ctx)
        return

    logger.info("[%s-WSS] Block listener başlatılıyor: %s...", tag, wss_url[:50])
    backoff = WSS_INITIAL_BACKOFF

    while True:
        try:
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
                    raise ValueError(f"Geçersiz sub_id yanıtı: {resp}")

                logger.info("[%s-WSS] ✅ Bağlandı. newHeads sub_id=%s...", tag, sub_id[:16])
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

                    logger.debug("[%s-WSS] ⛏  Yeni blok #%d", tag, block_num)

                    if ctx.hot_list:
                        asyncio.create_task(
                            hot_scan_once(ctx, block_num),
                            name=f"hot-{tag}-{block_num}",
                        )

        except asyncio.CancelledError:
            logger.info("[%s-WSS] Task iptal edildi.", tag)
            return

        except Exception as exc:
            logger.warning(
                "[%s-WSS] Bağlantı koptu: %s. %ds sonra yeniden bağlanılıyor...",
                tag, exc, backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WSS_MAX_BACKOFF)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2c — FALLBACK HOT POLL
# ─────────────────────────────────────────────────────────────────────────────

async def _fallback_hot_poll(ctx: ChainContext) -> None:
    """WSS URL tanımlı olmayan zincirler için HTTP polling modu."""
    tag       = ctx.config.tag
    chain_cfg = ctx.config

    logger.info("[%s-HOT] Polling modu aktif (hot_interval=%.1fs).", tag, chain_cfg.hot_interval)

    block_num = 0
    while True:
        try:
            block_num += 1
            if ctx.hot_list:
                await hot_scan_once(ctx, block_num)
        except Exception as exc:
            logger.error("[%s-HOT] Polling döngü hatası: %s", tag, exc)

        await asyncio.sleep(chain_cfg.hot_interval)


# ─────────────────────────────────────────────────────────────────────────────
# CLI + MAIN
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> Optional[str]:
    parser = argparse.ArgumentParser(description="Aave V3 Liquidation Watcher")
    parser.add_argument("chain", nargs="?", choices=["ARB", "BASE", "OP"], default=None)
    return parser.parse_args().chain


async def main() -> None:
    target_chain  = parse_args()
    all_chains    = cfg.load_chains()
    chain_configs = [c for c in all_chains if c.tag == target_chain] if target_chain else all_chains

    if not chain_configs:
        logger.error("Zincir bulunamadı: %s", target_chain)
        sys.exit(1)

    mode_str = target_chain or "MULTI-CHAIN"
    logger.info("=" * 65)
    logger.info("  AAVE V3 LIQUIDATION WATCHER — %s MODU (v12 WSS+JSON)", mode_str)
    logger.info("  Zincirler: %s", [c.tag for c in chain_configs])
    logger.info("  WSS: %s", [c.tag for c in chain_configs if c.wss_url])
    logger.info("  JSON Export: %s", TARGETS_FILE)
    logger.info("  Telegram: %s", "AKTİF" if tg.enabled else "DEVRE DIŞI (.env TELEGRAM_CHAT_ID)")
    logger.info("=" * 65)

    contexts = []
    for chain_cfg_item in chain_configs:
        ctx = await build_context(chain_cfg_item)
        if ctx:
            contexts.append(ctx)

    if not contexts:
        logger.error("Hiçbir zincire bağlanamadı.")
        sys.exit(1)

    async with aiohttp.ClientSession() as session:
        tasks = []

        # [v12] TargetsStore writer — tüm zincirler için tek global worker
        tasks.append(asyncio.create_task(
            targets_store.writer_loop(),
            name="targets-writer",
        ))

        for ctx in contexts:
            tasks.append(asyncio.create_task(
                cold_scan_loop(session, ctx),
                name=f"cold-{ctx.config.tag}",
            ))
            tasks.append(asyncio.create_task(
                wss_block_listener(ctx),
                name=f"wss-{ctx.config.tag}",
            ))

        logger.info("  %d paralel task başlatıldı:", len(tasks))
        for t in tasks:
            logger.info("    - %s", t.get_name())
        logger.info("=" * 65)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results):
            if isinstance(result, Exception):
                logger.error("Task '%s' sonlandı: %s", task.get_name(), result)


if __name__ == "__main__":
    asyncio.run(main())