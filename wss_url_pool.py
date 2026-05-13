"""
wss_url_pool.py — cluster_sniper WSS: şarjör key'leri ile çoklu wss:// endpoint

HTTP RpcRotator (arb_http_urls.txt) ile aynı host'taki Alchemy key'lerinden
wss://... URL listesi üretir. Oracle hub / block listener / state watcher
bağlantı başına round-robin atar; 429/403 sonrası indeks ilerletilir.

NOT: RpcRotator singleton önce get_rotator() ile yüklenmiş olmalı.
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def build_wss_urls_for_cluster(
    chain_cfg,
    rot,
    extra_wss: Optional[List[str]] = None,
) -> List[str]:
    """
    WSS URL havuzu oluşturur.  Kaynak sırası:
      1. rot.all_urls içinden chain_cfg.wss_url ile aynı hostname'e sahip HTTP → wss dönüşümü
      2. extra_wss listesi (QuikNode, Chainstack vb. farklı host WSS'ler)
      3. chain_cfg.wss_url (her zaman sonda — tükenmiş key güvencesi)

    Hostname eşleşmesi olmayan rotator URL'leri atlanır ama extra_wss ile
    açıkça verilen farklı-host endpoint'ler havuza dahil edilir.
    """
    base = (chain_cfg.wss_url or "").strip()
    if not base:
        return []
    want_host = urlparse(base).hostname
    if not want_host:
        return [base]

    out: List[str] = []
    seen: set[str] = set()

    # 1) HTTP şarjörden aynı host WSS türetimi
    matched_from_rotator = 0
    for u in rot.all_urls:
        if urlparse(u).hostname != want_host:
            continue
        ws = u.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
        if ws not in seen:
            seen.add(ws)
            out.append(ws)
            matched_from_rotator += 1

    # 2) Farklı-host ek WSS endpoint'leri (QuikNode, Chainstack vb.)
    extra_added = 0
    for raw in (extra_wss or []):
        u = (raw or "").strip()
        if not u:
            continue
        if not u.startswith("wss://") and not u.startswith("ws://"):
            u = u.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
        if u not in seen:
            seen.add(u)
            out.append(u)
            extra_added += 1

    # 3) Base (env WSS) her zaman sonda
    if base not in seen:
        out.append(base)
    else:
        out = [u for u in out if u != base] + [base]

    logger.debug(
        "[WSS-POOL-BUILD] host=%s | rotator_match=%d | extra=%d | toplam=%d",
        want_host, matched_from_rotator, extra_added, len(out),
    )
    return out if out else [base]


def build_same_host_http_probe_urls(primary_rpc: str, rot, extra: Optional[List[str]] = None) -> List[str]:
    """
    rpc_heartbeat_task için: primary_rpc ile aynı host'taki şarjör HTTP URL'leri.
    İlk sırada sağlam key'ler, primary genelde sonda.
    """
    primary = (primary_rpc or "").strip()
    if not primary:
        return list(rot.all_urls)
    want_host = urlparse(primary).hostname
    if not want_host:
        return [primary]

    out: List[str] = []
    seen: set[str] = set()
    for u in rot.all_urls:
        if urlparse(u).hostname == want_host and u not in seen:
            seen.add(u)
            out.append(u)
    if primary not in seen:
        out.append(primary)
    else:
        out = [u for u in out if u != primary] + [primary]

    if extra:
        for u in extra:
            u = (u or "").strip()
            if not u or u.startswith("wss://") or u.startswith("ws://"):
                continue
            if urlparse(u).hostname == want_host and u not in seen:
                seen.add(u)
                out.append(u)
    return out if out else [primary]


def wss_transport_limited(exc: BaseException) -> bool:
    """429 / 403 / benzeri WSS el sıkışma redleri."""
    code = getattr(exc, "status_code", None)
    if code in (403, 429, 503):
        return True
    t = str(exc).lower()
    return "429" in t or "403" in t or "503" in t or "rate limit" in t


def build_wss_urls_for_watcher(chain_cfg, rot) -> List[str]:
    """
    watcher.py için WSS URL havuzu.
    chain_cfg.wss_url ile aynı hostname'e sahip rotator URL'lerinden wss türetir.
    """
    return build_wss_urls_for_cluster(chain_cfg, rot)


def warn_if_single_endpoint(pool_size: int, label: str) -> None:
    """Pool tek endpoint ile başlıyorsa CRITICAL uyarı bas."""
    if pool_size <= 1:
        logger.critical(
            "[WSS-POOL] ⚠ CRITICAL: %s yalnızca %d WSS endpoint ile başlıyor! "
            "429/rate-limit durumunda rotasyon YAPILMAZ. "
            "ALCHEMY_HTTP_URLS_FILE'a ek key ekleyin veya extra_wss kullanın.",
            label, pool_size,
        )
    else:
        logger.info(
            "[WSS-POOL] %s: %d WSS endpoint hazır — round-robin aktif.",
            label, pool_size,
        )


class WssUrlPool:
    """Paralel WSS task'ları için round-robin + 429 sonrası atlama."""

    __slots__ = ("_urls", "_i", "_lock")

    def __init__(self, urls: List[str]) -> None:
        self._urls = [u.strip() for u in urls if u and u.strip()]
        if not self._urls:
            raise ValueError("WssUrlPool: en az bir WSS URL gerekli")
        self._i = 0
        self._lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return len(self._urls)

    async def acquire(self) -> str:
        async with self._lock:
            u = self._urls[self._i % len(self._urls)]
            self._i += 1
            return u

    async def on_transport_limit(self) -> None:
        """429/403 sonrası bir sonraki bağlantıda farklı key'e kay."""
        async with self._lock:
            self._i += 1
            logger.warning(
                "[WSS-POOL] 429/403 → sonraki endpoint'e kayıldı (idx=%d/%d)",
                self._i % len(self._urls), len(self._urls),
            )
