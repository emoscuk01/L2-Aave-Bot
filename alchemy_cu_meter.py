"""
alchemy_cu_meter.py — Tahmini Alchemy Compute Unit (CU) sayacı
==============================================================

Dashboard erişimi olmadan terminalden RPC yükünü izlemek için.
Sayım, rpc_rotator.call_with_retry üzerinden yapılan başarılı çağrılardan
ve birkaç bilinçli hook'tan (ör. watcher içi get_transaction_count) oluşur.

ÖNEMLİ — ÇİFTE TEYİT / SINIRLAR:
  • Gerçek faturalama ve kalan kota yalnızca Alchemy hesabında kesindir.
  • Varsayılan CU değerleri docs.alchemy.com «Compute Unit Costs» tablosundan
    alınmıştır (örn. eth_call=26, eth_getTransactionCount=20).
  • tryAggregate tek bir JSON-RPC isteği olarak sayılır; iç çağrı başına ek CU için
    ALCHEMY_CU_MULTICALL_PER_INNER kullanın.

ÇEVRE DEĞİŞKENLERİ:
  ALCHEMY_CU_METER=1             0 ile sayaç kapalı
  ALCHEMY_CU_LOG_EVERY_N=50      Her N başarılı kayıtta INFO özeti (0=kapat)
  ALCHEMY_CU_MONTHLY_BUDGET=30000000  Yüzde göstergesi için CU tavanı
  ALCHEMY_CU_ETH_CALL=26
  ALCHEMY_CU_MULTICALL_PER_INNER=0
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_DEFAULT_METHOD_CU: Dict[str, float] = {
    "eth_call": 26.0,
    "eth_blockNumber": 10.0,
    "eth_getTransactionCount": 20.0,
    "eth_chainId": 0.0,
}


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _method_cu_default(rpc_method: str) -> float:
    if rpc_method in _DEFAULT_METHOD_CU:
        base = _DEFAULT_METHOD_CU[rpc_method]
    else:
        base = _env_float("ALCHEMY_CU_DEFAULT_FALLBACK", 26.0)
    if rpc_method == "eth_call":
        return _env_float("ALCHEMY_CU_ETH_CALL", base)
    return base


def cu_try_aggregate(inner_calls: int) -> float:
    base = _env_float("ALCHEMY_CU_ETH_CALL", 26.0)
    per = _env_float("ALCHEMY_CU_MULTICALL_PER_INNER", 0.0)
    n = max(0, int(inner_calls))
    return base + float(n) * per


class _CuMeter:
    __slots__ = (
        "_lock",
        "_enabled",
        "_rpc_success",
        "_est_cu_total",
        "_started_mono",
        "_last_label",
        "_log_every",
        "_budget",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reload_env()
        self._reset_counters()

    def _reload_env(self) -> None:
        self._enabled = os.getenv("ALCHEMY_CU_METER", "1").strip().lower() not in (
            "0",
            "false",
            "no",
            "off",
        )
        self._log_every = int(os.getenv("ALCHEMY_CU_LOG_EVERY_N", "50"))
        self._budget = _env_float("ALCHEMY_CU_MONTHLY_BUDGET", 30_000_000.0)

    def record(
        self,
        *,
        rpc_method: str = "eth_call",
        estimated_cu: Optional[float] = None,
        context_label: str = "",
    ) -> None:
        if not self._enabled:
            return

        cu = float(estimated_cu) if estimated_cu is not None else _method_cu_default(rpc_method)

        with self._lock:
            self._rpc_success += 1
            self._est_cu_total += cu
            self._last_label = context_label or self._last_label
            n = self._rpc_success
            total = self._est_cu_total
            log_every = self._log_every
            budget = self._budget

        if log_every > 0 and n % log_every == 0:
            pct = (total / budget * 100.0) if budget > 0 else 0.0
            logger.info(
                "[CU-METER] rpc_ok=%d est_CU≈%.0f | budget=%.0f CU (~%.3f%% tahmini) | örnek_label=%s",
                n,
                total,
                budget,
                pct,
                (context_label or "?")[:56],
            )

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": self._enabled,
                "rpc_success_count": self._rpc_success,
                "estimated_cu_total": round(self._est_cu_total, 3),
                "budget_cu": self._budget,
                "elapsed_sec": round(time.monotonic() - self._started_mono, 3),
                "last_context_label": self._last_label,
            }

    def _reset_counters(self) -> None:
        self._rpc_success = 0
        self._est_cu_total = 0.0
        self._started_mono = time.monotonic()
        self._last_label = ""


_meter = _CuMeter()


def record_rpc_cu(
    *,
    rpc_method: str = "eth_call",
    estimated_cu: Optional[float] = None,
    context_label: str = "",
) -> None:
    _meter.record(
        rpc_method=rpc_method,
        estimated_cu=estimated_cu,
        context_label=context_label,
    )


def cu_meter_snapshot() -> Dict[str, Any]:
    return _meter.snapshot()


_banner_logged = False


def log_cu_meter_banner_once() -> None:
    global _banner_logged
    if _banner_logged or not _meter._enabled:
        return
    _banner_logged = True
    logger.info(
        "[CU-METER] Aktif — tahmini CU (dashboard yerine). Budget=%.0f CU | "
        "ALCHEMY_CU_ETH_CALL, ALCHEMY_CU_MULTICALL_PER_INNER",
        _meter._budget,
    )
