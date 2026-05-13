"""
rpc_rotator.py — Gerilla Tipi HTTP RPC Şarjör Değiştirici v1
==============================================================

Aave V3 MEV botunun HTTP isteklerini 50+ Alchemy Free Tier API key'i
arasında Round-Robin mantığıyla dağıtan Singleton rotator modülü.

MİMARİ KURALLAR:
  - WSS bağlantılarına ASLA DOKUNMAZ. Sadece HTTP RPC yönetir.
  - 429 / Rate Limit hatasında milisaniye içinde sonraki key'e geçer.
  - Tüm URL'ler tükenirse başa döner (circular). 50 key ile bir tur
    ~5-10 saniye sürer; ilk key'in cooldown'u çoktan bitmiştir.
  - ALCHEMY_HTTP_URLS boşsa ARB_RPC'yi tek mermi olarak kullanır.
  - İsteğe bağlı ALCHEMY_HTTP_URLS_FILE: satır başına tam URL veya yalnızca Alchemy API key;
    key satırları ALCHEMY_HTTP_KEY_PREFIX (varsayılan Arbitrum Alchemy HTTP v2) ile birleştirilir.
    Pompalı tx yayını bu dosyayı kullanmaz — sadece ARB_MULTI_RPC .env virgül listesi.

KULLANIM:
  from rpc_rotator import rotator, call_with_retry, is_rate_limit_429

  # Otomatik retry + rotation
  result = await call_with_retry(
      lambda: contract.functions.foo().call(),
      rotator, w3,
  )
"""

import logging
import os
import re
import time
from typing import Any, Awaitable, Callable, List, Optional, Set

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

from alchemy_cu_meter import record_rpc_cu

# Alchemy HTTP v2: satırda sadece key varsa bu prefix ile birleştirilir (.env: ALCHEMY_HTTP_KEY_PREFIX)
_DEFAULT_ALCHEMY_ARB_HTTP_PREFIX = "https://arb-mainnet.g.alchemy.com/v2/"
_RAW_KEY_LINE_RE = re.compile(r"^[A-Za-z0-9_-]{8,120}$")


def _merge_rpc_url_lists(list_a: List[str], list_b: List[str]) -> List[str]:
    """Sırayı koruyarak tekrarları atar (önce list_a, sonra list_b)."""
    seen: Set[str] = set()
    out: List[str] = []
    for chunk in (list_a, list_b):
        for u in chunk:
            u = u.strip()
            if not u or u in seen:
                continue
            seen.add(u)
            out.append(u)
    return out


def _file_line_to_http_rpc_url(line: str) -> Optional[str]:
    """
    Şarjör dosyası satırı: tam https://... URL veya yalnızca Alchemy API key (v2 path sonrası string).
    """
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    low = s.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return s
    if not _RAW_KEY_LINE_RE.match(s):
        logger.warning(
            "[ROTATOR] ALCHEMY_HTTP_URLS_FILE satırı atlandı (key/URL değil): %r",
            s[:24] + ("..." if len(s) > 24 else ""),
        )
        return None
    prefix = os.getenv("ALCHEMY_HTTP_KEY_PREFIX", _DEFAULT_ALCHEMY_ARB_HTTP_PREFIX).strip()
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    return prefix + s.lstrip("/")


def _urls_from_env_and_optional_file() -> List[str]:
    raw = os.getenv("ALCHEMY_HTTP_URLS", "").strip()
    comma_urls: List[str] = [u.strip() for u in raw.split(",") if u.strip()] if raw else []

    file_urls: List[str] = []
    path = os.getenv("ALCHEMY_HTTP_URLS_FILE", "").strip()
    if path:
        expanded = os.path.expandvars(os.path.expanduser(path))
        if os.path.isfile(expanded):
            with open(expanded, encoding="utf-8") as fh:
                for line in fh:
                    u = _file_line_to_http_rpc_url(line)
                    if u:
                        file_urls.append(u)
            logger.info(
                "[ROTATOR] %d HTTP endpoint ALCHEMY_HTTP_URLS_FILE içinden (%s)",
                len(file_urls),
                expanded,
            )
        else:
            logger.warning(
                "[ROTATOR] ALCHEMY_HTTP_URLS_FILE yolu dosya değil veya erişilemiyor: %s",
                expanded,
            )

    # Şarjör dosyası (.txt key'leri) önce: .env'deki ALCHEMY_HTTP_URLS sıklıkla
    # tükenmiş anahtar içerir; aksi halde ilk deneme hep 429 olur.
    return _merge_rpc_url_lists(file_urls, comma_urls)


# ─────────────────────────────────────────────────────────────────────────────
# 429 / RATE LIMIT TESPİTİ (Tek Kaynak — tüm modüller buradan import eder)
# ─────────────────────────────────────────────────────────────────────────────

class RPCRateLimited429(Exception):
    """Tüm RPC URL'leri tükendi veya döngü limiti aşıldı."""


def is_rate_limit_429(exc: BaseException) -> bool:
    """
    Bir exception'ın HTTP 429 / Rate Limit hatası olup olmadığını tespit eder.

    Alchemy, Infura, QuickNode gibi sağlayıcıların döndüğü farklı hata
    formatlarını kapsar:
      - HTTP 429 Too Many Requests
      - JSON-RPC error code -32005 / -32016 / 429
      - "rate limit" / "rate_limit" metin eşleşmesi
    """
    text = str(exc).lower()
    if "429" in text or "too many requests" in text:
        return True
    if "rate limit" in text or "rate_limit" in text:
        return True
    if "exceeded" in text and "capacity" in text:
        return True
    err = getattr(exc, "args", None)
    if err and isinstance(err[0], dict):
        code = err[0].get("code")
        if code in (-32005, 429, -32016):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# RPC ROTATOR — ŞARJÖR MEKANİZMASI
# ─────────────────────────────────────────────────────────────────────────────

class RpcRotator:
    """
    HTTP RPC URL'lerini Round-Robin mantığıyla yöneten Singleton sınıf.

    Özellikler:
      - URL listesi ve aktif index tutulur.
      - rotate() çağrıldığında bir sonraki URL'e geçilir.
      - apply_to(w3) ile mevcut AsyncWeb3 nesnesinin provider'ı güncellenir.
      - Tüm URL'ler tükenince başa döner (circular buffer).

    Provider Reset Stratejisi:
      endpoint_uri değiştirildiğinde, eski URL'ye açık kalan HTTP
      connection'lar sorun yaratabilir. _reset_provider_cache() metodu
      provider'ın dahili session cache'ini temizleyerek yeni URL'e
      temiz bağlantı açılmasını garantiler.
    """

    _instance: Optional["RpcRotator"] = None

    def __init__(self, urls: List[str]) -> None:
        if not urls:
            raise ValueError(
                "RpcRotator: En az bir HTTP URL gerekli! "
                ".env'de ALCHEMY_HTTP_URLS veya ARB_RPC tanımlayın."
            )
        self._urls: List[str] = urls
        self._index: int = 0
        self._total: int = len(urls)
        self._rotate_count: int = 0
        self._last_rotate_ts: float = 0.0
        logger.info(
            "[ROTATOR] Şarjör hazır | %d HTTP RPC yüklendi | "
            "Aktif: ...%s",
            self._total,
            self._mask_url(urls[0]),
        )

    # ── Factory ──────────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> "RpcRotator":
        """
        .env'den HTTP URL listesini yükler.

        Öncelik sırası:
          1. ALCHEMY_HTTP_URLS (virgül) + ALCHEMY_HTTP_URLS_FILE (satır: tam URL veya Alchemy key)
          2. ARB_RPC (tek URL — fallback, tek mermili silah modu)
          3. Hiçbiri yoksa → hata

        Singleton: İlk çağrıda oluşturulur, sonraki çağrılarda aynı
        instance döner.
        """
        if cls._instance is not None:
            return cls._instance

        urls = _urls_from_env_and_optional_file()

        if not urls:
            fallback = os.getenv("ARB_RPC", "").strip()
            if fallback:
                logger.warning(
                    "[ROTATOR] ALCHEMY_HTTP_URLS boş — ARB_RPC tek mermi "
                    "olarak yüklendi. Tek mermili silah modu aktif."
                )
                urls = [fallback]

        if not urls:
            raise ValueError(
                "[ROTATOR] ÖLÜMCÜL: Ne ALCHEMY_HTTP_URLS ne de ARB_RPC "
                "tanımlı! .env dosyasını kontrol edin."
            )

        cls._instance = cls(urls)
        return cls._instance

    @classmethod
    def reset_singleton(cls) -> None:
        """Test amaçlı: singleton'ı sıfırlar."""
        cls._instance = None

    # ── Temel İşlemler ───────────────────────────────────────────────────────

    @property
    def current_url(self) -> str:
        return self._urls[self._index]

    @property
    def total_urls(self) -> int:
        return self._total

    @property
    def current_index(self) -> int:
        return self._index

    @property
    def all_urls(self) -> List[str]:
        """Şarjördeki tüm HTTP endpoint'lerin kopyası (sıra korunur)."""
        return list(self._urls)

    def rotate_matching_host(self, host: Optional[str], reason: str = "429") -> str:
        """
        429 sonrası yalnızca verilen hostname ile aynı ağdaki bir sonraki URL'e geçer.

        Multi-chain watcher'da Base provider'ı Arbitrum URL'sine çevirmemek için
        call_with_retry bu metodu kullanır. host None ise klasik rotate() davranır.
        """
        if not host:
            return self.rotate(reason)

        from urllib.parse import urlparse

        matches = [i for i, u in enumerate(self._urls) if urlparse(u).hostname == host]
        if not matches:
            raise RPCRateLimited429(
                f"Şarjörde '{host}' host'lu HTTP URL yok — bu ağ için key dosyasına "
                "tam https://... URL'leri ekleyin veya ALCHEMY_HTTP_URLS'e yazın."
            )
        if len(matches) == 1:
            raise RPCRateLimited429(
                f"Şarjörde '{host}' için yalnızca tek URL — 429 sonrası aynı ağdan ikinci key gerekir."
            )

        if self._index not in matches:
            self._index = matches[0]
        else:
            pos = matches.index(self._index)
            next_pos = (pos + 1) % len(matches)
            self._index = matches[next_pos]
        self._rotate_count += 1
        new_url = self._urls[self._index]

        now = time.monotonic()
        if now - self._last_rotate_ts > 0.5:
            logger.warning(
                "[ROTATOR] HTTP %s! Şarjör (aynı host) ... "
                "Yeni Key Index: %d/%d | Toplam rotasyon: %d | host=%s",
                reason,
                self._index + 1,
                self._total,
                self._rotate_count,
                host,
            )
        self._last_rotate_ts = now
        return new_url

    def rotate(self, reason: str = "429") -> str:
        """
        Bir sonraki URL'e geçer. Terminale WARNING basar.
        Spam önleme: aynı saniye içinde ardışık rotate'lerde log basılmaz.
        """
        old_index = self._index
        self._index = (self._index + 1) % self._total
        self._rotate_count += 1
        new_url = self._urls[self._index]

        now = time.monotonic()
        if now - self._last_rotate_ts > 0.5:
            logger.warning(
                "[ROTATOR] HTTP %s! Şarjör değiştiriliyor... "
                "Yeni Key Index: %d/%d | Toplam rotasyon: %d",
                reason,
                self._index + 1,
                self._total,
                self._rotate_count,
            )
        self._last_rotate_ts = now
        return new_url

    def apply_to(self, w3) -> None:
        """
        Verilen AsyncWeb3 nesnesinin HTTP provider URL'ini güncel URL ile
        değiştirir ve provider cache'ini resetler.

        Bu fonksiyon 429 sonrası çağrılır. Eski URL'ye ait connection
        pool'u temizlenmezse yeni URL'e rağmen eski bağlantı kullanılabilir.
        """
        provider = getattr(w3, "provider", None)
        if provider is None:
            return

        provider.endpoint_uri = self.current_url
        self._reset_provider_cache(provider)

    def mark_all_urls(self) -> List[str]:
        """Tüm URL'lerin maskeli halini döndürür (debug amaçlı)."""
        return [self._mask_url(u) for u in self._urls]

    def sync_index_to_url(self, url: str) -> None:
        """Bağlanılan URL'nin şarjör içindeki indeksine hizala (429 rotasyonu için)."""
        try:
            self._index = self._urls.index(url.strip())
        except ValueError:
            pass

    # ── Dahili Yardımcılar ───────────────────────────────────────────────────

    @staticmethod
    def _reset_provider_cache(provider) -> None:
        """
        Provider'ın dahili aiohttp session/cache'ini resetler.

        web3.py v6/v7 AsyncHTTPProvider, _request_session_manager veya
        _async_session gibi dahili cache tutar. URL değiştikten sonra
        eski bağlantıların kullanılmasını önlemek için bunları temizleriz.

        Mevcut değilse sessizce geçer — farklı web3 versiyonlarıyla uyumlu.
        """
        # web3 v7.x: _request_data cache
        if hasattr(provider, "_request_data"):
            try:
                provider._request_data = {}
            except Exception:
                pass

        # web3 v6.x / v7.x: session cache attribute'ları
        for attr in ("_async_session", "_session"):
            if hasattr(provider, attr):
                try:
                    setattr(provider, attr, None)
                except Exception:
                    pass

    @staticmethod
    def _mask_url(url: str) -> str:
        """URL'nin son 6 karakterini gösterir, geri kalanını maskeler."""
        if len(url) <= 20:
            return url
        return f"...{url[-12:]}"


# ─────────────────────────────────────────────────────────────────────────────
# CALL WITH RETRY — Otomatik 429 Retry + Rotation Wrapper
# ─────────────────────────────────────────────────────────────────────────────

def _w3_http_hostname(w3) -> Optional[str]:
    """AsyncHTTPProvider endpoint host'u (multi-chain rotasyon filtresi)."""
    try:
        from urllib.parse import urlparse

        prov = getattr(w3, "provider", None)
        uri = getattr(prov, "endpoint_uri", None) if prov else None
        return urlparse(uri).hostname if uri else None
    except Exception:
        return None


async def call_with_retry(
    coro_factory: Callable[[], Awaitable[Any]],
    rot: RpcRotator,
    w3,
    max_retries: int = 0,
    context_label: str = "",
    *,
    rpc_method: str = "eth_call",
    estimated_cu: Optional[float] = None,
) -> Any:
    """
    Bir async çağrıyı 429 hatalarında otomatik olarak RPC rotasyonu ile
    yeniden dener.

    Parametreler:
      coro_factory: Her denemede YENİ bir coroutine üreten callable.
                    Örnek: lambda: contract.functions.foo().call()
                    NOT: Aynı coroutine iki kez await edilemez, bu yüzden
                    factory pattern kullanılır.
      rot:          RpcRotator instance
      w3:           AsyncWeb3 nesnesi (provider URL'i güncellenecek)
      max_retries:  Maksimum deneme sayısı. 0 = URL sayısı kadar dene.
      context_label: Log'larda gösterilecek bağlam etiketi (ör. "MULTICALL")

    Dönüş:
      Başarılı çağrının sonucu.

    Hatalar:
      RPCRateLimited429: Tüm URL'ler tükendi, hiçbiri yanıt vermedi.
      Diğer exception'lar: 429 olmayan hatalar olduğu gibi yükseltilir.
    """
    if max_retries <= 0:
        max_retries = rot.total_urls

    label = f"[{context_label}] " if context_label else ""
    rpc_host = _w3_http_hostname(w3)

    for attempt in range(1, max_retries + 1):
        try:
            result = await coro_factory()
            record_rpc_cu(
                rpc_method=rpc_method,
                estimated_cu=estimated_cu,
                context_label=context_label,
            )
            return result
        except Exception as exc:
            if is_rate_limit_429(exc):
                new_url = rot.rotate_matching_host(rpc_host, reason="429")
                rot.apply_to(w3)
                rpc_host = _w3_http_hostname(w3)

                if attempt < max_retries:
                    logger.debug(
                        "%s429 yakalandı (deneme %d/%d) → yeni endpoint: ...%s",
                        label, attempt, max_retries, new_url[-12:],
                    )
                    continue

                logger.error(
                    "%sTüm RPC URL'leri tükendi (%d deneme). Son hata: %s",
                    label, max_retries, exc,
                )
                raise RPCRateLimited429(
                    f"Tüm {max_retries} RPC denemesi başarısız: {exc}"
                ) from exc

            raise

    raise RPCRateLimited429(f"Max retry ({max_retries}) aşıldı")


# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL SINGLETON INSTANCE
# ─────────────────────────────────────────────────────────────────────────────

def get_rotator() -> RpcRotator:
    """
    Global RpcRotator singleton'ını döndürür.
    İlk çağrıda .env'den yüklenir.
    """
    return RpcRotator.from_env()
