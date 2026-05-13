---
name: Guerrilla RPC Rotator
overview: Implement a Round-Robin HTTP RPC rotator ("Sarjor Degistirici") that transparently swaps the HTTP provider URL on 429 errors while keeping WSS connections untouched on their golden key. The rotator will be a global singleton module used by all HTTP-consuming components (watcher.py Motor 2, cluster_sniper.py, sniper.py, aave_utils.py).
todos:
  - id: rotator-module
    content: "rpc_rotator.py olustur: RpcRotator sinifi, from_env() factory, call_with_retry() wrapper, _is_rate_limit_429() (aave_utils'ten tasima), log formati"
    status: completed
  - id: env-update
    content: ".env ve config.py guncelle: ALCHEMY_WSS_URL + ALCHEMY_HTTP_URLS parse, load_chains() entegrasyonu"
    status: completed
  - id: aave-utils-integrate
    content: "aave_utils.py: _is_rate_limit_429 import'u degistir, build_context/multicall/reserve_cache fonksiyonlarini rotator ile sar"
    status: completed
  - id: watcher-integrate
    content: "watcher.py: OptiPairEngine icindeki tum HTTP call'lari call_with_retry() ile sar, WSS'e dokunma"
    status: completed
  - id: cluster-sniper-integrate
    content: "cluster_sniper.py: build_w3/motor2_hf_monitor/fetch_user_reserve fonksiyonlarini rotator ile sar"
    status: completed
  - id: sniper-integrate
    content: "sniper.py: Motor 2 HF polling ve refresh_target_state fonksiyonlarini rotator ile sar"
    status: pending
isProject: false
---

# Guerrilla RPC Rotator (Sarjor Degistirici) Plani

## Mimari Karar: Provider-Level URL Swapping

Web3.py'nin `AsyncHTTPProvider` nesnesi, `endpoint_uri` attribute'unu runtime'da degistirmeye izin verir. Bu, en az invaziv yaklasimdir: mevcut `w3` ve contract nesneleri yeniden olusturulmaz, sadece provider'in hedef URL'si degistirilir. Her yeni HTTP istegi guncel URL'e gider.

```mermaid
flowchart TD
    subgraph golden ["ALTIN WSS (Dokunulmaz)"]
        WSS["ALCHEMY_WSS_URL (tek sabit key)"]
        M1_CL["Motor 1: Chainlink WSS"]
        M1_ST["Motor 1: State Watcher"]
        BL["wss_block_listener"]
        WSS --> M1_CL
        WSS --> M1_ST
        WSS --> BL
    end
    subgraph rotator ["RPC ROTATOR (Sarjor)"]
        ROT["RpcRotator Singleton"]
        K1["key1"] --> ROT
        K2["key2"] --> ROT
        K3["key3"] --> ROT
        KN["...key50"] --> ROT
    end
    subgraph http ["HTTP Tuketiciler"]
        MC["Multicall (Motor 2)"]
        OP["OptiPairEngine (Oracle)"]
        FR["fetch_user_reserve"]
        HF["get_current_hf"]
    end
    ROT -->|"current_url"| MC
    ROT -->|"current_url"| OP
    ROT -->|"current_url"| FR
    ROT -->|"current_url"| HF
    MC -->|"429 hatasi"| ROT
    OP -->|"429 hatasi"| ROT
```

## Dosya Degisiklikleri

### 1. `.env` - Yeni Format

Mevcut `ARB_RPC` tek URL kalir (geriye uyumluluk). Yeni `ALCHEMY_HTTP_URLS` alani eklenir:

```
ALCHEMY_WSS_URL="wss://arb-mainnet.g.alchemy.com/v2/ALTIN_KEY"
ALCHEMY_HTTP_URLS="https://arb.../v2/key1,https://arb.../v2/key2,...,https://arb.../v2/key50"
```

`ALCHEMY_HTTP_URLS` bossa, `ARB_RPC` tek URL olarak kullanilir (fallback).

### 2. `rpc_rotator.py` - Yeni Dosya (Cekirdek Modul)

- **`RpcRotator` sinifi** (singleton): HTTP URL listesini ve aktif index'i tutar.
- **`rotate()`**: Index'i bir ilerletir (round-robin), WARNING logu basar.
- **`apply_to(w3)`**: Verilen web3 nesnesinin `w3.provider.endpoint_uri`'sini guncel URL ile gunceller.
- **`call_with_retry(coro_factory, w3, max_retries)`**: Generic async retry wrapper:
  1. `coro_factory()` calistirir (ornek: `lambda: contract.functions.foo().call()`)
  2. Exception yakalanirsa `_is_rate_limit_429()` kontrolu
  3. 429 ise `rotate()` + `apply_to(w3)` + tekrar dene
  4. Tum URL'ler tukendiyse `RPCRateLimited429` firlatir
- **Log formati**: `WARNING [ROTATOR] HTTP 429 Limit asildi! Sarjor degistiriliyor... Yeni Key Index: 3/50`
- **Global instance**: `rotator = RpcRotator.from_env()` modul seviyesinde olusturulur.

Referans: Mevcut 429 algilama fonksiyonu [aave_utils.py](aave_utils.py) satirlar 54-65:

```50:65:c:\Users\Emre Polat\Desktop\Python\files\aave_utils.py
class RPCRateLimited429(Exception):
    """RPC 429 — tarama turu iptal; chunk bolme yapilmaz."""

def _is_rate_limit_429(exc: BaseException) -> bool:
    text = str(exc).lower()
    if "429" in text or "too many requests" in text:
        return True
    if "rate limit" in text or "rate_limit" in text:
        return True
    # ...
```

Bu fonksiyon `rpc_rotator.py`'a tasinacak ve her yerden import edilecek.

### 3. `config.py` - Env Okuma Guncelleme

- `ALCHEMY_WSS_URL` okunur, `ChainConfig.wss_url`'e atanir (mevcut `ARB_WSS` fallback olarak kalir).
- `ALCHEMY_HTTP_URLS` okunur, virgulden parse edilir, `rpc_rotator`'a gecilir.
- `load_chains()` icinde `rpc_url` alani artik rotator'un ilk URL'ini kullanir.

### 4. `aave_utils.py` - Rotator Entegrasyonu

Kritik degisiklikler:
- `_is_rate_limit_429()` ve `RPCRateLimited429` → `rpc_rotator`'dan import edilir (tek kaynak).
- `build_context()` HTTP fallback'inde: `AsyncHTTPProvider` URL'i rotator'dan alinir.
- `_multicall_raw()`: `call_with_retry()` ile sarilir.
- `multicall_account_data()`: Mevcut 429 yakalama mantigi korunur, ek olarak rotation eklenir.
- `load_reserve_cache()`: RPC cagrisi retry ile sarilir.
- `ChainContext`'e opsiyonel `rotator` referansi eklenir.

### 5. `watcher.py` - Rotator Entegrasyonu

- `main()` icinde `rpc_rotator` import edilir, global instance alinir.
- `OptiPairEngine._fetch_oracle_prices()`: `getAssetsPrices().call()` → `call_with_retry()` ile sarilir.
- `OptiPairEngine._enrich_positions()`: Multicall → `call_with_retry()`.
- `OptiPairEngine._detect_emode()`: Tek call → `call_with_retry()`.
- `cold_scan_loop()` ve `hot_scan_once()`: `multicall_account_data()` icindeki 429 yakalama zaten var, rotator otomatik devreye girer.
- WSS baglantilari (`wss_block_listener`, `_fallback_hot_poll`) **DOKUNULMAZ**.

### 6. `cluster_sniper.py` - Rotator Entegrasyonu

- `build_w3()`: URL'yi rotator'dan alir.
- `motor2_hf_monitor()`: Multicall call'u → `call_with_retry()`.
- `fetch_user_reserve()`: Data provider call'u → `call_with_retry()`.
- `init_all_targets()`: Reserve data call'lari → `call_with_retry()`.
- WSS baglantilari (`oracle_hub`, `state_change_watcher`) **DOKUNULMAZ**.

### 7. `sniper.py` - Rotator Entegrasyonu

- `motor2_confirmed_fire()`: `get_current_hf()` icindeki `getUserAccountData().call()` → `call_with_retry()`.
- `build_w3()`: HTTP URL rotator'dan.
- `refresh_target_state()`: `balanceOf().call()` → `call_with_retry()`.
- WSS motorlari (`motor1_chainlink_price_watcher`, `motor1_alchemy_state_watcher`) **DOKUNULMAZ**.

## Kritik Tasarim Kararlari

- **Neden provider.endpoint_uri degistirme?** Web3 contract nesneleri provider'a baglidir. Provider URL'i degistirmek, tum contract nesnelerini gecersiz kilmadan rotasyon yapmanin tek yoludur. Yeni w3/contract olusturmak MEV'de kabul edilemez gecikme yaratir.
- **Neden global singleton?** Birden fazla async task ayni rotator'u kullanmali. Biri 429 aldiginda digerleri de ayni URL'den 429 alir; hepsinin ayni anda yeni URL'e gecmesi gerekir.
- **Thread safety**: asyncio tek thread'dir, GIL altinda `_index` degistirmek atomiktir. Lock gerekmez (mevcut kodda da yok).
- **Tum URL'ler tukenirse?** Son URL'den sonra bas'a doner (round-robin). 429 cooldown'u genellikle 1-5 saniyedir, 50 key ile dongu tamamlanana kadar ilk key yeniden kullanilabilir.
