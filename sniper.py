"""
sniper.py — Arbitrum Aave V3 Dual-Engine Keskin Nişancı v1
===========================================================

Bağımsız modül — watcher.py'den tamamen ayrıdır.
config.py, aave_utils.py ve telegram_utils.py'yi ortak kullanır.

Kullanım:
    python sniper.py                           # .env'den otomatik hedef yükle
    python sniper.py --target 0xABC...         # Manuel hedef adresi
    python sniper.py --target 0xABC... --dry   # Kuru çalışma (tx atmaz)

.env'e eklenecek değişkenler:
    SNIPER_TARGET=0x...          Hedef cüzdan adresi
    SNIPER_PRIVATE_KEY=0x...     İşlem imzalama için private key
    FAST_RPC=https://...         Motor 2 için premium RPC (QuickNode vb.)
                                 Boşsa Motor 1 ile aynı RPC kullanılır.
    CHAINLINK_WSS=wss://...      Chainlink fiyat feed WSS (opsiyonel override)
    SNIPER_MAX_RETRIES=3         İşlem tekrar sayısı (varsayılan 3)
    SNIPER_GAS_LIMIT=800000      Gas limiti (varsayılan 800000)
    SNIPER_GAS_MULTIPLIER=1.2    Gas price çarpanı (varsayılan 1.2x)

──────────────────────────────────────────────────────────────────────────────
DUAL-ENGINE MİMARİSİ
──────────────────────────────────────────────────────────────────────────────

                    ┌─────────────────────────────────┐
                    │       SniperTarget (Hedef)       │
                    │  collateral_token | debt_token   │
                    │  collateral_raw   | debt_raw      │
                    │  liquidation_price (USD/WETH)    │
                    └──────────────┬──────────────────┘
                                   │
              ┌────────────────────┼───────────────────────┐
              │                                            │
    ┌─────────▼──────────┐                    ┌───────────▼──────────┐
    │   MOTOR 1          │                    │   MOTOR 2            │
    │   KÖR ATIŞ         │                    │   KESİN ATIŞ         │
    │                    │                    │                      │
    │ Chainlink WSS       │                    │ Fast RPC polling      │
    │ fiyat ≤ liq_price  │                    │ HF < 1.00 görülünce  │
    │        +           │                    │ tx gönder            │
    │ Alchemy WSS         │                    │                      │
    │ balance change     │                    │ Motor 1 daha önce    │
    │ → liq_price update │                    │ attıysa atlar        │
    └─────────┬──────────┘                    └───────────┬──────────┘
              │                                            │
              └──────────────────┬─────────────────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │   fire_sniper_tx()        │
                    │   Web3.py imzala+gönder   │
                    │   Nonce çakışma koruması  │
                    │   Gas bump retry          │
                    └──────────────────────────┘

NEDEN BU MİMARİ?
  Arbitrum'da bir tx'in maliyeti ~0.05$, revert'i ~0.02$.
  "Kurşun bedava" prensibi: iki motor aynı anda ateşler.
  Biri revert yese bile öteki hedefe isabet eder.
  Net beklenti: 200$ kâr - (0.05 + 0.05) maliyet = +199.90$.

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
from typing import Optional, Tuple

import aiohttp
import websockets
from dotenv import load_dotenv
from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.middleware import ExtraDataToPOAMiddleware

from config import (
    LIQUIDATION_BONUS_MAP, DEFAULT_BONUS,
    FLASH_LOAN_FEE, DEX_SLIPPAGE,
    WAD,
)
from telegram_utils import TelegramNotifier

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
tg = TelegramNotifier(chat_id=os.getenv("TELEGRAM_CHAT_ID", ""))

# ── Sabitler ──────────────────────────────────────────────────────────────────

# Chainlink Arbitrum WETH/USD fiyat feed'i.
# Adres: resmi Chainlink docs'tan alındı — değiştirme.
# AggregatorV3Interface sadece latestRoundData() kullanıyoruz.
CHAINLINK_WETH_USD_ARB = "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"

# Chainlink WSS: Alchemy'nin eth_subscribe("logs") ile Chainlink AnswerUpdated
# event'ini dinleyeceğiz. Bu, her fiyat güncellemesinde tetiklenir.
# AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)
CHAINLINK_ANSWER_UPDATED_TOPIC = (
    "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
)

# Aave V3 Pool adresi (Arbitrum)
AAVE_POOL_ARB = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

# liquidationCall() ABI — sadece ihtiyacımız olan fonksiyon
AAVE_LIQUIDATION_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "collateralAsset", "type": "address"},
            {"internalType": "address", "name": "debtAsset",       "type": "address"},
            {"internalType": "address", "name": "user",            "type": "address"},
            {"internalType": "uint256", "name": "debtToCover",     "type": "uint256"},
            {"internalType": "bool",    "name": "receiveAToken",   "type": "bool"},
        ],
        "name": "liquidationCall",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Chainlink AggregatorV3 ABI — sadece latestRoundData
CHAINLINK_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80",  "name": "roundId",         "type": "uint80"},
            {"internalType": "int256",  "name": "answer",          "type": "int256"},
            {"internalType": "uint256", "name": "startedAt",       "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt",       "type": "uint256"},
            {"internalType": "uint80",  "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

# ERC-20 balanceOf ABI — cüzdan state change tespiti için
ERC20_BALANCE_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# WSS sabitleri (Alchemy idle-disconnect ~60s)
WSS_PING_INTERVAL   = 20
WSS_PING_TIMEOUT    = 10
WSS_CLOSE_TIMEOUT   = 5
WSS_MAX_MSG_SIZE    = 2**20
WSS_INITIAL_BACKOFF = 2
WSS_MAX_BACKOFF     = 30   # Sniper için daha agresif reconnect

# Chainlink fiyat decimals = 8 (sabit standart)
CHAINLINK_DECIMALS = 10 ** 8


# ─────────────────────────────────────────────────────────────────────────────
# VERİ YAPILARI
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SniperTarget:
    """
    Tek bir hedef cüzdanın tam anatomisi.

    Bu yapı hem Kör Atış (Motor 1) hem Kesin Atış (Motor 2) tarafından
    okunur. Motor 1 sadece liquidation_price ve on_chain değerlerle çalışır;
    Motor 2 ek olarak HF'yi RPC'den doğrular.

    Neden ayrı bir dataclass?
      watcher.py'nin TargetInfo'sundan bağımsız kalmak için.
      Sniper tek bir hedefe odaklanır; genel tarayıcı yapısına gerek yok.
    """
    # Hedef cüzdan
    address:              str

    # Teminat (collateral)
    collateral_token:     str   # Sembol, ör. "WETH"
    collateral_address:   str   # ERC-20 adres
    collateral_decimals:  int   # 18
    collateral_raw:       int   # Wei cinsinden mevcut teminat miktarı

    # Borç (debt)
    debt_token:           str   # Sembol, ör. "USDC"
    debt_address:         str   # ERC-20 adres
    debt_decimals:        int   # 6
    debt_raw:             int   # Wei cinsinden mevcut borç miktarı

    # Hesaplanan tasfiye fiyatı (Motor 1'in tetik eşiği)
    # = Fiyat bu seviyeye düşerse HF ≈ 1.00 olur
    liquidation_price:    float = 0.0   # USD/birim (ör. USD/WETH)

    # Kâr tahmini (son hesaplanan)
    estimated_profit_usd: float = 0.0

    # Likidite bonusu
    bonus:                float = DEFAULT_BONUS

    # close_factor: Aave V3 kuralı — HF > 0.95 → %50, HF < 0.95 → %100
    close_factor:         float = 0.5


@dataclass
class SniperState:
    """
    İki motorun paylaştığı çalışma zamanı durumu.

    fired: True olduğunda her iki motor da tx atmaktan kaçınır.
    Neden shared state?
      Motor 1 ateş ettiyse Motor 2 "ben de atayım" dememeli.
      asyncio.Event ile senkronize edilir — Lock gerekmez (GIL korumalı set).
    """
    target:          SniperTarget
    fired:           asyncio.Event = field(default_factory=asyncio.Event)
    fire_count:      int           = 0     # Kaç tx gönderildi
    last_price:      float         = 0.0   # Son Chainlink fiyatı
    last_hf:         float         = 999.0 # Son bilinen HF
    last_balance_ts: float         = 0.0   # Son balance check timestamp
    dry_run:         bool          = False  # True → tx imzalamaz


# ─────────────────────────────────────────────────────────────────────────────
# MATEMATİK: LİKİDASYON FİYATI HESABI
# ─────────────────────────────────────────────────────────────────────────────

def calculate_liquidation_price(
    collateral_amount:   float,   # İnsan birimi (ör. 10.0 WETH)
    collateral_lt:       float,   # Liquidation Threshold (ör. 0.825 = %82.5)
    debt_amount_usd:     float,   # Toplam borç USD değeri (ör. 8000.0)
) -> float:
    """
    Hedef cüzdanın HF'sinin tam olarak 1.00'a düştüğü teminat fiyatını hesapla.

    Aave V3 Health Factor formülü:
        HF = (collateral_amount × price × LT) / debt_amount_usd

    HF = 1.00 koşulunu price için çözelim:
        1.00 = (collateral_amount × price × LT) / debt_amount_usd
        price = debt_amount_usd / (collateral_amount × LT)

    Neden bu formülü kullanıyoruz?
      Motor 1 RPC'ye sormadan, sadece anlık Chainlink fiyatıyla tetik kararı verir.
      Eğer WETH fiyatı bu eşiğe düşerse, on-chain HF'nin 1.00'a düştüğünü
      matematiksel kesinlikle biliriz — hiçbir gecikme yok.

    Parametreler:
      collateral_amount: teminat miktarı (insan birimi, wei değil)
      collateral_lt:     Aave'nin bu token için Liquidation Threshold değeri
                         WETH/ARB için 0.825 (docs: aave.com/risk/ethereum-v3)
      debt_amount_usd:   toplam borç USD (aToken değil, gerçek borç)

    Dönüş:
      float: USD cinsinden tasfiye fiyatı (ör. 2345.67 USD/WETH)

    Örnek:
      10 WETH teminat, LT=0.825, 8000 USDC borç
      price = 8000 / (10 × 0.825) = 8000 / 8.25 = 969.70 USD/WETH
      Yani WETH 969.70$'a düşerse HF = 1.00 olur ve tasfiye açılır.

    Güvenlik marjı:
      Pratikte TRIGGER_MARGIN (örn %0.5) kadar yukarıdan tetikliyoruz.
      Zincir gecikmesi ve gas süresi gözetilerek erken ateş ederiz.
    """
    if collateral_amount <= 0 or collateral_lt <= 0 or debt_amount_usd <= 0:
        raise ValueError(
            f"Geçersiz parametre: collateral={collateral_amount}, "
            f"lt={collateral_lt}, debt={debt_amount_usd}"
        )

    liq_price = debt_amount_usd / (collateral_amount * collateral_lt)
    return liq_price


def calculate_estimated_profit(
    debt_amount_usd: float,
    bonus:           float,
    close_factor:    float,
    gas_fee_usd:     float = 0.05,  # Arbitrum ~$0.05
) -> float:
    """
    Tasfiyeden beklenen net kâr.

    Formül (watcher.py/aave_utils.py ile tutarlı):
      gross  = (debt × close_factor) × bonus
      flash  = (debt × close_factor) × FLASH_LOAN_FEE
      slip   = (debt × close_factor) × DEX_SLIPPAGE
      net    = gross − flash − slip − gas

    Neden close_factor ile çarpıyoruz?
      Aave tek seferde borçun %50'sine (veya %100'üne) dokunmamıza izin verir.
      Kâr hesabı bu gerçek miktara göre yapılmalı.
    """
    covered   = debt_amount_usd * close_factor
    gross     = covered * bonus
    flash_fee = covered * FLASH_LOAN_FEE
    slippage  = covered * DEX_SLIPPAGE
    net       = gross - flash_fee - slippage - gas_fee_usd
    return net


# ─────────────────────────────────────────────────────────────────────────────
# WEB3 BAĞLANTISI
# ─────────────────────────────────────────────────────────────────────────────

async def build_w3(rpc_url: str) -> AsyncWeb3:
    """
    AsyncWeb3 nesnesi oluşturur.

    Arbitrum PoA zinciri olduğundan ExtraDataToPOAMiddleware eklenir.
    Bu olmadan bazı blok alanları decode edilemez.
    """
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    # Arbitrum'un ExtraData alanı standart 32 byte'ı aşar — middleware şart
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    if not await w3.is_connected():
        raise ConnectionError(f"RPC bağlantısı başarısız: {rpc_url}")

    chain_id = await w3.eth.chain_id
    logger.info("Web3 bağlandı | Chain ID: %d | RPC: %s...", chain_id, rpc_url[:50])
    return w3


# ─────────────────────────────────────────────────────────────────────────────
# CHAINLINK: ANLK FİYAT OKUMA
# ─────────────────────────────────────────────────────────────────────────────

async def get_chainlink_price(w3: AsyncWeb3) -> float:
    """
    Chainlink WETH/USD fiyat feed'inden anlık fiyatı çeker.

    latestRoundData() → answer (int256, 8 decimals)
    Dönüş: float (USD, 2 ondalık)

    Neden Chainlink?
      Aave'nin kendi oracle'ı Chainlink tabanlıdır. Dolayısıyla
      on-chain HF hesabında kullanılan fiyat, bizim okuyacağımız
      fiyatla birebir örtüşür — sapma riski sıfır.
    """
    feed = w3.eth.contract(
        address=AsyncWeb3.to_checksum_address(CHAINLINK_WETH_USD_ARB),
        abi=CHAINLINK_ABI,
    )
    try:
        _round_id, answer, _started, _updated, _answered = (
            await feed.functions.latestRoundData().call()
        )
        return answer / CHAINLINK_DECIMALS
    except Exception as exc:
        logger.warning("Chainlink fiyat okunamadı: %s", exc)
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# HEDEF DURUMU: CÜZDAN ON-CHAIN GÜNCELLEMESİ
# ─────────────────────────────────────────────────────────────────────────────

async def refresh_target_state(
    w3:     AsyncWeb3,
    target: SniperTarget,
) -> None:
    """
    Hedefin teminat ve borç miktarlarını on-chain'den günceller.
    Liquidation price yeniden hesaplanır.

    Bu fonksiyon Motor 1'in "Alchemy state change" kolu tarafından çağrılır:
    Hedef cüzdan herhangi bir tx yaparsa (borç ödeme, teminat ekleme vb.)
    balanceOf değişir → biz de tasfiye fiyatını yeniden hesaplarız.

    Neden collateral_lt sabit?
      LT, Aave governance kararıyla değişir (aylar sürer).
      Operasyonel süre boyunca sabit kabul etmek güvenlidir.
      Değişirse .env'den manuel güncellenir.
    """
    try:
        coll_contract = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(target.collateral_address),
            abi=ERC20_BALANCE_ABI,
        )
        debt_contract = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(target.debt_address),
            abi=ERC20_BALANCE_ABI,
        )

        # aToken bakiyesi = teminat (Aave V3'te aToken 1:1 underlying'e eşit)
        new_coll_raw = await coll_contract.functions.balanceOf(
            AsyncWeb3.to_checksum_address(target.address)
        ).call()
        # variableDebtToken bakiyesi = borç
        new_debt_raw = await debt_contract.functions.balanceOf(
            AsyncWeb3.to_checksum_address(target.address)
        ).call()

        old_coll = target.collateral_raw
        old_debt = target.debt_raw

        target.collateral_raw = new_coll_raw
        target.debt_raw       = new_debt_raw

        logger.info(
            "[STATE] Teminat: %s → %s | Borç: %s → %s",
            old_coll, new_coll_raw, old_debt, new_debt_raw,
        )

    except Exception as exc:
        logger.error("[STATE] Güncelleme hatası: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# HF DOĞRULAMA (Motor 2 için)
# ─────────────────────────────────────────────────────────────────────────────

async def get_current_hf(w3: AsyncWeb3, target_address: str) -> float:
    """
    Aave Pool'dan hedef cüzdanın anlık HF'sini çeker.

    getUserAccountData() → healthFactor (uint256, 18 decimals)

    Motor 2 bu fonksiyonu kullanır. Motor 1'in aksine burada
    gerçek on-chain değeri okuyoruz — matematik değil, kesin bilgi.

    Neden Motor 2'de ayrıca okuyoruz?
      Motor 1 matematiksel tahmin yapar (Chainlink fiyat → HF).
      Motor 2 bunu blockchain'den doğrular.
      İki motorun farklı "bilgi kaynağı" kullanması, birinin
      yanlış pozisyon açmasını engeller (çift güvence).
    """
    pool = w3.eth.contract(
        address=AsyncWeb3.to_checksum_address(AAVE_POOL_ARB),
        abi=[{
            "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
            "name": "getUserAccountData",
            "outputs": [
                {"internalType": "uint256", "name": "totalCollateralBase",  "type": "uint256"},
                {"internalType": "uint256", "name": "totalDebtBase",        "type": "uint256"},
                {"internalType": "uint256", "name": "availableBorrowsBase", "type": "uint256"},
                {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
                {"internalType": "uint256", "name": "ltv",                  "type": "uint256"},
                {"internalType": "uint256", "name": "healthFactor",         "type": "uint256"},
            ],
            "stateMutability": "view",
            "type": "function",
        }]
    )
    try:
        data = await pool.functions.getUserAccountData(
            AsyncWeb3.to_checksum_address(target_address)
        ).call()
        hf_wei = data[5]
        # uint256 max = pozisyon yok (sonsuz HF)
        if hf_wei >= (2**256 - 1):
            return 999.0
        return hf_wei / WAD
    except Exception as exc:
        logger.error("[HF] Okuma hatası: %s", exc)
        return 999.0


# ─────────────────────────────────────────────────────────────────────────────
# TETİK: TX İMZALA VE GÖNDER
# ─────────────────────────────────────────────────────────────────────────────

async def fire_sniper_tx(
    w3:          AsyncWeb3,
    state:       SniperState,
    source:      str,            # "MOTOR1" veya "MOTOR2" — loglama için
    private_key: str,
    gas_limit:   int   = 800_000,
    gas_multiplier: float = 1.2,
    max_retries: int   = 3,
) -> bool:
    """
    Aave liquidationCall() işlemini imzalar ve Arbitrum ağına gönderir.

    Neden bu şekilde yapılandırıldı?
      1. Nonce önbelleği: Her çağrıda on-chain nonce okunur.
         İki motor aynı anda çağırırsa pending nonce çakışır.
         Çözüm: asyncio.Event (fired) ile koordinasyon.
         İlk ateş eden fired.set() yapar; ikincisi pending nonce+1 alır.

      2. Gas bump: Her retry'da gas_price %15 artar.
         Ağda tıkanma varsa işlem önce sıraya alınır, bump ile öne geçer.

      3. receiveAToken=False: Teminatı aToken değil, underlying olarak alıyoruz.
         Daha likid — hemen DEX'te satılabilir.

    Parametreler:
      w3:             AsyncWeb3 nesnesi (Fast RPC veya Alchemy)
      state:          Paylaşılan sniper durumu
      source:         Hangi motor ateşledi (log için)
      private_key:    İşlemi imzalayan key (0x prefix ile)
      gas_limit:      liquidationCall için yeterli limit (800k safe)
      gas_multiplier: Mevcut base fee × bu çarpan = max gas price
      max_retries:    Başarısız tx'te kaç kez tekrar dene

    Dönüş:
      True:  tx mined ve başarılı
      False: tüm retry'lar tükendi veya hata
    """
    target = state.target
    account = AsyncWeb3.to_checksum_address(
        AsyncWeb3.from_key(private_key).address
    )

    # liquidationCall'da kullanılacak borç miktarı
    # close_factor uygulanmış wei değeri
    debt_to_cover_wei = int(target.debt_raw * target.close_factor)

    pool_contract = w3.eth.contract(
        address=AsyncWeb3.to_checksum_address(AAVE_POOL_ARB),
        abi=AAVE_LIQUIDATION_ABI,
    )

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "[%s] 🔫 TX Gönderiliyor... Deneme %d/%d | "
                "Hedef: %s | debtToCover: %d wei",
                source, attempt, max_retries,
                target.address, debt_to_cover_wei,
            )

            # ── Nonce: her denemede taze oku ─────────────────────────────────
            # Neden pending yerine latest?
            # "latest" ile on-chain confirm'lenmiş nonce alırız.
            # İki motor aynı anda çalışıyorsa biri MUTLAKA farklı nonce alır
            # ve Arbitrum sequencer ikisini de sırayla işler.
            nonce = await w3.eth.get_transaction_count(account, "latest")

            # ── Gas fiyatı: base fee + agresif tip ───────────────────────────
            # Arbitrum'da EIP-1559 destekli L2 tip genellikle düşüktür (~0.01 gwei).
            # Sequencer önceliği için base_fee × multiplier kullanırız.
            block        = await w3.eth.get_block("latest")
            base_fee     = block.get("baseFeePerGas", 100_000_000)  # 0.1 gwei fallback
            # Retry'larda gas'ı artır — stuck tx'i çözmek için
            retry_bump   = 1.0 + (attempt - 1) * 0.15
            max_fee      = int(base_fee * gas_multiplier * retry_bump)
            priority_fee = int(0.01 * 1e9 * retry_bump)  # ~0.01 gwei, retry'da artar

            # ── TX verisi ─────────────────────────────────────────────────────
            tx = await pool_contract.functions.liquidationCall(
                AsyncWeb3.to_checksum_address(target.collateral_address),  # collateralAsset
                AsyncWeb3.to_checksum_address(target.debt_address),        # debtAsset
                AsyncWeb3.to_checksum_address(target.address),             # user (kurban)
                debt_to_cover_wei,                                          # debtToCover
                False,                                                      # receiveAToken
            ).build_transaction({
                "from":                 account,
                "nonce":                nonce,
                "gas":                  gas_limit,
                "maxFeePerGas":         max_fee,
                "maxPriorityFeePerGas": priority_fee,
                "chainId":              42161,   # Arbitrum One chain ID
            })

            # ── Dry run kontrolü ──────────────────────────────────────────────
            if state.dry_run:
                logger.warning(
                    "[%s] 🧪 DRY RUN — TX imzalanmadı.\n"
                    "  collateral: %s\n  debt:       %s\n  user:       %s\n"
                    "  debtToCover: %d wei\n  gasLimit:   %d\n  maxFee:     %d",
                    source,
                    target.collateral_address, target.debt_address,
                    target.address, debt_to_cover_wei, gas_limit, max_fee,
                )
                state.fire_count += 1
                state.fired.set()
                return True

            # ── İmzala ────────────────────────────────────────────────────────
            signed = w3.eth.account.sign_transaction(tx, private_key)

            # ── Gönder ────────────────────────────────────────────────────────
            tx_hash = await w3.eth.send_raw_transaction(signed.raw_transaction)
            tx_hex  = tx_hash.hex()

            logger.info(
                "[%s] 🚀 TX Gönderildi | Hash: %s | Nonce: %d",
                source, tx_hex, nonce,
            )

            # ── Telegram bildirimi ────────────────────────────────────────────
            tg.send(
                f"🔫 <b>[SNIPER-{source}] TX ATEŞLENDI!</b>\n"
                f"🎯 Hedef: <code>{target.address}</code>\n"
                f"💰 Borç Kapatılıyor: {target.close_factor*100:.0f}% ({target.debt_token})\n"
                f"🏦 Teminat Alınıyor: {target.collateral_token}\n"
                f"📊 Tahmini Kâr: <code>${state.target.estimated_profit_usd:,.2f}</code>\n"
                f"🔗 TX: <code>{tx_hex}</code>\n"
                f"⛽ Max Fee: {max_fee/1e9:.4f} gwei | Gas: {gas_limit:,}"
            )

            # ── Mine bekle ────────────────────────────────────────────────────
            # Arbitrum sequencer hızlıdır — blok süresi ~0.25s
            # 60 saniye timeout: revert de dahil her sonucu yakalarız
            receipt = await asyncio.wait_for(
                w3.eth.wait_for_transaction_receipt(tx_hash, poll_latency=0.3),
                timeout=60.0,
            )

            if receipt["status"] == 1:
                # ── BAŞARI ────────────────────────────────────────────────────
                state.fire_count += 1
                state.fired.set()   # Diğer motora "artık atma" sinyali ver
                gas_used = receipt["gasUsed"]
                logger.warning(
                    "[%s] ✅ LİKİDASYON BAŞARILI! | Gas: %d | "
                    "Blok: %d | TX: %s",
                    source, gas_used, receipt["blockNumber"], tx_hex,
                )
                tg.send(
                    f"✅ <b>[SNIPER-{source}] LİKİDASYON ONAYLANDI!</b>\n"
                    f"🎯 Hedef: <code>{target.address}</code>\n"
                    f"⛽ Gas Kullanıldı: {gas_used:,}\n"
                    f"📦 Blok: {receipt['blockNumber']}\n"
                    f"🔗 TX: <code>{tx_hex}</code>"
                )
                return True

            else:
                # ── REVERT ────────────────────────────────────────────────────
                # Arbitrum'da revert ucuzdur (~0.02$). Logla, sonraki denemeye geç.
                logger.warning(
                    "[%s] ❌ TX REVERT | Blok: %d | TX: %s | "
                    "Deneme %d/%d",
                    source, receipt["blockNumber"], tx_hex, attempt, max_retries,
                )
                if attempt < max_retries:
                    await asyncio.sleep(0.5)  # Arbitrum bloğu gelmeden yeniden deneme
                continue

        except asyncio.TimeoutError:
            logger.warning("[%s] TX timeout — Deneme %d/%d", source, attempt, max_retries)
            continue
        except Exception as exc:
            logger.error("[%s] TX hatası (Deneme %d/%d): %s", source, attempt, max_retries, exc)
            if attempt < max_retries:
                await asyncio.sleep(1.0)
            continue

    logger.error("[%s] ❌ Tüm denemeler tükendi.", source)
    tg.send(
        f"💀 <b>[SNIPER-{source}] TÜM DENEMELER TÜKENDİ</b>\n"
        f"🎯 Hedef: <code>{target.address}</code>\n"
        f"🔁 {max_retries} deneme başarısız"
    )
    return False


# ─────────────────────────────────────────────────────────────────────────────
# MOTOR 1 — KÖR ATIŞ: CHAINLINK WSS + ALCHEMY STATE WATCH
# ─────────────────────────────────────────────────────────────────────────────

async def motor1_chainlink_price_watcher(
    state:       SniperState,
    w3_alchemy:  AsyncWeb3,
    wss_url:     str,
    private_key: str,
    gas_limit:   int,
    gas_multiplier: float,
    max_retries: int,
    trigger_margin: float = 0.005,  # %0.5 erken tetik marjı
) -> None:
    """
    Motor 1 — Kör Atış: Chainlink fiyatı izler, HF'ye sormaz.

    Algoritma:
      1. eth_subscribe("logs") ile Chainlink AnswerUpdated event'ini dinle.
      2. Her fiyat güncellemesinde: yeni_fiyat ≤ liq_price × (1 + margin)?
         → Tetik eşiğine girildi → fire_sniper_tx()
      3. fired.is_set() kontrolü: diğer motor zaten attıysa atla.

    Neden "logs" abonesi?
      Chainlink her round'da AnswerUpdated emit eder.
      Bu event'i dinlemek, sürekli latestRoundData() polling'den çok
      daha az gecikme verir (push vs pull).

    trigger_margin:
      %0.5 erken tetik — Arbitrum'da tx gönderme + mining süresi ~0.5s.
      WETH hızlı düşerse tam eşikte tetiklemek tx'i kaçırabilir.
      Bu marj, tx mined olduğunda fiyat tam eşikte olur.

    Reconnect:
      WSS düşerse exponential backoff ile yeniden bağlanır.
      Sniper asla ölmez.
    """
    tag     = "MOTOR1-CHAINLINK"
    backoff = WSS_INITIAL_BACKOFF
    target  = state.target

    logger.info(
        "[%s] Başlatılıyor | Tasfiye Fiyatı: $%.4f | Margin: %%%s",
        tag, target.liquidation_price, trigger_margin * 100,
    )
    tg.send(
        f"🎯 <b>[SNIPER-MOTOR1] Aktif</b>\n"
        f"📍 Hedef: <code>{target.address}</code>\n"
        f"💥 Tasfiye Fiyatı: <code>${target.liquidation_price:,.4f}</code> ({target.collateral_token}/USD)\n"
        f"📊 Tahmini Kâr: <code>${target.estimated_profit_usd:,.2f}</code>"
    )

    while True:
        try:
            async with websockets.connect(
                wss_url,
                ping_interval = WSS_PING_INTERVAL,
                ping_timeout  = WSS_PING_TIMEOUT,
                close_timeout = WSS_CLOSE_TIMEOUT,
                max_size      = WSS_MAX_MSG_SIZE,
            ) as ws:

                # ── Chainlink AnswerUpdated event abonesi ─────────────────────
                # address: CHAINLINK_WETH_USD_ARB feed contract
                # topics[0]: AnswerUpdated event signature hash
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": CHAINLINK_WETH_USD_ARB,
                            "topics":  [CHAINLINK_ANSWER_UPDATED_TOPIC],
                        },
                    ],
                }))

                raw_resp = await asyncio.wait_for(ws.recv(), timeout=10.0)
                resp     = json.loads(raw_resp)

                if "error" in resp:
                    raise ValueError(f"eth_subscribe hatası: {resp['error']}")

                sub_id = resp.get("result", "")
                logger.info("[%s] ✅ Chainlink feed aboneliği: %s...", tag, sub_id[:12])
                backoff = WSS_INITIAL_BACKOFF

                async for raw_msg in ws:
                    # ── Fired kontrolü: diğer motor attıysa dinlemeyi bırakma
                    # ama ateş etme — pozisyon hâlâ mevcut olabilir
                    if state.fired.is_set() and state.fire_count >= 2:
                        logger.info("[%s] İki motor da ateşledi — dinleme sonlandı.", tag)
                        return

                    try:
                        msg = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    params = msg.get("params", {})
                    if (msg.get("method") != "eth_subscription"
                            or params.get("subscription") != sub_id):
                        continue

                    result = params.get("result", {})
                    topics = result.get("topics", [])

                    if not topics:
                        continue

                    # ── Fiyatı ABI decode et ──────────────────────────────────
                    # AnswerUpdated: topics[1] = int256 current (indexed)
                    # Indexed int256 → hex string → int → / 10^8
                    try:
                        price_hex   = topics[1] if len(topics) > 1 else "0x0"
                        # Negatif int256 için signed decode
                        price_raw   = int.from_bytes(
                            bytes.fromhex(price_hex.lstrip("0x").zfill(64)),
                            byteorder="big", signed=True,
                        )
                        current_price = price_raw / CHAINLINK_DECIMALS
                    except Exception as decode_exc:
                        logger.debug("[%s] Fiyat decode hatası: %s", tag, decode_exc)
                        continue

                    if current_price <= 0:
                        continue

                    state.last_price = current_price

                    # ── Tetik kontrolü ────────────────────────────────────────
                    # Trigger eşiği: tasfiye_fiyatı × (1 + margin)
                    # Yani fiyat eşiğin %0.5 üzerine girince ateş ederiz.
                    # Böylece tx blokda işlendiğinde fiyat tam eşikte olur.
                    trigger_threshold = target.liquidation_price * (1 + trigger_margin)

                    logger.debug(
                        "[%s] Fiyat: $%.2f | Eşik: $%.2f | Tasfiye: $%.2f",
                        tag, current_price, trigger_threshold, target.liquidation_price,
                    )

                    if current_price <= trigger_threshold:
                        # Fired kontrolü — hızlı check, async lock yok
                        if state.fired.is_set():
                            logger.info("[%s] Zaten ateşlendi — KÖR ATIŞ atlandı.", tag)
                            continue

                        logger.warning(
                            "[%s] 🎯 TETİK! Fiyat $%.2f ≤ Eşik $%.2f (Tasfiye: $%.2f)",
                            tag, current_price, trigger_threshold, target.liquidation_price,
                        )

                        # ── Ateş et ───────────────────────────────────────────
                        asyncio.create_task(
                            fire_sniper_tx(
                                w3_alchemy, state, "MOTOR1",
                                private_key, gas_limit, gas_multiplier, max_retries,
                            ),
                            name="motor1-fire",
                        )

        except asyncio.CancelledError:
            logger.info("[%s] Task iptal edildi.", tag)
            return
        except Exception as exc:
            logger.warning("[%s] WSS hatası: %s. %ds sonra reconnect...", tag, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WSS_MAX_BACKOFF)


async def motor1_alchemy_state_watcher(
    state:      SniperState,
    w3_alchemy: AsyncWeb3,
    wss_url:    str,
) -> None:
    """
    Motor 1'in ikinci kolu — Alchemy WSS üzerinden cüzdan state değişimi izler.

    Algoritma:
      1. eth_subscribe("logs") ile hedef cüzdanın ERC-20 Transfer event'lerini
         dinle (hem collateral hem debt token için).
      2. Transfer tetiklenince refresh_target_state() → yeni bakiyeler okunur.
      3. Yeni bakiyelerle liquidation_price yeniden hesaplanır.
      4. Telegram'a bildirim gönderilir (hedef pozisyonu değiştirdi).

    Neden Transfer event?
      Hedef teminat ekler   → aToken Transfer (mint) emit edilir.
      Hedef borç öder       → debtToken Transfer (burn) emit edilir.
      Hedef teminat çeker   → aToken Transfer (burn) emit edilir.
      Tüm bu durumlar liquidation_price'ı değiştirir → yeniden hesaplamalıyız.

    NOT: Bu kol ateş etmez, sadece state günceller.
    Ateşleme kararı Motor 1'in Chainlink kolunda.
    """
    tag     = "MOTOR1-STATE"
    backoff = WSS_INITIAL_BACKOFF
    target  = state.target

    # ERC-20 Transfer topic
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    # İzlenecek adresler: collateral token + debt token
    watch_addresses = [
        AsyncWeb3.to_checksum_address(target.collateral_address),
        AsyncWeb3.to_checksum_address(target.debt_address),
    ]

    logger.info("[%s] Cüzdan aktivitesi izleniyor: %s", tag, target.address[:12])

    while True:
        try:
            async with websockets.connect(
                wss_url,
                ping_interval = WSS_PING_INTERVAL,
                ping_timeout  = WSS_PING_TIMEOUT,
                close_timeout = WSS_CLOSE_TIMEOUT,
                max_size      = WSS_MAX_MSG_SIZE,
            ) as ws:

                # Transfer event'lerini dinle
                # topics[1] = from address (indexed), topics[2] = to address (indexed)
                # Hedef cüzdanın dahil olduğu tüm transfer'ları yakalar
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 2,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": watch_addresses,
                            "topics":  [transfer_topic],
                        },
                    ],
                }))

                raw_resp = await asyncio.wait_for(ws.recv(), timeout=10.0)
                resp     = json.loads(raw_resp)
                sub_id   = resp.get("result", "")
                logger.info("[%s] ✅ Transfer log aboneliği: %s...", tag, sub_id[:12])
                backoff  = WSS_INITIAL_BACKOFF

                # Hedef adresin kısaltılmış ve sıfır padded hali (topic formatı)
                target_topic = (
                    "0x000000000000000000000000"
                    + target.address.lower().lstrip("0x")
                )

                async for raw_msg in ws:
                    if state.fired.is_set() and state.fire_count >= 2:
                        return

                    try:
                        msg = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    params = msg.get("params", {})
                    if (msg.get("method") != "eth_subscription"
                            or params.get("subscription") != sub_id):
                        continue

                    result = params.get("result", {})
                    topics = result.get("topics", [])

                    # Hedef cüzdan bu Transfer'da taraf mı?
                    if len(topics) < 3:
                        continue
                    if topics[1].lower() != target_topic and topics[2].lower() != target_topic:
                        continue

                    logger.info(
                        "[%s] 🔄 STATE DEĞİŞİMİ! Hedef Transfer event aldı | "
                        "Token: %s | Blok: %s",
                        tag,
                        result.get("address", "?")[:12],
                        result.get("blockNumber", "?"),
                    )

                    # Yeni bakiyeleri oku ve liquidation_price güncelle
                    old_liq = target.liquidation_price
                    await refresh_target_state(w3_alchemy, target)

                    # Yeni liquidation_price hesabı için collateral_lt gerekir
                    # .env'den veya sabit olarak alınır (WETH ARB LT = 0.825)
                    coll_lt = float(os.getenv("COLLATERAL_LT", "0.825"))
                    coll_amount = target.collateral_raw / (10 ** target.collateral_decimals)
                    debt_usd    = (target.debt_raw / (10 ** target.debt_decimals)) * state.last_price

                    if coll_amount > 0 and debt_usd > 0:
                        target.liquidation_price = calculate_liquidation_price(
                            coll_amount, coll_lt, debt_usd
                        )
                        target.estimated_profit_usd = calculate_estimated_profit(
                            debt_usd, target.bonus, target.close_factor,
                        )

                    logger.info(
                        "[%s] Tasfiye fiyatı güncellendi: $%.4f → $%.4f",
                        tag, old_liq, target.liquidation_price,
                    )
                    tg.send(
                        f"🔄 <b>[SNIPER] Hedef Pozisyon DEĞİŞTİ</b>\n"
                        f"👤 <code>{target.address}</code>\n"
                        f"📉 Eski Tasfiye Fiyatı: <code>${old_liq:,.4f}</code>\n"
                        f"📈 Yeni Tasfiye Fiyatı: <code>${target.liquidation_price:,.4f}</code>\n"
                        f"💰 Tahmini Kâr: <code>${target.estimated_profit_usd:,.2f}</code>"
                    )

        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("[%s] WSS hatası: %s. %ds reconnect...", tag, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WSS_MAX_BACKOFF)


# ─────────────────────────────────────────────────────────────────────────────
# MOTOR 2 — KESİN ATIŞ: FAST RPC HF POLLING
# ─────────────────────────────────────────────────────────────────────────────

async def motor2_confirmed_fire(
    state:       SniperState,
    w3_fast:     AsyncWeb3,
    private_key: str,
    poll_interval: float = 0.5,  # saniye — Arbitrum blok süresi ~0.25s
    gas_limit:   int   = 800_000,
    gas_multiplier: float = 1.15,  # Motor 2 biraz daha düşük gas — Motor 1 önce gitsin
    max_retries: int   = 3,
) -> None:
    """
    Motor 2 — Kesin Ateş: HF'yi Fast RPC'den okuyarak tetik kararı verir.

    Algoritma:
      1. Her poll_interval'da getUserAccountData() → HF çek.
      2. HF < 1.00 → fired kontrolü → fire_sniper_tx()
      3. fired.is_set() → Motor 1 zaten attı → ben de at (ikinci mermi)
         Neden?
           Motor 1 Revert yapmış olabilir. Motor 2 sigorta.
           Arbitrum'da iki tx peş peşe işlenir. Biri başarılı olur.

    Neden poll_interval = 0.5s?
      Arbitrum blok süresi ~0.25s. İki blokta bir kontrol yeterli.
      Daha sık = daha fazla RPC quota tüketimi.

    Neden ayrı w3_fast?
      Alchemy shared endpoint ile Motor 1'in WSS bağlantısı RPC rate
      limitini paylaşır. Motor 2'nin QuickNode/Blast gibi premium
      bir endpoint'i olması, yüksek yük altında gecikmeyi önler.
    """
    tag    = "MOTOR2-CONFIRMED"
    target = state.target

    logger.info(
        "[%s] Başlatılıyor | Poll: %.1fs | Fast RPC aktif",
        tag, poll_interval,
    )

    while True:
        await asyncio.sleep(poll_interval)

        try:
            hf = await get_current_hf(w3_fast, target.address)
            state.last_hf = hf

            logger.debug("[%s] HF: %.6f | Fired: %s", tag, hf, state.fired.is_set())

            if hf < 1.00:
                # HF < 1.00 onaylandı — ateş et
                if state.fired.is_set():
                    # Motor 1 zaten ateşledi — sigorta mermisi
                    logger.info(
                        "[%s] ⚡ HF=%.4f < 1.00 | Motor 1 zaten ateşledi → "
                        "SİGORTA MERMİSİ gönderiliyor",
                        tag, hf,
                    )
                else:
                    logger.warning(
                        "[%s] 🎯 TETİK! HF=%.4f < 1.00 (Motor 1 henüz ateşlemedi)",
                        tag, hf,
                    )

                asyncio.create_task(
                    fire_sniper_tx(
                        w3_fast, state, "MOTOR2",
                        private_key, gas_limit, gas_multiplier, max_retries,
                    ),
                    name="motor2-fire",
                )

            elif hf > 1.10 and state.last_hf < 1.05:
                # HF güvenli bölgeye çıktı — pozisyon kapatılmış veya teminat eklendi
                logger.info(
                    "[%s] 🟢 HF güvenli bölgeye çıktı: %.4f → Sniper beklemeye geçiyor.",
                    tag, hf,
                )

        except asyncio.CancelledError:
            logger.info("[%s] Task iptal edildi.", tag)
            return
        except Exception as exc:
            logger.error("[%s] HF polling hatası: %s", tag, exc)
            await asyncio.sleep(1.0)


# ─────────────────────────────────────────────────────────────────────────────
# CLI ARGÜMAN PARSER + KONFİGÜRASYON
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Aave V3 Arbitrum Dual-Engine Sniper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Örnek:\n"
            "  python sniper.py --target 0xABC... --collateral-token WETH "
            "--collateral-addr 0x... --collateral-lt 0.825 \\\n"
            "      --debt-token USDC --debt-addr 0x... "
            "--debt-decimals 6 --debt-usd 8000\n"
        )
    )
    p.add_argument("--target",            help="Hedef cüzdan adresi (.env SNIPER_TARGET)")
    p.add_argument("--collateral-token",  default="WETH",  help="Teminat token sembolü")
    p.add_argument("--collateral-addr",   help="Teminat token ERC-20 adresi")
    p.add_argument("--collateral-raw",    type=int, default=0, help="Teminat ham miktar (wei)")
    p.add_argument("--collateral-decimals", type=int, default=18)
    p.add_argument("--collateral-lt",     type=float, default=0.825,
                   help="Liquidation Threshold (WETH ARB = 0.825)")
    p.add_argument("--debt-token",        default="USDC",  help="Borç token sembolü")
    p.add_argument("--debt-addr",         help="Borç token ERC-20 adresi")
    p.add_argument("--debt-raw",          type=int, default=0, help="Borç ham miktar (wei)")
    p.add_argument("--debt-decimals",     type=int, default=6)
    p.add_argument("--debt-usd",          type=float, default=0.0,
                   help="Toplam borç USD değeri (elle girilirse on-chain sorgu yapılmaz)")
    p.add_argument("--close-factor",      type=float, default=0.5,
                   help="Close factor: 0.5 (HF>0.95) veya 1.0 (HF<0.95)")
    p.add_argument("--trigger-margin",    type=float, default=0.005,
                   help="Erken tetik marjı (varsayılan %%0.5)")
    p.add_argument("--poll-interval",     type=float, default=0.5,
                   help="Motor 2 HF poll aralığı saniye (varsayılan 0.5)")
    p.add_argument("--dry",               action="store_true",
                   help="Kuru çalışma — TX imzalamaz, sadece simüle eder")
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    args = parse_args()

    # ── Ortam değişkenleri ────────────────────────────────────────────────────
    target_addr  = args.target         or os.getenv("SNIPER_TARGET", "")
    private_key  = os.getenv("SNIPER_PRIVATE_KEY", "")
    alchemy_wss  = os.getenv("ALCHEMY_WSS_URL",   "wss://arb-mainnet.g.alchemy.com/v2/hmJIUy5LpYIq8eW4OGtmy")
    alchemy_http = os.getenv("ARB_RPC",            "https://arb-mainnet.g.alchemy.com/v2/hmJIUy5LpYIq8eW4OGtmy")
    fast_rpc     = os.getenv("FAST_RPC", alchemy_http)  # Premium RPC yoksa Alchemy fallback
    gas_limit    = int(os.getenv("SNIPER_GAS_LIMIT",      "800000"))
    gas_mult     = float(os.getenv("SNIPER_GAS_MULTIPLIER", "1.2"))
    max_retries  = int(os.getenv("SNIPER_MAX_RETRIES",    "3"))
    coll_lt      = float(os.getenv("COLLATERAL_LT",       str(args.collateral_lt)))

    # ── Zorunlu parametre kontrolleri ─────────────────────────────────────────
    errors = []
    if not target_addr:
        errors.append("Hedef cüzdan eksik! --target veya SNIPER_TARGET")
    if not private_key and not args.dry:
        errors.append("Private key eksik! SNIPER_PRIVATE_KEY (.env)")
    if not args.collateral_addr and not os.getenv("COLLATERAL_ADDR"):
        errors.append("Teminat token adresi eksik! --collateral-addr veya COLLATERAL_ADDR")
    if not args.debt_addr and not os.getenv("DEBT_ADDR"):
        errors.append("Borç token adresi eksik! --debt-addr veya DEBT_ADDR")

    if errors:
        for e in errors:
            logger.error("❌ %s", e)
        sys.exit(1)

    coll_addr = args.collateral_addr or os.getenv("COLLATERAL_ADDR", "")
    debt_addr = args.debt_addr       or os.getenv("DEBT_ADDR", "")

    # ── Web3 bağlantıları ─────────────────────────────────────────────────────
    logger.info("Web3 bağlantıları kuruluyor...")
    w3_alchemy = await build_w3(alchemy_http)
    w3_fast    = await build_w3(fast_rpc) if fast_rpc != alchemy_http else w3_alchemy

    # ── Başlangıç fiyatı ──────────────────────────────────────────────────────
    init_price = await get_chainlink_price(w3_alchemy)
    if init_price <= 0:
        logger.error("Chainlink fiyat alınamadı — devam edilemiyor.")
        sys.exit(1)
    logger.info("Başlangıç %s/USD fiyatı: $%.2f", args.collateral_token, init_price)

    # ── Teminat ve borç miktarları ────────────────────────────────────────────
    # CLI'dan verilmediyse on-chain'den oku
    coll_raw = args.collateral_raw
    debt_raw = args.debt_raw

    if coll_raw == 0:
        try:
            coll_contract = w3_alchemy.eth.contract(
                address=AsyncWeb3.to_checksum_address(coll_addr), abi=ERC20_BALANCE_ABI
            )
            coll_raw = await coll_contract.functions.balanceOf(
                AsyncWeb3.to_checksum_address(target_addr)
            ).call()
            logger.info("Teminat bakiyesi (on-chain): %d wei", coll_raw)
        except Exception as exc:
            logger.error("Teminat bakiyesi okunamadı: %s", exc)

    if debt_raw == 0:
        try:
            debt_contract = w3_alchemy.eth.contract(
                address=AsyncWeb3.to_checksum_address(debt_addr), abi=ERC20_BALANCE_ABI
            )
            debt_raw = await debt_contract.functions.balanceOf(
                AsyncWeb3.to_checksum_address(target_addr)
            ).call()
            logger.info("Borç bakiyesi (on-chain): %d wei", debt_raw)
        except Exception as exc:
            logger.error("Borç bakiyesi okunamadı: %s", exc)

    # ── SniperTarget oluştur ──────────────────────────────────────────────────
    coll_amount = coll_raw / (10 ** args.collateral_decimals)
    debt_usd    = args.debt_usd if args.debt_usd > 0 else \
                  (debt_raw / (10 ** args.debt_decimals))   # 1:1 stablecoin varsayımı

    bonus = LIQUIDATION_BONUS_MAP.get(args.collateral_token, DEFAULT_BONUS)

    liq_price  = calculate_liquidation_price(coll_amount, coll_lt, debt_usd)
    est_profit = calculate_estimated_profit(debt_usd, bonus, args.close_factor)

    target = SniperTarget(
        address              = AsyncWeb3.to_checksum_address(target_addr),
        collateral_token     = args.collateral_token,
        collateral_address   = coll_addr,
        collateral_decimals  = args.collateral_decimals,
        collateral_raw       = coll_raw,
        debt_token           = args.debt_token,
        debt_address         = debt_addr,
        debt_decimals        = args.debt_decimals,
        debt_raw             = debt_raw,
        liquidation_price    = liq_price,
        estimated_profit_usd = est_profit,
        bonus                = bonus,
        close_factor         = args.close_factor,
    )

    state         = SniperState(target=target, dry_run=args.dry)
    state.last_price = init_price

    # ── Banner ────────────────────────────────────────────────────────────────
    logger.info("=" * 68)
    logger.info("  AAVE V3 ARB DUAL-ENGINE SNIPER")
    logger.info("  Hedef        : %s", target.address)
    logger.info("  Teminat      : %s (%.4f %s, LT=%.3f)",
                target.collateral_token, coll_amount, target.collateral_token, coll_lt)
    logger.info("  Borç         : $%.2f %s", debt_usd, target.debt_token)
    logger.info("  Bonus        : %.1f%%", bonus * 100)
    logger.info("  Close Factor : %.0f%%", args.close_factor * 100)
    logger.info("  Liq. Fiyatı  : $%.4f / %s", liq_price, target.collateral_token)
    logger.info("  Güncel Fiyat : $%.2f / %s", init_price, target.collateral_token)
    logger.info("  Fiyat Marjı  : $%.2f (Eşikten %%%s uzakta)",
                init_price - liq_price, f"{((init_price/liq_price)-1)*100:.1f}")
    logger.info("  Tahmini Kâr  : $%.2f", est_profit)
    logger.info("  Dry Run      : %s", "EVET — TX GÖNDERİLMEZ" if args.dry else "HAYIR")
    logger.info("  Fast RPC     : %s", fast_rpc[:50])
    logger.info("=" * 68)

    if est_profit < 0:
        logger.warning("⚠️  Tahmini kâr negatif ($%.2f) — devam mı? (Ctrl+C ile durdur)", est_profit)
        await asyncio.sleep(5)

    if args.dry:
        logger.warning("⚠️  DRY RUN MODU — İşlem imzalanmayacak.")

    # ── Paralel task'lar ──────────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(
            motor1_chainlink_price_watcher(
                state, w3_alchemy, alchemy_wss, private_key,
                gas_limit, gas_mult, max_retries, args.trigger_margin,
            ),
            name="motor1-chainlink",
        ),
        asyncio.create_task(
            motor1_alchemy_state_watcher(state, w3_alchemy, alchemy_wss),
            name="motor1-state",
        ),
        asyncio.create_task(
            motor2_confirmed_fire(
                state, w3_fast, private_key,
                args.poll_interval, gas_limit, gas_mult * 0.95, max_retries,
            ),
            name="motor2-confirmed",
        ),
    ]

    logger.info("  %d task başlatıldı:", len(tasks))
    for t in tasks:
        logger.info("    - %s", t.get_name())
    logger.info("=" * 68)

    # Herhangi bir task bitene kadar bekle
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    for t in done:
        if t.exception():
            logger.error("Task '%s' hata ile sonlandı: %s", t.get_name(), t.exception())

    for t in pending:
        t.cancel()

    await asyncio.gather(*pending, return_exceptions=True)
    logger.info("Sniper sonlandı.")


if __name__ == "__main__":
    asyncio.run(main())
