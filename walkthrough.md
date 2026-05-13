# Ghost Offset Mimari Revizyon — Walkthrough

## Yapılan Değişiklikler

Tüm değişiklikler **tek dosyada**: `cluster_sniper.py`

### 1. LST Fiyat Çözünürlüğü — Deterministik Feed Type

```diff:cluster_sniper.py
"""
cluster_sniper.py — Multi-Chain Aave V3 Cluster Dual-Engine Sniper v4
=====================================================================

watcher.py → targets.json → cluster_sniper.py köprüsü.
config.py, telegram_utils.py ortak olarak kullanılır.

Kullanım:
    python cluster_sniper.py --chain ARB          # Arbitrum hedefleri
    python cluster_sniper.py --chain BASE --dry   # Base, TX göndermez
    python cluster_sniper.py --chain OP            # Optimism hedefleri

──────────────────────────────────────────────────────────────────────────────
v3 → v4 REFACTOR: targets.json Entegrasyonu
──────────────────────────────────────────────────────────────────────────────

DEĞİŞİKLİKLER:
  1. kurban.txt kaldırıldı. Hedef kaynağı artık watcher.py'nin yazdığı
     targets.json dosyasıdır. Zorunlu alanlar: chain, address, hf, debt_usd,
     collateral_usd, updated_at. İsteğe bağlı (watcher OptiPair): debt_asset,
     collateral_asset, bonus_pct, effective_close_factor.

  2. targets_json_watcher(): Her 45 saniyede targets.json'u okur,
     yeni hedefler tespit edilir → on-chain pozisyon keşfi yapılır →
     Motor 1/2'ye eklenir. Kaldırılan hedefler devre dışı bırakılır.

  3. Auto-Pair Discovery: init_all_targets artık en büyük borç ve
     en büyük teminat tokenını otomatik seçer. Dossier gereksiz.

  4. E-mode kontrolü kaldırıldı: Basit yapı, gereksiz hantallık.
     Normal LT/bonus değerleri CHAIN_RESERVES'dan kullanılır.

──────────────────────────────────────────────────────────────────────────────
ZORUNLU .env DEĞİŞKENLERİ
──────────────────────────────────────────────────────────────────────────────

  SNIPER_PRIVATE_KEY=0x...     İşlem imzalama key'i
  ARB_RPC=https://...          Alchemy HTTP endpoint (Arbitrum)
  ALCHEMY_WSS_URL=wss://...    Alchemy WSS (Oracle Hub + State watch)
  FAST_RPC=https://...         Motor 2 için premium RPC (QuickNode vb.)
                               Boşsa ARB_RPC kullanılır.

OPSİYONEL:
  BURST_M1=0.001               Bullet 1 marjı (%0.1)
  BURST_M2=0.0005              Bullet 2 marjı (%0.05)
  GAS_LIMIT=900000
  GAS_MULTIPLIER=1.2
  POLL_INTERVAL=0.4
  TARGETS_POLL=45              targets.json okuma aralığı (saniye)
  AAVE_EXECUTOR=0x...          Flash loan executor (opsiyonel)
  UNISWAP_POOL=0x...           Executor'ın kullanacağı Uniswap V3 havuzu
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import websockets
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.middleware import ExtraDataToPOAMiddleware

from config import (
    POOL_ABI, MULTICALL3_ABI, DATA_PROVIDER_ABI, ERC20_ABI,
    LIQUIDATION_BONUS_MAP, DEFAULT_BONUS,
    FLASH_LOAN_FEE, DEX_SLIPPAGE,
    WAD, USD_DECIMALS, MANUAL_LT_VALUES,
    ChainConfig, load_chains,
    ASSET_CLASS,
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


# ─────────────────────────────────────────────────────────────────────────────
# TARGETS.JSON OKUYUCU — watcher.py Köprüsü (v4)
# ─────────────────────────────────────────────────────────────────────────────
# watcher.py hedefleri targets.json'a yazar. cluster_sniper bu dosyayı
# periyodik olarak okuyarak güncel hedef listesini alır.
# ─────────────────────────────────────────────────────────────────────────────

TARGETS_JSON_PATH = os.getenv("TARGETS_JSON", "targets.json")
TARGETS_POLL_INTERVAL = int(os.getenv("TARGETS_POLL", "45"))


def read_targets_json(chain_tag: str) -> Dict[str, Dict]:
    """
    targets.json'dan seçilen ağın hedeflerini okur.

    Dönüş:
      {"0xABC...": {"hf": 1.02, "debt_usd": 5000, "collateral_usd": 6000}, ...}
    """
    try:
        with open(TARGETS_JSON_PATH, "r", encoding="utf-8") as f:
            records = json.load(f)
    except FileNotFoundError:
        logger.warning("[TARGETS] targets.json bulunamadı — boş hedef listesi.")
        return {}
    except json.JSONDecodeError as exc:
        logger.error("[TARGETS] targets.json parse hatası: %s", exc)
        return {}

    result: Dict[str, Dict] = {}
    for rec in records:
        if rec.get("chain", "").upper() != chain_tag.upper():
            continue
        addr = rec.get("address", "")
        if not addr:
            continue
        row: Dict[str, Any] = {
            "hf":             rec.get("hf", 999.0),
            "debt_usd":       rec.get("debt_usd", 0.0),
            "collateral_usd": rec.get("collateral_usd", 0.0),
            "updated_at":     rec.get("updated_at", ""),
        }
        for key in ("collateral_asset", "debt_asset", "bonus_pct", "effective_close_factor"):
            if key in rec and rec[key] is not None:
                row[key] = rec[key]
        result[addr] = row

    n_pair = sum(
        1
        for v in result.values()
        if v.get("collateral_asset") and v.get("debt_asset")
    )
    logger.info(
        "[TARGETS] %s: %d hedef (targets.json), %d kayıtta parite (BORÇ/TEMİNAT) alanı var.",
        chain_tag,
        len(result),
        n_pair,
    )
    return result

# ─────────────────────────────────────────────────────────────────────────────
# ZİNCİR BAZLI AAVE RESERVE TABLOLARI (v3)
# ─────────────────────────────────────────────────────────────────────────────
# Her zincirin kendi reserve listesi, Chainlink feed'leri ve token adresleri.
# Multicall3 adresi EIP-2470 standardı → tüm zincirlerde aynı.
#
# Not: Bu tablolar SADECE cluster_sniper'ın ihtiyaç duyduğu alanları içerir:
#   symbol, asset (underlying), atoken, vtoken, decimals, lt, feed
# ─────────────────────────────────────────────────────────────────────────────

CHAIN_RESERVES: Dict[str, List[Dict]] = {
    # ── Arbitrum ─────────────────────────────────────────────────────────────
    "ARB": [
        {"symbol": "WETH",   "asset": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "atoken": "0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8", "vtoken": "0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351", "decimals": 18, "lt": 0.84, "bonus": 0.05, "feed": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"},
        {"symbol": "WBTC",   "asset": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "atoken": "0x078f358208685046a11C85e8ad32895DED33A249", "vtoken": "0x92b42c66840C7AD907b4BF74879FF3eF7c529473", "decimals": 8,  "lt": 0.78, "bonus": 0.07, "feed": "0x6ce185860a4963106506C203335A2910413708e9"},
        {"symbol": "USDC",   "asset": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "atoken": "0x724dc807b04555b71ed48a6896b6F41593b8C637", "vtoken": "0xf611aEb5013fD2c0511c9CD55c7dc5C1140741A6", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3"},
        {"symbol": "USDC.e", "asset": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", "atoken": "0x625E7708f30cA75bfd92586e17077590C60eb4cD", "vtoken": "0xFCCf3cAbbe80101232d343252614b6A3eE81C989", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3"},
        {"symbol": "USDT",   "asset": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "atoken": "0x6ab707Aca953eDAeFBc4fD23bA73294241490620", "vtoken": "0xfb00AC187a8Eb5AFAE4eACE434F493Eb62672df7", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7"},
        {"symbol": "ARB",    "asset": "0x912CE59144191C1204E64559FE8253a0e49E6548", "atoken": "0x6533afac2E7BCCB20dca161449A13A32D391fb00", "vtoken": "0x44705f578135cC5d703b4c9c122528C73Eb87145", "decimals": 18, "lt": 0.63, "bonus": 0.10, "feed": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6"},
        {"symbol": "DAI",    "asset": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "atoken": "0x82E64f49Ed5EC1bC6e43DAD4FC8Af9bb3A2312EE", "vtoken": "0x8619d80FB0141ba7F184CbF22fd724116D9f7ffC", "decimals": 18, "lt": 0.77, "bonus": 0.05, "feed": "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB"},
        {"symbol": "wstETH", "asset": "0x5979D7b546E38E414F7E9822514be443A4800529", "atoken": "0x513c7E3a9c69cA3e22550eF58AC1C0088e918FFf", "vtoken": "0x77CA01483f379E58174739308945f044e1a764dc", "decimals": 18, "lt": 0.79, "bonus": 0.072, "feed": "0xb523AE262D20A936BC152e6023996e46FDC2A95D"},
        {"symbol": "weETH",  "asset": "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe", "atoken": "0x8437d7c167dfb82ed4cb79cd44b7a32a1dd95c77", "vtoken": "0x3ca5fa07689f266e907439afd1fbb59c44fe12f6", "decimals": 18, "lt": 0.77, "bonus": 0.075, "feed": "0x258a576895DC50c990500775d6591ff2D52059f2"},
        {"symbol": "LINK",   "asset": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4", "atoken": "0x191c10Aa4AF7C30e871E70C95dB0E4eb77237530", "vtoken": "0x953A573793604aF8d41F306FEb8274190dB4aE0e", "decimals": 18, "lt": 0.75, "bonus": 0.10, "feed": "0x86E53CF1B870786351Da77A57575e79CB55812CB"},
        {"symbol": "AAVE",   "asset": "0xba5DdD1f9d7F570dc94a51479a000E3BCE967196", "atoken": "0xe3241b2Bcc3D81580A18bc86105F678AE66380cE", "vtoken": "0x0000000000000000000000000000000000000000", "decimals": 18, "lt": 0.73, "bonus": 0.10, "feed": "0xf97eEAac36bdd096bb2445c7582F9095bfCE04C7"},
    ],
    # ── Base ──────────────────────────────────────────────────────────────────
    "BASE": [
        {"symbol": "WETH",     "asset": "0x4200000000000000000000000000000000000006", "atoken": "0xD4a0e0b9149BCee3C920d2E00b5dE09138fd8bb7", "vtoken": "0x24e6e0795b3c7c71D965fCc4f371803d1c1DcA1E", "decimals": 18, "lt": 0.83, "bonus": 0.05,  "feed": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70"},
        {"symbol": "cbBTC",    "asset": "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf", "atoken": "0xBdb9300b7CDE636d9cD4AFF00f6F009fFBBc8EE6", "vtoken": "0x05e08702028de6AaD395DC6478b554a56920b9AD", "decimals": 8,  "lt": 0.78, "bonus": 0.07,  "feed": "0x07DA0E54543a844a80ABE69c8A12F22B3aA59f9D"},
        {"symbol": "USDC",     "asset": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "atoken": "0x4e65fE4DbA92790696d040ac24Aa414708F5c0AB", "vtoken": "0x59dca05b6c26dBd64b5381374aAaC5CD05644C28", "decimals": 6,  "lt": 0.78, "bonus": 0.05,  "feed": "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B"},
        {"symbol": "wstETH",   "asset": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452", "atoken": "0x99CBC45ea5bb7eF3a5BC08FB1B7E56bB2442Ef0D", "vtoken": "0x41A7C3f5904ad176dACbb1D99101F59ef0811DC1", "decimals": 18, "lt": 0.79, "bonus": 0.06,  "feed": "0xB88BAc61a4Ca37C43a3725912B1f472c9A5bc061"},
        {"symbol": "weETH",    "asset": "0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A", "atoken": "0x7C307e128efA31F540F2E2d976C995E0B65F51F6", "vtoken": "0x8D2e3F1f4b38AA9f1ceD22ac06019c7561B03901", "decimals": 18, "lt": 0.77, "bonus": 0.075, "feed": "0x0A0fFc2952B682eb36cde5Bdb03169C2c5F5305c"},
        {"symbol": "tBTC",     "asset": "0x236aa50979D5f3De3Bd1Eeb40E81137F22ab794b", "atoken": "0xbcFFB4B3beADc989Bd1458740952aF6EC8fBE431", "vtoken": "0x182cDEEC1D52ccad869d621bA422F449FA5809f5", "decimals": 18, "lt": 0.78, "bonus": 0.075, "feed": "0x07DA0E54543a844a80ABE69c8A12F22B3aA59f9D"},
        {"symbol": "EURC",     "asset": "0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42", "atoken": "0x90DA57E0A6C0d166Bf15764E03b83745Dc90025B", "vtoken": "0x03D01595769333174036832e18fA2f17C74f8161", "decimals": 6,  "lt": 0.78, "bonus": 0.05,  "feed": "0xDAe398520e2B67cd3f27aeF9Cf14D93D927f8250"},
        {"symbol": "syrupUSDC","asset": "0x660975730059246A68521a3e2FBD4740173100f5", "atoken": "0xD7424238CcbE7b7198Ab3cFE232e0271E22da7bd", "vtoken": "0x57B8C05ee2cD9d0143eBC21FBD9288C39B9F716c", "decimals": 6,  "lt": 0.92, "bonus": 0.04,  "feed": "0x5e9eCce4aCBBc8172A40f7b8A7c086A4bb5CDeac"},
        {"symbol": "wrsETH",   "asset": "0xEDfa23602D0EC14714057867A78d01e94176BEA0", "atoken": "0x80a94C36747CF51b2FbabDfF045f6D22c1930eD1", "vtoken": "0xe9541C77a111bCAa5dF56839bbC50894eba7aFcb", "decimals": 18, "lt": 0.75, "bonus": 0.075, "feed": "0x87445df51b4A2E5BbD2F0F8564C5bfe65cC3dB61"},
        {"symbol": "AAVE",     "asset": "0x63706e401c06ac8513145b7687A14804d17f814b", "atoken": "0x67EAF2BeE4384a2f84Da9Eb8105C661C123736BA", "vtoken": "0xcEC1Ea95dDEF7CFC27D3D9615E05b035af460978", "decimals": 18, "lt": 0.70, "bonus": 0.10,  "feed": "0xAAEbc0287EC3BF3fe56fCA1E4bcC793b219De551"},
        {"symbol": "GHO",      "asset": "0x6Bb7a212910682DCFdbd5BCBb3e28FB4E8da10Ee", "atoken": "0x067ae75628177FD257c2B1e500993e1a0baBcBd1", "vtoken": "0x38e59ADE183BbEb94583d44213c8f3297e9933e9", "decimals": 18, "lt": 0.00, "bonus": 0.00,  "feed": "0xfc421aD3C883Bf9E7C4f42dE845C4e4405799e73"},
    ],
    # ── Optimism ──────────────────────────────────────────────────────────────
    "OP": [
        {"symbol": "WETH",   "asset": "0x4200000000000000000000000000000000000006", "atoken": "0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8", "vtoken": "0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351", "decimals": 18, "lt": 0.83, "bonus": 0.05, "feed": "0x13e3Ee699D1909E989722E753853AE30b17e08c5"},
        {"symbol": "WBTC",   "asset": "0x68f180fcCe6836688e9084f035309E29Bf0A2095", "atoken": "0x078f358208685046a11C85e8ad32895DED33A249", "vtoken": "0x92b42c66840C7AD907b4BF74879FF3eF7c529473", "decimals": 8,  "lt": 0.78, "bonus": 0.075, "feed": "0xD702DD976Fb76Fffc2D3963D037dfDae5b04E593"},
        {"symbol": "USDC",   "asset": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85", "atoken": "0x38d693cE1dF5AaDF7bC62043aE5EF4B05deC7805", "vtoken": "0x5D557B07776D12967914379C71a1310e917C7b8c", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0x16a9FA2FDa030272Ce99B29CF780dFA30361E0f3"},
        {"symbol": "USDT",   "asset": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58", "atoken": "0x6ab707Aca953eDAeFBc4fD23bA73294241490620", "vtoken": "0xfb00AC187a8Eb5AFAE4eACE434F493Eb62672df7", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0xECef79E109e997bCA29c1c0897ec9d7b03647F5E"},
        {"symbol": "wstETH", "asset": "0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb", "atoken": "0xc45A479877e1e9Dfe9FcD4056c699575a1045dAA", "vtoken": "0x34e2eD44EF7466D5f9E0b782B5c08b57475e7907", "decimals": 18, "lt": 0.79, "bonus": 0.072, "feed": "0x698B585CbC4407e2D54aa898B2600B53C68958f7"},
    ],
}


def _build_chain_data(chain_tag: str) -> tuple:
    """
    Seçilen zincire göre FEED_MAP, ALL_FEED_ADDRESSES ve ALL_AAVE_TOKENS
    oluşturur. Eski modül-düzeyindeki global'lerin runtime karşılığı.
    """
    reserves = CHAIN_RESERVES.get(chain_tag.upper(), [])

    feed_map: Dict[str, List[str]] = {}
    for r in reserves:
        f = r["feed"].lower()
        feed_map.setdefault(f, []).append(r["symbol"])

    all_feed_addresses = list(set(r["feed"] for r in reserves))

    all_aave_tokens = set()
    for r in reserves:
        all_aave_tokens.add(r["atoken"])
        if r["vtoken"] != "0x0000000000000000000000000000000000000000":
            all_aave_tokens.add(r["vtoken"])

    return reserves, feed_map, all_feed_addresses, list(all_aave_tokens)



# ─────────────────────────────────────────────────────────────────────────────
# SABİTLER
# ─────────────────────────────────────────────────────────────────────────────

# Chainlink AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)
CHAINLINK_ANSWER_UPDATED = (
    "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
)
CHAINLINK_DECIMALS = 10 ** 8  # Chainlink fiyatları 8 ondalıklıdır

# Multicall3: EIP-2470 standardı, tüm zincirlerde aynı adres
MULTICALL3_ADDR = "0xcA11bde05977b3631167028862bE2a173976CA11"

# Modül-düzeyi runtime değişkenleri — main() tarafından set edilir
# build_and_sign_bullet() bu değerlere ihtiyaç duyar ama parametreye erişimi yok
_ACTIVE_CHAIN_ID:  int = 42161  # Varsayılan Arbitrum, main()'de güncellenir
_ACTIVE_POOL_ADDR: str = ""     # main()'de chain_cfg.pool_address ile set edilir

# Uniswap V3 Fee Tier (executor kontratın swap yaparken kullanacağı komisyon katmanı)
# 100 = %0.01, 500 = %0.05, 3000 = %0.30, 10000 = %1.00
# .sol kontratındaki executeSnipe() fonksiyonuna uint24 olarak geçilir.
DEFAULT_UNISWAP_FEE_TIER = int(os.getenv("UNISWAP_FEE_TIER", "500"))  # %0.05 varsayılan

# Parite bazlı optimal fee tier haritası
# Uniswap V3 Arbitrum'da likiditenin yoğunlaştığı havuzlar:
UNISWAP_FEE_TIER_MAP: Dict[str, int] = {
    "WBTC/USDC":  500,    # %0.05 — Ana stablecoin havuzu
    "WBTC/USDT":  500,    # %0.05
    "WETH/USDC":  500,    # %0.05 — Ana stablecoin havuzu
    "WETH/USDT":  500,    # %0.05
    "ARB/WETH":   3000,   # %0.30 — Volatil parite, daha geniş spread
    "WETH/ARB":   3000,   # %0.30 (tersi)
}

# Aave V3 liquidationCall ABI
LIQUIDATION_CALL_ABI = [{
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
}]

# getUserAccountData ABI (Motor 2 — HF okuma)
GET_ACCOUNT_DATA_ABI = [{
    "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
    "name": "getUserAccountData",
    "outputs": [
        {"internalType": "uint256", "name": "totalCollateralBase",         "type": "uint256"},
        {"internalType": "uint256", "name": "totalDebtBase",               "type": "uint256"},
        {"internalType": "uint256", "name": "availableBorrowsBase",        "type": "uint256"},
        {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
        {"internalType": "uint256", "name": "ltv",                         "type": "uint256"},
        {"internalType": "uint256", "name": "healthFactor",                "type": "uint256"},
    ],
    "stateMutability": "view",
    "type": "function",
}]

# Chainlink latestRoundData ABI (HTTP başlangıç fetch için)
CHAINLINK_ROUND_ABI = [{
    "inputs": [],
    "name": "latestRoundData",
    "outputs": [
        {"name": "roundId",    "type": "uint80"},
        {"name": "answer",     "type": "int256"},
        {"name": "startedAt",  "type": "uint256"},
        {"name": "updatedAt",  "type": "uint256"},
        {"name": "answeredIn", "type": "uint80"},
    ],
    "stateMutability": "view",
    "type": "function",
}]

# WSS bağlantı parametreleri
WSS_PING_INTERVAL   = 20
WSS_PING_TIMEOUT    = 10
WSS_CLOSE_TIMEOUT   = 5
WSS_MAX_MSG_SIZE    = 2 ** 20
WSS_INITIAL_BACKOFF = 2
WSS_MAX_BACKOFF     = 30

# Volatilite Etki Filtresi — watcher.py ile aynı eşik
STABLE_HEAVY_VOLATILE_RATIO = 0.10


# ─────────────────────────────────────────────────────────────────────────────
# VERİ YAPILARI
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OracleState:
    """
    [v2] Dinamik fiyat sözlüğü.

    Eski: arb_usd, weth_usd, wsteth_usd + iki türetilmiş oran → hardcode
    Yeni: prices = {"WETH": 2500.0, "WBTC": 65000.0, "USDC": 1.0, ...}

    oracle_hub tüm Chainlink güncelleme eventlerinde ilgili alanı günceller.
    burst_fire_engine, her hedef için anlık oranı: prices[coll] / prices[debt]
    şeklinde dinamik hesaplar.
    """
    prices:       Dict[str, float] = field(default_factory=dict)
    last_updated: float            = 0.0

    def get(self, symbol: str) -> float:
        """Güvenli fiyat okuma. Bilinmeyen sembol → 0.0"""
        return self.prices.get(symbol, 0.0)

    

    def is_ready(self, coll_symbol: str, debt_symbol: str) -> bool:
        """Her iki fiyat da mevcut mu?"""
        return self.prices.get(coll_symbol, 0.0) > 0 and \
               self.prices.get(debt_symbol, 0.0) > 0



@dataclass
class HFEntry:
    symbol: str
    asset: str
    amount: float
    lt: float
    price_key: str

@dataclass
class ClusterTarget:
    address:          str
    label:            str

    coll_token:       str   = ""
    coll_address:     str   = ""
    coll_atoken:      str   = ""
    coll_decimals:    int   = 18
    coll_amount:      float = 0.0
    coll_lt:          float = 0.0
    coll_bonus:       float = 0.0

    debt_token:       str   = ""
    debt_address:     str   = ""
    debt_vtoken:      str   = ""
    debt_decimals:    int   = 18
    debt_amount:      float = 0.0

    close_factor:     float = 0.5

    hf_collaterals: Dict[str, HFEntry] = field(default_factory=dict)
    hf_debts: Dict[str, HFEntry] = field(default_factory=dict)
    in_memory_hf:         float = 999.0
    estimated_profit_usd: float = 0.0
    capped_debt_amount:   float = 0.0

    def compute_hf(self, prices: Dict[str, float]) -> float:
        total_coll_usd_lt = 0.0
        total_debt_usd = 0.0
        
        for entry in self.hf_collaterals.values():
            price = prices.get(entry.price_key, 0.0)
            if price == 0.0 and entry.amount > 0:
                self.in_memory_hf = 999.0
                return self.in_memory_hf
            total_coll_usd_lt += entry.amount * price * entry.lt
            
        for entry in self.hf_debts.values():
            price = prices.get(entry.price_key, 0.0)
            if price == 0.0 and entry.amount > 0:
                self.in_memory_hf = 999.0
                return self.in_memory_hf
            total_debt_usd += entry.amount * price
            
        if total_debt_usd == 0.0:
            self.in_memory_hf = 999.0
        else:
            self.in_memory_hf = total_coll_usd_lt / total_debt_usd
            
        return self.in_memory_hf

    def recalculate(self, oracle: OracleState) -> None:
        if self.debt_amount <= 0:
            self.in_memory_hf = 999.0
            self.estimated_profit_usd = 0.0
            self.capped_debt_amount = 0.0
            return

        debt_price   = oracle.get(self.debt_token)
        covered_usd  = self.debt_amount * self.close_factor * debt_price
        
        # Kapasite kontrolü: seçili coll_token için RAM'de ne kadar bakiye varsa
        available_coll = self.hf_collaterals[self.coll_token].amount if self.coll_token in self.hf_collaterals else 0.0
        coll_price = oracle.get(self.coll_token)
        max_coll_usd = available_coll * coll_price
        
        # %0.05 güven marjı (safe margin: 0.9995) ekleyerek revert koruması
        max_coverable_usd = (max_coll_usd / (1 + self.coll_bonus)) * 0.9995
        
        if max_coverable_usd > 0 and covered_usd > max_coverable_usd:
            covered_usd = max_coverable_usd
            
        self.capped_debt_amount = covered_usd / debt_price if debt_price > 0 else 0.0
        
        gross        = covered_usd * self.coll_bonus
        flash_fee    = covered_usd * FLASH_LOAN_FEE
        slippage     = covered_usd * DEX_SLIPPAGE
        self.estimated_profit_usd = gross - flash_fee - slippage - 0.05
        
        self.compute_hf(oracle.prices)

@dataclass
class BurstState:
    target:            ClusterTarget
    bullet1_threshold: float = 1.0010
    bullet2_threshold: float = 1.0005
    bullet3_threshold: float = 1.0000
    bullet1_sent:      bool  = False
    bullet2_sent:      bool  = False
    bullet3_sent:      bool  = False
    confirmed:         bool  = False
    base_nonce:        int   = -1
    last_fire_ts:      float = 0.0

    def reset_thresholds(self) -> None:
        m1 = float(os.getenv("BURST_M1", "0.001"))
        m2 = float(os.getenv("BURST_M2", "0.0005"))
        self.bullet1_threshold = 1.0 + m1
        self.bullet2_threshold = 1.0 + m2
        self.bullet3_threshold = 1.0

        pass

    @property
    def all_sent(self) -> bool:
        return self.bullet1_sent and self.bullet2_sent and self.bullet3_sent

    @property
    def any_sent(self) -> bool:
        return self.bullet1_sent or self.bullet2_sent or self.bullet3_sent


class NonceManager:
    """
    Merkezi atomik nonce yöneticisi.

    Flash crash senaryosunda birden fazla hedefe aynı anda mermi atıldığında
    nonce çakışmasını önler. Her `reserve()` çağrısı atomik olarak bir sonraki
    nonce'u döndürür. İlk kullanımda RPC'den çekilen nonce ile başlatılır.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._current: int = -1       # -1 = henüz başlatılmadı
        self._initialized = False

    async def initialize(self, w3: AsyncWeb3, account: str) -> None:
        """RPC'den güncel nonce'u çekerek başlat. Sadece bir kez çalışır."""
        async with self._lock:
            if self._initialized:
                return
            self._current = await w3.eth.get_transaction_count(
                AsyncWeb3.to_checksum_address(account), "latest"
            )
            self._initialized = True
            logger.info("[NONCE] Başlangıç nonce = %d", self._current)

    async def reserve(self) -> int:
        """Atomik olarak bir sonraki nonce'u döndürür."""
        async with self._lock:
            if not self._initialized:
                raise RuntimeError("NonceManager henüz başlatılmadı!")
            nonce = self._current
            self._current += 1
            return nonce

    async def sync(self, w3: AsyncWeb3, account: str) -> None:
        """RPC'den güncel nonce ile yeniden senkronize et (revert sonrası)."""
        async with self._lock:
            self._current = await w3.eth.get_transaction_count(
                AsyncWeb3.to_checksum_address(account), "latest"
            )
            logger.info("[NONCE] Yeniden senkronize = %d", self._current)


@dataclass
class ClusterSniperState:
    """Tüm kümenin ortak çalışma zamanı durumu. [v3 — Multi-Chain]"""
    oracle:             OracleState
    burst_states:       List[BurstState]
    chain_cfg:          Optional[ChainConfig] = None
    chain_tag:          str   = "ARB"
    chain_id:           int   = 42161
    dry_run:            bool  = False

    # Merkezi nonce yöneticisi — tüm bullet'lar paylaşır
    nonce_manager:      NonceManager = field(default_factory=NonceManager)

    # _build_chain_data() sonuçları — runtime'da set edilir
    reserves:           List[Dict]          = field(default_factory=list)
    feed_map:           Dict[str, List[str]] = field(default_factory=dict)
    all_feed_addresses: List[str]           = field(default_factory=list)
    all_aave_tokens:    List[str]           = field(default_factory=list)

    def get_burst(self, address: str) -> Optional[BurstState]:
        for bs in self.burst_states:
            if bs.target.address.lower() == address.lower():
                return bs
        return None


# ─────────────────────────────────────────────────────────────────────────────
# WEB3 YARDIMCILARI [DEĞİŞMEDİ]
# ─────────────────────────────────────────────────────────────────────────────

async def build_w3(rpc_url: str, label: str = "") -> AsyncWeb3:
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    if not await w3.is_connected():
        raise ConnectionError(f"RPC bağlantısı başarısız ({label}): {rpc_url[:60]}")
    chain_id = await w3.eth.chain_id
    logger.info("[W3] %s bağlandı | Chain ID: %d", label or rpc_url[:30], chain_id)
    return w3


def _decode_topic_int256(topic_hex: str) -> int:
    raw = bytes.fromhex(topic_hex.lstrip("0x").zfill(64))
    return int.from_bytes(raw, byteorder="big", signed=True)


# ─────────────────────────────────────────────────────────────────────────────
# ON-CHAIN VERİ ÇEKME
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_user_reserve(
    w3:                    AsyncWeb3,
    asset:                 str,
    user:                  str,
    data_provider_address: str,
) -> Tuple[int, int, bool]:
    """
    PoolDataProvider.getUserReserveData(asset, user) ile
    (currentATokenBalance, currentVariableDebt, usageAsCollateralEnabled) döndürür.

    [v3] data_provider_address artık parametre olarak alınır —
    her zincirin kendi DataProvider adresi ChainConfig'den gelir.
    [v5] usageAsCollateralEnabled de döndürülür — RAM HF hesabı
    için collateral-enabled olmayan varlıklar hf_collaterals'a eklenmez.
    """
    cs = AsyncWeb3.to_checksum_address
    dp = w3.eth.contract(address=cs(data_provider_address), abi=DATA_PROVIDER_ABI)
    try:
        result = await dp.functions.getUserReserveData(
            cs(asset), cs(user)
        ).call()
        atoken_bal    = result[0]  # currentATokenBalance
        variable_debt = result[2]  # currentVariableDebt
        coll_enabled  = result[8]  # usageAsCollateralEnabled
        return atoken_bal, variable_debt, coll_enabled
    except Exception as exc:
        logger.error(
            "[FETCH] getUserReserveData başarısız | asset=%s user=%s: %s",
            asset[:12], user[:12], exc,
        )
        return 0, 0, False


async def init_all_targets(w3: AsyncWeb3, cluster: ClusterSniperState) -> None:
    """
    [v4] PoolDataProvider.getUserReserveData ile pozisyon yükler.
    Auto-pair discovery: En büyük borç ve teminat tokenını otomatik seçer.
    """
    dp_addr  = cluster.chain_cfg.data_provider_address
    reserves = cluster.reserves

    logger.info("[INIT] %d hedefin pozisyonları yükleniyor...", len(cluster.burst_states))

    for bs in cluster.burst_states:
        t = bs.target

        for r in reserves:
            c_raw, d_raw, coll_enabled = await fetch_user_reserve(w3, r["asset"], t.address, dp_addr)

            if c_raw > 0 and coll_enabled:
                amount = c_raw / (10 ** r["decimals"])
                t.hf_collaterals[r["symbol"]] = HFEntry(
                    symbol=r["symbol"], asset=r["asset"], amount=amount,
                    lt=r["lt"], price_key=r["symbol"]
                )

            if d_raw > 0:
                amount = d_raw / (10 ** r["decimals"])
                t.hf_debts[r["symbol"]] = HFEntry(
                    symbol=r["symbol"], asset=r["asset"], amount=amount,
                    lt=0.0, price_key=r["symbol"]
                )

        # ── Auto-Pair Discovery ────────────────────────────────────────────
        # Henüz coll/debt token belirlenmemişse (targets.json kaynaklı),
        # on-chain veriden en büyük USD değerli çifti otomatik seç.
        if not t.coll_token and t.hf_collaterals:
            best_coll_sym = max(
                t.hf_collaterals,
                key=lambda s: t.hf_collaterals[s].amount * cluster.oracle.prices.get(s, 0.0),
            )
            best_coll_entry = t.hf_collaterals[best_coll_sym]
            coll_res = _resolve_reserve(best_coll_sym, reserves)
            if coll_res:
                t.coll_token    = best_coll_sym
                t.coll_address  = coll_res["asset"]
                t.coll_atoken   = coll_res["atoken"]
                t.coll_decimals = coll_res["decimals"]
                t.coll_lt       = coll_res["lt"]
                t.coll_bonus    = coll_res.get("bonus", LIQUIDATION_BONUS_MAP.get(best_coll_sym, DEFAULT_BONUS))

        if not t.debt_token and t.hf_debts:
            best_debt_sym = max(
                t.hf_debts,
                key=lambda s: t.hf_debts[s].amount * cluster.oracle.prices.get(s, 0.0),
            )
            best_debt_entry = t.hf_debts[best_debt_sym]
            debt_res = _resolve_reserve(best_debt_sym, reserves)
            if debt_res:
                t.debt_token    = best_debt_sym
                t.debt_address  = debt_res["asset"]
                t.debt_vtoken   = debt_res["vtoken"]
                t.debt_decimals = debt_res["decimals"]

        if t.coll_token and t.debt_token:
            t.label = f"{t.debt_token}/{t.coll_token}"

        # ── eMode LT Sync ─────────────────────────────────────────────
        # load_cluster_from_targets_json, Aave HF'den effective LT
        # turetip target.coll_lt'ye set etti. Ancak compute_hf()
        # hf_collaterals[sym].lt kullanir. Burada senkronize ediyoruz.
        if (t.coll_token and t.coll_token in t.hf_collaterals
                and t.coll_lt != t.hf_collaterals[t.coll_token].lt):
            t.hf_collaterals[t.coll_token] = HFEntry(
                symbol=t.hf_collaterals[t.coll_token].symbol,
                asset=t.hf_collaterals[t.coll_token].asset,
                amount=t.hf_collaterals[t.coll_token].amount,
                lt=t.coll_lt,
                price_key=t.hf_collaterals[t.coll_token].price_key,
            )

        # ── Auto-Pair Token Adresi Doğrulaması ─────────────────────────
        # Pozisyon kapanmış veya resolve başarısız olduysa adresler "" kalır.
        # Geçersiz adresle Tx inşa etmeye çalışmak 'Unknown format' hatasına
        # yol açar. Bu hedefi ölü ilan et ve atla.
        label = t.label or t.address[:12]
        _invalid_addr = False
        if not t.debt_address or not AsyncWeb3.is_address(t.debt_address):
            logger.warning(
                "[INIT] %s | debt_address geçersiz ('%s') — hedef ölü ilan edildi.",
                label, t.debt_address,
            )
            _invalid_addr = True
        if not t.coll_address or not AsyncWeb3.is_address(t.coll_address):
            logger.warning(
                "[INIT] %s | coll_address geçersiz ('%s') — hedef ölü ilan edildi.",
                label, t.coll_address,
            )
            _invalid_addr = True
        if _invalid_addr:
            bs.confirmed = True
            continue

        coll_raw = int(t.hf_collaterals[t.coll_token].amount * (10 ** t.coll_decimals)) if t.coll_token in t.hf_collaterals else 0
        debt_raw = int(t.hf_debts[t.debt_token].amount * (10 ** t.debt_decimals)) if t.debt_token in t.hf_debts else 0

        if coll_raw == 0 or debt_raw == 0:
            logger.warning(
                "[INIT] %s (%s): Teminat veya borç SIFIR — atlanıyor. "
                "coll_raw=%d debt_raw=%d",
                t.label or t.address[:12], t.address[:12], coll_raw, debt_raw,
            )
            bs.confirmed = True
            continue

        t.coll_amount = coll_raw / (10 ** t.coll_decimals)
        t.debt_amount = debt_raw / (10 ** t.debt_decimals)

        logger.info(
            "[INIT] %-14s | Teminat: %.6f %s | Borç: %.6f %s",
            t.label, t.coll_amount, t.coll_token,
            t.debt_amount, t.debt_token,
        )

    # ── Stable-Heavy filtresi (Volatilite Etki Filtresi) ─────────────────────
    logger.info("[INIT] Stable-Heavy volatilite filtresi uygulanıyor...")
    for bs in cluster.burst_states:
        if bs.confirmed:
            continue
        t = bs.target
        total_usd    = 0.0
        volatile_usd = 0.0
        for entry in list(t.hf_collaterals.values()) + list(t.hf_debts.values()):
            price = cluster.oracle.prices.get(entry.price_key, 0.0)
            pos_usd = entry.amount * price if price > 0 else 0.0
            total_usd += pos_usd
            if ASSET_CLASS.get(entry.symbol, "ALT") != "STABLE":
                volatile_usd += pos_usd

        if total_usd > 0:
            volatile_ratio = volatile_usd / total_usd
            if volatile_ratio < STABLE_HEAVY_VOLATILE_RATIO:
                label = t.label or t.address[:12]
                logger.warning(
                    f"[INIT] STABLE-HEAVY ATILDI | {label:<14} | {t.address[:12]} | "
                    f"Toplam: ${total_usd:,.0f} | Volatil: ${volatile_usd:,.0f} ({volatile_ratio*100:.1f}%) | "
                    f"Fiyat hareketiyle tasfiye imkansız → atlanıyor"
                )
                bs.confirmed = True

    logger.info("[INIT] Tüm pozisyonlar yüklendi.")


# ─────────────────────────────────────────────────────────────────────────────
# TX İMZALAMA VE GÖNDERİM [DEĞİŞMEDİ]
# ─────────────────────────────────────────────────────────────────────────────

async def build_and_sign_bullet(
    w3:           AsyncWeb3,
    target:       ClusterTarget,
    nonce:        int,
    gas_limit:    int,
    base_fee:     int,
    gas_mult:     float,
    bullet_index: int,
    private_key:  str,
    executor:     Optional[str] = None,
) -> Optional[object]:
    """
    Tek bir bullet için TX oluşturur ve imzalar.
    Bullet'lar farklı gas premium'larıyla (1.05/1.10/1.20) inşa edilir.

    İKİ YOLLU LİKİDASYON:
      executor doluysa → AaveFlashloanSniper.sol kontratının executeSnipe()
                         fonksiyonu çağrılır. Bu yol Balancer flashloan ile
                         borç tokenini bedavaya alıp, Aave liquidationCall
                         yapıp, ganimetleri Uniswap'ta swap edip kârı sahibine
                         geri gönderir. Cüzdanda borç tokeni olmasına GEREK YOK.

      executor boşsa  → Doğrudan Aave Pool'da liquidationCall yapılır.
                         Bu yol cüzdanda yeterli borç tokeni gerektirir.

    .sol Kontrat Fonksiyon İmzası:
      executeSnipe(
          address _collateralAsset,   ← Aave'den alınacak teminat token
          address _debtAsset,         ← Borç token (flashloan ile alınacak)
          address _targetUser,        ← Tasfiye edilecek cüzdan
          uint256 _debtToCover,       ← Kapatılacak borç miktarı (wei)
          uint24  _uniswapFeeTier,    ← Uniswap V3 swap havuz komisyonu
          uint256 _minProfitAmount    ← Minimum kâr (anti-sandwich koruma)
      )
    """
    cs           = AsyncWeb3.to_checksum_address
    gas_premiums = [1.05, 1.10, 1.20]
    gas_premium  = gas_premiums[min(bullet_index, 2)]
    max_fee      = int(base_fee * gas_mult * gas_premium)
    priority_fee = int(0.01e9 * gas_premium)

    debt_to_cover_wei = int(
        target.capped_debt_amount * (10 ** target.debt_decimals)
    )

    account = AsyncWeb3.to_checksum_address(
        w3.eth.account.from_key(private_key).address
    )

    try:
        if executor:
            # ─────────────────────────────────────────────────────────────
            # FLASHLOAN YOLU: AaveFlashloanSniper.sol → executeSnipe()
            # ─────────────────────────────────────────────────────────────
            # ABI — .sol kontratındaki executeSnipe() fonksiyonuyla BİREBİR eşleşir
            exec_abi = [{
                "inputs": [
                    {"name": "_collateralAsset", "type": "address"},
                    {"name": "_debtAsset",       "type": "address"},
                    {"name": "_targetUser",      "type": "address"},
                    {"name": "_debtToCover",     "type": "uint256"},
                    {"name": "_uniswapFeeTier",  "type": "uint24"},
                    {"name": "_minProfitAmount", "type": "uint256"},
                ],
                "name": "executeSnipe",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            }]

            # Parite bazlı Uniswap V3 fee tier seç
            # Ör: WBTC/USDC → 500 (%0.05), ARB/WETH → 3000 (%0.30)
            pair_key = f"{target.coll_token}/{target.debt_token}"
            fee_tier = UNISWAP_FEE_TIER_MAP.get(pair_key, DEFAULT_UNISWAP_FEE_TIER)

            # Minimum kâr koruması — ŞİMDİLİK DEVRE DIŞI
            # Mevcut hedefler $20-50 kâr seviyesinde, sandwich botlar bu
            # kadar düşük miktarlarla ilgilenmez. minProfitAmount > 0 koymak
            # gereksiz revert riski yaratır. Büyük cüzdanlara geçince aktif edilecek.
            # Kontrat hâlâ require(amountOut >= amountToRepay) ile flashloan
            # borcunun karşılandığını garanti eder.
            min_profit_amount = 0

            exec_contract = w3.eth.contract(address=cs(executor), abi=exec_abi)
            tx = await exec_contract.functions.executeSnipe(
                cs(target.coll_address),    # _collateralAsset
                cs(target.debt_address),    # _debtAsset
                cs(target.address),         # _targetUser
                debt_to_cover_wei,          # _debtToCover
                fee_tier,                   # _uniswapFeeTier (uint24)
                min_profit_amount,          # _minProfitAmount (uint256)
            ).build_transaction({
                "from": account, "nonce": nonce,
                "gas": gas_limit, "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": priority_fee,
                "chainId": _ACTIVE_CHAIN_ID,
            })

            logger.info(
                "[TX] executeSnipe hazır | %s | borç=%d wei | fee=%d | minProfit=%d",
                target.label, debt_to_cover_wei, fee_tier, min_profit_amount,
            )
        else:
            # ─────────────────────────────────────────────────────────────
            # DİREKT YOLU: Flashloan olmadan Aave Pool.liquidationCall()
            # Cüzdanda yeterli borç tokeni olmalı!
            # ─────────────────────────────────────────────────────────────
            pool = w3.eth.contract(address=cs(_ACTIVE_POOL_ADDR), abi=LIQUIDATION_CALL_ABI)
            tx = await pool.functions.liquidationCall(
                cs(target.coll_address),
                cs(target.debt_address),
                cs(target.address),
                debt_to_cover_wei,
                False,
            ).build_transaction({
                "from": account, "nonce": nonce,
                "gas": gas_limit, "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": priority_fee,
                "chainId": _ACTIVE_CHAIN_ID,
            })

        return w3.eth.account.sign_transaction(tx, private_key)
    except Exception as exc:
        logger.error("[TX] TX inşa hatası (bullet %d, %s): %s",
                     bullet_index, target.label, exc)
        return None


async def send_bullet(
    w3:           AsyncWeb3,
    signed_tx:    object,
    target:       ClusterTarget,
    bullet_index: int,
    bs:           BurstState,
    dry_run:      bool = False,
) -> Optional[str]:
    labels = ["🔫 ÖNCÜ", "🔥 SICAK", "💀 ÖLÜMCÜL"]
    label  = labels[min(bullet_index, 2)]

    if dry_run:
        sim_hash = f"0xDRY_RUN_BULLET{bullet_index}_{target.address[:8]}"
        logger.warning("[DRY] %s bullet %d | %s | %.4f %s",
                       label, bullet_index, target.label,
                       target.debt_amount * target.close_factor, target.debt_token)
        return sim_hash

    try:
        t0 = time.perf_counter()
        tx_hash = await w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        tx_hex  = tx_hash.hex()
        t1 = time.perf_counter()
        tx_ms = (t1 - t0) * 1000.0
        logger.warning("[FIRE] %s | %s | Bullet %d | TX: %s | (TX Ağ Gönderim: %.1f ms)",
                       label, target.label, bullet_index, tx_hex, tx_ms)
        tg.send(
            f"{label} <b>[{target.label}] Bullet {bullet_index + 1}/3</b>\n"
            f"👤 <code>{target.address}</code>\n"
            f"💎 {target.debt_amount * target.close_factor:.6f} {target.debt_token}\n"
            f"🔗 <code>{tx_hex}</code>"
        )
        return tx_hex
    except Exception as exc:
        logger.error("[FIRE] Bullet %d gönderim hatası (%s): %s",
                     bullet_index, target.label, exc)
        return None


async def monitor_receipt(
    w3:           AsyncWeb3,
    tx_hex:       str,
    bs:           BurstState,
    bullet_index: int,
) -> None:
    try:
        receipt = await asyncio.wait_for(
            w3.eth.wait_for_transaction_receipt(
                bytes.fromhex(tx_hex.lstrip("0x")), poll_latency=0.3
            ),
            timeout=30.0,
        )
        target = bs.target
        if receipt["status"] == 1:
            bs.confirmed = True
            logger.warning(
                "[✅] LİKİDASYON ONAYLANDI! | %s | Gas: %d | Blok: %d",
                target.label, receipt["gasUsed"], receipt["blockNumber"],
            )
            tg.send(
                f"✅ <b>LİKİDASYON ONAYLANDI — {target.label}</b>\n"
                f"👤 <code>{target.address}</code>\n"
                f"⚡ Bullet: {bullet_index + 1}/3\n"
                f"💰 Kâr: <code>${target.estimated_profit_usd:,.2f}</code>\n"
                f"⛽ Gas: {receipt['gasUsed']:,} | Blok: {receipt['blockNumber']}\n"
                f"🔗 <code>{tx_hex}</code>"
            )
        else:
            logger.info("[REVERT] Bullet %d | %s | Blok: %d | ~$0.02",
                        bullet_index, target.label, receipt["blockNumber"])
    except asyncio.TimeoutError:
        logger.warning("[TIMEOUT] Bullet %d | %s", bullet_index, bs.target.label)
    except Exception as exc:
        logger.debug("[RECEIPT] Hata (bullet %d, %s): %s", bullet_index, bs.target.label, exc)


async def execute_burst(
    w3:            AsyncWeb3,
    bs:            BurstState,
    bullet_index:  int,
    private_key:   str,
    gas_limit:     int,
    gas_mult:      float,
    executor:      Optional[str],
    dry_run:       bool,
    nonce_manager: Optional[NonceManager] = None,
) -> None:
    """
    Tek bir bullet'ı imzalar ve gönderir.
    [v5] Merkezi NonceManager ile nonce çakışması önlenir.
    """
    target = bs.target
    if bs.confirmed:
        return

    try:
        account = AsyncWeb3.to_checksum_address(
            w3.eth.account.from_key(private_key).address
        )
    except Exception as e:
        logger.error("[BURST] Private Key hatası: .env dosyasındaki SNIPER_PRIVATE_KEY hatalı veya eksik. Lütfen geçerli bir key girin.")
        return

    # ── Merkezi nonce yönetimi ─────────────────────────────────────────────
    if nonce_manager is not None:
        # NonceManager henüz başlatılmadıysa ilk kullanımda başlat
        try:
            await nonce_manager.initialize(w3, account)
        except Exception as exc:
            logger.error("[BURST] NonceManager başlatılamadı: %s", exc)
            return
        try:
            nonce = await nonce_manager.reserve()
        except Exception as exc:
            logger.error("[BURST] Nonce rezerve edilemedi (%s): %s", target.label, exc)
            return
    else:
        # Fallback: eski per-target nonce (geriye uyumluluk)
        if bs.base_nonce == -1:
            try:
                bs.base_nonce = await w3.eth.get_transaction_count(account, "latest")
            except Exception as exc:
                logger.error("[BURST] Nonce okunamadı (%s): %s", target.label, exc)
                return
        nonce = bs.base_nonce + bullet_index

    # ── Tx İnşa İzolasyonu ─────────────────────────────────────────────────
    # Hatalı veri (boş adres, geçersiz format) geldiğinde tüm döngüyü
    # çökertmek yerine sadece bu hedefi atla ve diğerlerine devam et.
    try:
        try:
            block    = await w3.eth.get_block("latest")
            base_fee = block.get("baseFeePerGas", 100_000_000)
        except Exception:
            base_fee = 100_000_000

        t_b0 = time.perf_counter()
        signed = await build_and_sign_bullet(
            w3, target, nonce, gas_limit, base_fee, gas_mult,
            bullet_index, private_key, executor,
        )
        t_b1 = time.perf_counter()
        build_ms = (t_b1 - t_b0) * 1000.0
        if not signed:
            return
    except Exception as e:
        logger.error(f"Tx inşa hatası atlandı: {e}")
        return
        
    logger.info("[BURST] %s | Mermi inşa ve imzalama: %.1f ms | nonce=%d", target.label, build_ms, nonce)

    tx_hex = await send_bullet(w3, signed, target, bullet_index, bs, dry_run)
    if not tx_hex:
        return

    if not dry_run:
        asyncio.create_task(
            monitor_receipt(w3, tx_hex, bs, bullet_index),
            name=f"receipt-{target.label}-b{bullet_index}",
        )

    bs.last_fire_ts = time.time()


# ─────────────────────────────────────────────────────────────────────────────
# ORACLE HUB [v2 — Dinamik Feed Aboneliği]
# ─────────────────────────────────────────────────────────────────────────────

async def oracle_hub(
    state:       ClusterSniperState,
    wss_url:     str,
    price_queue: asyncio.Queue,
) -> None:
    """
    Motor 1'in gözleri: CHAIN_RESERVES'dan toplanan TÜM Chainlink
    feed'lerini tek bir WSS aboneliğiyle dinler.

    FEED_MAP'teki tüm benzersiz feed adresleri → ALL_FEED_ADDRESSES

    Her AnswerUpdated event'inde:
      1. Hangi feed → hangi token: FEED_MAP ile çöz
      2. oracle.prices[token_symbol] = yeni_fiyat
      3. price_queue'ya tüm fiyatların anlık kopyasını at

    burst_fire_engine, snapshot'taki prices dict'ten her hedef için
    coll_usd/debt_usd oranını dinamik hesaplar.
    """
    tag     = "ORACLE-HUB"
    backoff = WSS_INITIAL_BACKOFF

    # Zincire özgü feed verileri state'ten okunur
    FEED_MAP           = state.feed_map
    ALL_FEED_ADDRESSES = state.all_feed_addresses

    logger.info("[%s] %d Chainlink feed dinleniyor: %s",
                tag, len(ALL_FEED_ADDRESSES),
                [FEED_MAP.get(a.lower(), "?") for a in ALL_FEED_ADDRESSES])

    while True:
        try:
            async with websockets.connect(
                wss_url,
                ping_interval = WSS_PING_INTERVAL,
                ping_timeout  = WSS_PING_TIMEOUT,
                close_timeout = WSS_CLOSE_TIMEOUT,
                max_size      = WSS_MAX_MSG_SIZE,
            ) as ws:

                # Tüm benzersiz feed adresleri tek abonelikte
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1,
                    "method":  "eth_subscribe",
                    "params":  [
                        "logs",
                        {
                            "address": ALL_FEED_ADDRESSES,
                            "topics":  [CHAINLINK_ANSWER_UPDATED],
                        },
                    ],
                }))

                raw_resp = await asyncio.wait_for(ws.recv(), timeout=10.0)
                resp     = json.loads(raw_resp)
                if "error" in resp:
                    raise ValueError(f"eth_subscribe hatası: {resp['error']}")

                sub_id = resp.get("result", "")
                logger.info("[%s] ✅ Bağlandı. %d feed | sub_id=%s...",
                            tag, len(ALL_FEED_ADDRESSES), sub_id[:12])
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

                    result     = params.get("result", {})
                    event_addr = result.get("address", "").lower()
                    topics     = result.get("topics", [])

                    # FEED_MAP'ten hangi token'ın güncellendiğini bul
                    token_symbols = FEED_MAP.get(event_addr)
                    if not token_symbols or len(topics) < 2:
                        continue

                    try:
                        raw_price = _decode_topic_int256(topics[1])
                        price_usd = raw_price / CHAINLINK_DECIMALS
                        
                        # Eğer feed wstETH/ETH veya weETH/ETH (18-dec) ise price milyonların üstüne çıkar!
                        if price_usd > 10**6:
                            weth_price = state.oracle.prices.get("WETH", 2500.0)
                            price_usd = (raw_price / 10**18) * weth_price
                            
                    except Exception as dec_exc:
                        logger.debug("[%s] Decode hatası: %s", tag, dec_exc)
                        continue

                    if price_usd <= 0:
                        continue

                    # Fiyatı güncelle
                    for token_symbol in token_symbols:
                        state.oracle.prices[token_symbol] = price_usd
                        logger.debug("[%s] %s = $%.4f", tag, token_symbol, price_usd)
                    state.oracle.last_updated = time.time()

                    # Kuyruğa tüm güncel fiyatların kopyasını at
                    # burst_fire_engine bu dict'ten ratio'ları hesaplar
                    if price_queue.qsize() >= 64:
                        try:
                            price_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    try:
                        price_queue.put_nowait({
                            "prices": dict(state.oracle.prices),
                            "ts":     state.oracle.last_updated,
                        })
                    except asyncio.QueueFull:
                        pass

        except asyncio.CancelledError:
            logger.info("[%s] Task iptal edildi.", tag)
            return
        except Exception as exc:
            logger.warning("[%s] WSS koptu: %s. %ds reconnect...", tag, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WSS_MAX_BACKOFF)


# ─────────────────────────────────────────────────────────────────────────────
# STATE WATCHER [v2 — Dinamik Token Adresleri]
# ─────────────────────────────────────────────────────────────────────────────

async def state_change_watcher(
    state:        ClusterSniperState,
    w3_alchemy:   AsyncWeb3,
    wss_url:      str,
    trigger_lock: asyncio.Lock,
) -> None:
    tag     = "STATE-WATCHER"
    backoff = WSS_INITIAL_BACKOFF

    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    def _addr_to_topic(addr: str) -> str:
        return "0x" + "0" * 24 + addr.lower().lstrip("0x")

    # Zincire özgü Aave token adresleri state'ten okunur
    ALL_AAVE_TOKENS = state.all_aave_tokens
    RESERVES        = state.reserves

    target_topic_map: Dict[str, List[BurstState]] = {}
    for bs in state.burst_states:
        if not bs.confirmed:
            topic = _addr_to_topic(bs.target.address)
            target_topic_map.setdefault(topic, []).append(bs)

    logger.info("[%s] %d hedef, %d Aave token Transfer izlemesi.",
                tag, len(target_topic_map), len(ALL_AAVE_TOKENS))

    while True:
        try:
            async with websockets.connect(
                wss_url,
                ping_interval=10,
                ping_timeout=10,
                max_size=2**20,
            ) as ws:
                logger.info("[%s] WSS Bağlandı.", tag)
                
                sub_request = {
                    "id": 2,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": ALL_AAVE_TOKENS,
                            "topics": [transfer_topic]
                        }
                    ]
                }
                await ws.send(json.dumps(sub_request))
                
                async for message in ws:
                    data = json.loads(message)
                    if "params" not in data:
                        continue
                        
                    result = data["params"]["result"]
                    topics = result.get("topics", [])
                    if not topics or len(topics) < 3:
                        continue
                        
                    from_topic = topics[1]
                    to_topic = topics[2]
                    
                    affected = {}
                    if from_topic in target_topic_map:
                        for bs in target_topic_map[from_topic]:
                            affected[bs.target.address] = bs
                    if to_topic in target_topic_map:
                        for bs in target_topic_map[to_topic]:
                            affected[bs.target.address] = bs
                            
                    if not affected:
                        continue
                        
                    for bs in affected.values():
                        if bs.confirmed:
                            continue
                        t = bs.target
                        
                        ev_addr = result.get("address", "").lower()
                        matched_res = None
                        for r in RESERVES:
                            if ev_addr == r["atoken"].lower() or ev_addr == r["vtoken"].lower():
                                matched_res = r
                                break
                                
                        if not matched_res:
                            continue

                        c_raw, d_raw, _ = await fetch_user_reserve(
                            w3_alchemy, matched_res["asset"], t.address,
                            state.chain_cfg.data_provider_address,
                        )
                        
                        sym = matched_res["symbol"]
                        if c_raw > 0:
                            amount = c_raw / (10 ** matched_res["decimals"])
                            t.hf_collaterals[sym] = HFEntry(
                                symbol=sym, asset=matched_res["asset"], amount=amount, 
                                lt=matched_res["lt"], price_key=sym
                            )
                        elif sym in t.hf_collaterals:
                            del t.hf_collaterals[sym]
                            
                        if d_raw > 0:
                            amount = d_raw / (10 ** matched_res["decimals"])
                            t.hf_debts[sym] = HFEntry(
                                symbol=sym, asset=matched_res["asset"], amount=amount, 
                                lt=0.0, price_key=sym
                            )
                        elif sym in t.hf_debts:
                            del t.hf_debts[sym]

                        if sym == t.coll_token:
                            t.coll_amount = c_raw / (10 ** t.coll_decimals)
                        if sym == t.debt_token:
                            t.debt_amount = d_raw / (10 ** t.debt_decimals)

                        old_hf = t.in_memory_hf
                        t.compute_hf(state.oracle.prices)
                        t.recalculate(state.oracle)
                        bs.reset_thresholds()
                        
                        logger.info(
                            "[%s] %s | HF: %.6f → %.6f | Kâr: $%.2f",
                            tag, t.label, old_hf, t.in_memory_hf,
                            t.estimated_profit_usd,
                        )

        except Exception as e:
            logger.error("[%s] WSS Hata: %s | Yeniden bağlanılıyor...", tag, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WSS_MAX_BACKOFF)

# ─────────────────────────────────────────────────────────────────────────────
# Motor 1: Burst Fire Engine (Kör Nişancı)
# ─────────────────────────────────────────────────────────────────────────────

async def burst_fire_engine(
    state:       ClusterSniperState,
    w3:          AsyncWeb3,
    private_key: str,
    executor:    str,
) -> None:
    tag = "Motor1(Burst)"
    if state.dry_run:
        logger.info("[%s] DRY_RUN = True. Tx atılmayacak.", tag)

    w3_alchemy = AsyncWeb3(AsyncHTTPProvider(state.chain_cfg.rpc_url))
    base_gas_limit = 2000000

    while True:
        try:
            prices = dict(state.oracle.prices)
            nm = state.nonce_manager  # Merkezi nonce yöneticisi

            for bs in state.burst_states:
                if bs.confirmed:
                    continue

                t = bs.target

                # ── Race Condition Koruması (Token Adresi) ─────────────
                # targets_json_watcher hedefi listeye ekler ama
                # init_all_targets 4-5 sn sürer. Bu boşlukta debt_token
                # ve coll_token henüz "" (boş) kalır. Boş adresle TX
                # inşa etmeye çalışmak çökmeye yol açar → atla.
                if not t.debt_token or not t.coll_token:
                    continue

                current_hf = t.compute_hf(prices)

                # ── Race Condition Koruması (HF) ───────────────────────
                # HF == 0.0 veya None → oracle henüz güncellenmemiş demek.
                # Bu durumu tasfiye eşiğinin altı sanıp ateş etmeyi önle.
                if current_hf is None or current_hf == 0.0:
                    continue

                if current_hf > bs.bullet1_threshold:
                    continue

                if not bs.bullet1_sent and current_hf <= bs.bullet1_threshold:
                    logger.warning(
                        "[%s] 🔫 BULLET 1 ÖNCÜ | %-14s | HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet1_threshold
                    )
                    bs.bullet1_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 0, private_key, base_gas_limit, 1.0, executor, state.dry_run, nm),
                        name=f"burst-{t.label}-b0",
                    )

                if (bs.bullet1_sent and not bs.bullet2_sent and current_hf <= bs.bullet2_threshold):
                    logger.warning(
                        "[%s] 🔥 BULLET 2 SICAK | %-14s | HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet2_threshold,
                    )
                    bs.bullet2_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 1, private_key, base_gas_limit, 1.0, executor, state.dry_run, nm),
                        name=f"burst-{t.label}-b1",
                    )

                if (bs.bullet2_sent and not bs.bullet3_sent and current_hf <= bs.bullet3_threshold):
                    logger.warning(
                        "[%s] 💀 BULLET 3 ÖLÜMCÜL | %-14s | HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet3_threshold,
                    )
                    bs.bullet3_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 2, private_key, base_gas_limit, 1.0, executor, state.dry_run, nm),
                        name=f"burst-{t.label}-b2",
                    )

            await asyncio.sleep(0.001)

        except Exception as e:
            logger.error("[%s] Motor hatası: %s", tag, e)
            await asyncio.sleep(1)


async def motor2_hf_monitor(
    state:         ClusterSniperState,
    w3_fast:       AsyncWeb3,
    private_key:   str,
    gas_limit:     int,
    gas_mult:      float,
    executor:      Optional[str],
    poll_interval: float = 0.4,
) -> None:
    """
    Tüm hedeflerin HF'sini tek Multicall ile okur.
    HF < 1.00 → burst başlat veya sigorta mermisi. [DEĞİŞMEDİ]
    """
    tag = "MOTOR2-HF"
    logger.info("[%s] Multicall HF polling başladı (%.1fs)", tag, poll_interval)

    pool_addr  = state.chain_cfg.pool_address
    cs         = AsyncWeb3.to_checksum_address
    multicall  = w3_fast.eth.contract(address=cs(MULTICALL3_ADDR), abi=MULTICALL3_ABI)
    pool_dummy = w3_fast.eth.contract(address=cs(pool_addr),       abi=GET_ACCOUNT_DATA_ABI)

    while True:
        await asyncio.sleep(poll_interval)

        active = [bs for bs in state.burst_states if not bs.confirmed]
        if not active:
            logger.info("[%s] Tüm hedefler tamamlandı — Motor 2 durdu.", tag)
            return

        try:
            calls = [
                (cs(pool_addr),
                 pool_dummy.encode_abi("getUserAccountData",
                                       args=[cs(bs.target.address)]))
                for bs in active
            ]
            t_mc0 = time.perf_counter()
            results = await multicall.functions.tryAggregate(False, calls).call()
            t_mc1 = time.perf_counter()
            mc_ms = (t_mc1 - t_mc0) * 1000.0

            for i, (success, return_data) in enumerate(results):
                if not success or not return_data:
                    continue

                bs = active[i]
                try:
                    decoded = abi_decode(
                        ["uint256","uint256","uint256","uint256","uint256","uint256"],
                        return_data,
                    )
                    hf_wei = decoded[5]
                    if hf_wei >= (2 ** 256 - 1):
                        continue
                    hf = hf_wei / WAD
                except Exception:
                    continue

                # ── Race Condition Koruması ─────────────────────────────
                # HF == 0.0 → on-chain veri henüz geçerli değil, atla.
                if hf == 0.0:
                    continue

                logger.debug("[%s] %s | HF: %.6f", tag, bs.target.label, hf)

                if hf >= 1.10 and bs.any_sent and not getattr(state, "benchmark", False):
                    logger.info("[%s] 🟢 %s HF güvenli (%.4f) — tasfiye tamamlandı.",
                                tag, bs.target.label, hf)
                    bs.confirmed = True
                    continue

                benchmark_limit = 999.0 if getattr(state, "benchmark", False) else 1.00
                if hf < benchmark_limit:
                    t = bs.target

                    # ── Race Condition Koruması (Token Adresi) ─────────
                    # debt_token/coll_token henüz init edilmediyse TX
                    # inşa etme — boş adresle çökmeyi önle.
                    if not t.debt_token or not t.coll_token:
                        continue

                    if bs.all_sent and not getattr(state, "benchmark", False):
                        continue

                    logger.warning(
                        "[%s] 🎯 HF=%.4f < 1.00 ONAYLANDI | %s | b1=%s b2=%s b3=%s (Multicall gecikmesi: %.1f ms)",
                        tag, hf, t.label,
                        bs.bullet1_sent, bs.bullet2_sent, bs.bullet3_sent, mc_ms
                    )

                    for bullet_idx in range(3):
                        flags = [bs.bullet1_sent, bs.bullet2_sent, bs.bullet3_sent]
                        if not flags[bullet_idx] or getattr(state, "benchmark", False):
                            if bullet_idx == 0:
                                bs.bullet1_sent = True
                            elif bullet_idx == 1:
                                bs.bullet2_sent = True
                            else:
                                bs.bullet3_sent = True

                            asyncio.create_task(
                                execute_burst(
                                    w3_fast, bs, bullet_idx, private_key,
                                    gas_limit, gas_mult * 1.1,
                                    executor, state.dry_run,
                                    state.nonce_manager,
                                ),
                                name=f"m2-burst-{t.label}-b{bullet_idx}",
                            )
                            await asyncio.sleep(0.05)

        except asyncio.CancelledError:
            logger.info("[%s] Task iptal edildi.", tag)
            return
        except Exception as exc:
            logger.error("[%s] Multicall hatası: %s", tag, exc)


# ─────────────────────────────────────────────────────────────────────────────
# TARGETS.JSON WATCHER — Periyodik Güncelleme (v4)
# ─────────────────────────────────────────────────────────────────────────────

async def targets_json_watcher(
    state:       ClusterSniperState,
    w3:          AsyncWeb3,
) -> None:
    """
    Her TARGETS_POLL_INTERVAL saniyede targets.json'u okur.
    Yeni hedefler → on-chain keşif + cluster'a ekle.
    Kaldırılan hedefler → confirmed olarak işaretle.
    """
    tag = "TARGETS-WATCHER"
    logger.info(
        "[%s] Başladı — %ds aralıkla targets.json kontrol edilecek.",
        tag, TARGETS_POLL_INTERVAL,
    )

    known_addresses: Set[str] = {
        bs.target.address.lower() for bs in state.burst_states
    }

    while True:
        await asyncio.sleep(TARGETS_POLL_INTERVAL)

        try:
            fresh = read_targets_json(state.chain_tag)
            fresh_lower = {addr.lower(): addr for addr in fresh}

            # ── Yeni hedefler ──────────────────────────────────────────────
            new_addrs = set(fresh_lower.keys()) - known_addresses
            if new_addrs:
                logger.info("[%s] %d YENİ hedef tespit edildi.", tag, len(new_addrs))

                reserves = state.reserves
                new_burst_states: List[BurstState] = []

                for addr_low in new_addrs:
                    raw_addr = fresh_lower[addr_low]
                    try:
                        address = AsyncWeb3.to_checksum_address(raw_addr)
                    except Exception:
                        continue

                    target = ClusterTarget(
                        address = address,
                        label   = address[:12],
                    )

                    # ── OptiPair pair bilgisini pre-populate et ──────────
                    tdata = fresh.get(raw_addr, {})
                    coll_sym = tdata.get("collateral_asset")
                    debt_sym = tdata.get("debt_asset")

                    if coll_sym:
                        coll_res = _resolve_reserve(coll_sym, reserves)
                        if coll_res:
                            canonical_coll = coll_res["symbol"]
                            target.coll_token    = canonical_coll
                            target.coll_address  = coll_res["asset"]
                            target.coll_atoken   = coll_res["atoken"]
                            target.coll_decimals = coll_res["decimals"]
                            target.coll_lt       = coll_res["lt"]
                            target.coll_bonus    = coll_res.get(
                                "bonus", LIQUIDATION_BONUS_MAP.get(canonical_coll, DEFAULT_BONUS),
                            )
                    if debt_sym:
                        debt_res = _resolve_reserve(debt_sym, reserves)
                        if debt_res:
                            canonical_debt = debt_res["symbol"]
                            target.debt_token    = canonical_debt
                            target.debt_address  = debt_res["asset"]
                            target.debt_vtoken   = debt_res["vtoken"]
                            target.debt_decimals = debt_res["decimals"]

                    if tdata.get("bonus_pct") is not None and target.coll_token:
                        target.coll_bonus = tdata["bonus_pct"] / 100.0
                    if tdata.get("effective_close_factor") is not None:
                        target.close_factor = tdata["effective_close_factor"]

                    # eMode LT türetimi (load_cluster ile aynı mantık)
                    # Sadece eMode aktifse türet! Aksi takdirde dust varlıklar LT'yi bozar (Motor1 HF: 0.98)
                    emode_cat    = tdata.get("emode_category", 0)
                    if emode_cat > 0:
                        hf_val       = tdata.get("hf")
                        debt_usd_val = tdata.get("debt_usd")
                        coll_usd_val = tdata.get("collateral_usd")
                        if (hf_val and debt_usd_val and coll_usd_val
                                and coll_usd_val > 0 and debt_usd_val > 0):
                            derived_lt = (hf_val * debt_usd_val) / coll_usd_val
                            if 0.50 <= derived_lt <= 1.00:
                                target.coll_lt = derived_lt

                    if target.coll_token and target.debt_token:
                        target.label = f"{target.debt_token}/{target.coll_token}"

                    bs = BurstState(target=target)
                    new_burst_states.append(bs)
                    state.burst_states.append(bs)
                    known_addresses.add(addr_low)

                # Yeni hedefler için on-chain pozisyon keşfi
                if new_burst_states:
                    temp_state = ClusterSniperState(
                        oracle=state.oracle,
                        burst_states=new_burst_states,
                        chain_cfg=state.chain_cfg,
                        chain_tag=state.chain_tag,
                        reserves=state.reserves,
                        feed_map=state.feed_map,
                        all_feed_addresses=state.all_feed_addresses,
                        all_aave_tokens=state.all_aave_tokens,
                    )
                    await init_all_targets(w3, temp_state)

                    for bs in new_burst_states:
                        if not bs.confirmed:
                            bs.target.recalculate(state.oracle)
                            bs.reset_thresholds()

                    valid = sum(1 for bs in new_burst_states if not bs.confirmed)
                    logger.info(
                        "[%s] %d yeni hedef eklendi (%d geçerli, %d atlandı).",
                        tag, len(new_burst_states), valid,
                        len(new_burst_states) - valid,
                    )

            # ── Kaldırılan hedefler ────────────────────────────────────────
            removed_addrs = known_addresses - set(fresh_lower.keys())
            if removed_addrs:
                for bs in state.burst_states:
                    if bs.target.address.lower() in removed_addrs and not bs.confirmed:
                        bs.confirmed = True
                        logger.info(
                            "[%s] KALDIRILDI | %s | targets.json'dan silindi.",
                            tag, bs.target.label,
                        )
                known_addresses -= removed_addrs

            active_count = sum(1 for bs in state.burst_states if not bs.confirmed)
            logger.debug(
                "[%s] Kontrol tamamlandı | Aktif: %d | Toplam: %d",
                tag, active_count, len(state.burst_states),
            )

        except asyncio.CancelledError:
            logger.info("[%s] Task iptal edildi.", tag)
            return
        except Exception as exc:
            logger.error("[%s] Hata: %s", tag, exc)


# ─────────────────────────────────────────────────────────────────────────────
# CLUSTER YÜKLEME [v4 — targets.json]
# ─────────────────────────────────────────────────────────────────────────────

# watcher.py OptiPairEngine ile aynı normalleştirme tablosu.
# On-chain semboller Unicode veya bridge ekleri içerebilir;
# CHAIN_RESERVES'daki kanonik isimlerle eşleştirmek gerekir.
_SYMBOL_NORMALIZE: Dict[str, str] = {
    "USD₮0": "USDT", "USDT0": "USDT", "USDt": "USDT",
    "USDT.e": "USDT", "USDTe": "USDT",
    "USDC0": "USDC", "USDCn": "USDC",
}


def _resolve_reserve(symbol: str, reserves: List[Dict]) -> Optional[Dict]:
    """
    Reserve tablosundan sembolle eşleşen kaydı döndürür.
    Önce orijinal sembolü dener, bulamazsa normalleştirilmiş
    halini dener (USD₮0 → USDT, USDCn → USDC vb.)
    """
    for r in reserves:
        if r["symbol"] == symbol:
            return r
    # Normalleştirilmiş sembol ile tekrar dene
    normalized = _SYMBOL_NORMALIZE.get(symbol)
    if normalized:
        for r in reserves:
            if r["symbol"] == normalized:
                return r
    return None


def load_cluster_from_targets_json(chain_tag: str, chain_cfg: ChainConfig) -> ClusterSniperState:
    """
    [v4] Hedefleri targets.json'dan yükler.

    [v4.1] OptiPair Entegrasyonu:
      targets.json'daki collateral_asset / debt_asset / bonus_pct alanları
      ClusterTarget'a pre-populate edilir. init_all_targets bu alanları
      dolu görüp auto-pair discovery'yi atlar → watcher.py'nin OptiPair
      motoru tarafından seçilen optimal çift korunur.

      Eski davranış: pair bilgisi okunuyor ama kullanılmıyordu →
      init_all_targets kendi "en büyük bakiye" keşfini yapıyordu → YANLIŞ ÇİFT.
    """
    oracle       = OracleState()
    burst_states: List[BurstState] = []

    targets_data = read_targets_json(chain_tag)

    chain_reserves, chain_feed_map, chain_feed_addrs, chain_aave_tokens = _build_chain_data(chain_tag)

    for raw_addr, tdata in targets_data.items():
        try:
            address = AsyncWeb3.to_checksum_address(raw_addr)
        except Exception:
            logger.warning("[LOAD] Geçersiz adres: %s", raw_addr)
            continue

        target = ClusterTarget(
            address    = address,
            label      = address[:12],
        )

        # ── OptiPair Pair Bilgisini Pre-Populate Et ────────────────────────
        # targets.json'daki collateral_asset / debt_asset sembollerini
        # CHAIN_RESERVES tablosundan çözerek ClusterTarget alanlarını doldur.
        # init_all_targets "if not t.coll_token" kontrolüyle bu alanları
        # dolu görüp auto-discovery'yi atlayacak.
        coll_sym = tdata.get("collateral_asset")
        debt_sym = tdata.get("debt_asset")

        if coll_sym:
            coll_res = _resolve_reserve(coll_sym, chain_reserves)
            if coll_res:
                # Kanonik sembol kullan (USD₮0 → USDT gibi)
                canonical_coll = coll_res["symbol"]
                target.coll_token    = canonical_coll
                target.coll_address  = coll_res["asset"]
                target.coll_atoken   = coll_res["atoken"]
                target.coll_decimals = coll_res["decimals"]
                target.coll_lt       = coll_res["lt"]
                target.coll_bonus    = coll_res.get(
                    "bonus", LIQUIDATION_BONUS_MAP.get(canonical_coll, DEFAULT_BONUS),
                )
            else:
                logger.warning(
                    "[LOAD] %s | collateral_asset='%s' CHAIN_RESERVES'da bulunamadı — "
                    "init_all_targets auto-discovery yapacak.",
                    address[:12], coll_sym,
                )

        if debt_sym:
            debt_res = _resolve_reserve(debt_sym, chain_reserves)
            if debt_res:
                # Kanonik sembol kullan
                canonical_debt = debt_res["symbol"]
                target.debt_token    = canonical_debt
                target.debt_address  = debt_res["asset"]
                target.debt_vtoken   = debt_res["vtoken"]
                target.debt_decimals = debt_res["decimals"]
            else:
                logger.warning(
                    "[LOAD] %s | debt_asset='%s' CHAIN_RESERVES'da bulunamadı — "
                    "init_all_targets auto-discovery yapacak.",
                    address[:12], debt_sym,
                )

        # OptiPair bonus ve close factor
        if tdata.get("bonus_pct") is not None and target.coll_token:
            target.coll_bonus = tdata["bonus_pct"] / 100.0
        if tdata.get("effective_close_factor") is not None:
            target.close_factor = tdata["effective_close_factor"]

        # ── eMode LT Derivation ─────────────────────────────────────
        # CHAIN_RESERVES base-mode LT icerir (orn: wrsETH=0.75).
        # Ama pozisyon eMode'daysa gercek LT farkli (orn: 0.95).
        # Aave'nin kendi HF hesabindan effective LT turetiyoruz:
        #   effective_LT = (HF * debt_usd) / coll_usd
        # RAM HF, on-chain HF ile eslesir.
        # Sadece eMode kullanıcıları için türetme yap! (Watcher'daki dust filtreleri yüzünden
        # normal pozisyonların hesabında derived_lt çöküyor ve Motor1 hf'si 0.98'e düşüyor)
        emode_cat    = tdata.get("emode_category", 0)
        if emode_cat > 0:
            hf_val       = tdata.get("hf")
            debt_usd_val = tdata.get("debt_usd")
            coll_usd_val = tdata.get("collateral_usd")
            if (hf_val and debt_usd_val and coll_usd_val
                    and coll_usd_val > 0 and debt_usd_val > 0):
                derived_lt = (hf_val * debt_usd_val) / coll_usd_val
                if 0.50 <= derived_lt <= 1.00:
                    target.coll_lt = derived_lt

        # Label: pair doluysa sembol etiketini koy
        if target.coll_token and target.debt_token:
            target.label = f"{target.debt_token}/{target.coll_token}"

        burst_states.append(BurstState(target=target))

    if not burst_states:
        logger.warning("[%s] targets.json'da bu ağ için hedef bulunamadı. Watcher bekleniyor...", chain_tag)

    pre_paired = sum(
        1 for bs in burst_states
        if bs.target.coll_token and bs.target.debt_token
    )
    logger.info(
        "[LOAD] %s: %d hedef yüklendi, %d/%d pair pre-populated (OptiPair).",
        chain_tag, len(burst_states), pre_paired, len(burst_states),
    )

    state = ClusterSniperState(
        oracle=oracle,
        burst_states=burst_states,
        chain_cfg=chain_cfg,
        chain_tag=chain_tag,
        reserves=chain_reserves,
        feed_map=chain_feed_map,
        all_feed_addresses=chain_feed_addrs,
        all_aave_tokens=chain_aave_tokens,
    )
    return state


def log_cluster_banner(state: ClusterSniperState, fast_rpc: str) -> None:
    logger.info("=" * 76)
    logger.info("  AAVE V3 %s CLUSTER DUAL-ENGINE SNIPER v4 — %d HEDEF",
                state.chain_tag, len(state.burst_states))
    logger.info("  Kaynak: targets.json (watcher.py köprüsü)")
    logger.info("  Güncelleme Aralığı: %ds", TARGETS_POLL_INTERVAL)
    logger.info("  Dry Run: %s", "EVET — TX GÖNDERİLMEZ" if state.dry_run else "HAYIR")
    logger.info("  Fast RPC: %s...", fast_rpc[:55])
    logger.info("─" * 76)
    logger.info("  %-14s | %-42s | TEMİNAT       | BORÇ",
                "LABEL", "ADRES")
    logger.info("─" * 76)
    for bs in state.burst_states:
        t = bs.target
        if bs.confirmed or not t.coll_token:
            logger.info(
                "  %-14s | %s | (on-chain yok / atlandı)",
                (t.label or t.address[:12]), t.address[:12],
            )
            continue
        logger.info(
            "  %-14s | %s | %-6s LT=%.0f%% Bns=%.0f%% | %-6s CF=%.0f%%",
            t.label, t.address[:12],
            t.coll_token, t.coll_lt * 100, t.coll_bonus * 100,
            t.debt_token, t.close_factor * 100,
        )
    logger.info("─" * 76)
    logger.info("  İzlenen Chainlink Feed'leri (%d benzersiz):", len(state.all_feed_addresses))
    for feed_addr in state.all_feed_addresses:
        symbol = state.feed_map.get(feed_addr.lower(), "?")
        logger.info("    %-6s → %s", symbol, feed_addr)
    logger.info("=" * 76)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN [v3 — Multi-Chain]
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Aave V3 Multi-Chain Cluster Sniper v4")
    p.add_argument("--chain",     type=str, default="ARB",
                   choices=["ARB", "BASE", "OP"],
                   help="Hedef ağ (varsayılan: ARB)")
    p.add_argument("--dry",       action="store_true", help="TX imzalamaz")
    p.add_argument("--skip-init", action="store_true", help="On-chain fetch atla")
    p.add_argument("--benchmark", action="store_true",
                   help="Test icin baglanir, motor2'yi devamli atesler. (DRY RUN aktif olur)")
    return p.parse_args()


async def main() -> None:
    global _ACTIVE_CHAIN_ID, _ACTIVE_POOL_ADDR

    args = parse_args()
    chain_tag = args.chain.upper()

    # ── ChainConfig çözümleme ─────────────────────────────────────────────────
    all_chains = load_chains()
    chain_cfg  = next((c for c in all_chains if c.tag == chain_tag), None)
    if not chain_cfg:
        logger.error("❌ %s ağı için ChainConfig bulunamadı! .env'de %s_RPC tanımlı mı?", chain_tag, chain_tag)
        sys.exit(1)

    # ── Env değişkenleri ──────────────────────────────────────────────────────
    private_key  = os.getenv("SNIPER_PRIVATE_KEY", "")
    # Chain-bazli FAST_RPC: once {CHAIN}_FAST_RPC, yoksa FAST_RPC, yoksa normal RPC
    fast_rpc     = os.getenv(f"{chain_tag}_FAST_RPC",
                             os.getenv("FAST_RPC", chain_cfg.rpc_url))
    gas_limit    = int(os.getenv("GAS_LIMIT",       "900000"))
    gas_mult     = float(os.getenv("GAS_MULTIPLIER", "1.2"))
    # Chain-bazli EXECUTOR: once {CHAIN}_EXECUTOR, yoksa AAVE_EXECUTOR
    executor     = os.getenv(f"{chain_tag}_EXECUTOR",
                             os.getenv("AAVE_EXECUTOR", ""))

    errors = []
    if not chain_cfg.rpc_url:
        errors.append(f"{chain_tag}_RPC eksik")
    if not chain_cfg.wss_url:
        errors.append(f"{chain_tag}_WSS eksik")
    if not private_key and not args.dry:
        errors.append("SNIPER_PRIVATE_KEY eksik")
    if errors:
        for e in errors:
            logger.error("❌ %s", e)
        sys.exit(1)

    # ── Cluster yükle (targets.json'dan) ──────────────────────────────────────
    cluster_state = load_cluster_from_targets_json(chain_tag, chain_cfg)
    cluster_state.dry_run = args.dry or args.benchmark
    cluster_state.benchmark = getattr(args, "benchmark", False)

    # ── Modül-düzeyi runtime değişkenleri set et ──────────────────────────────
    w3_temp = await build_w3(chain_cfg.rpc_url, f"{chain_tag}-ChainID")
    _ACTIVE_CHAIN_ID  = await w3_temp.eth.chain_id
    _ACTIVE_POOL_ADDR = chain_cfg.pool_address
    cluster_state.chain_id = _ACTIVE_CHAIN_ID

    # ── Web3 bağlantıları ─────────────────────────────────────────────────────
    w3_alchemy = w3_temp  # Zaten bağlandık
    w3_fast    = (await build_w3(fast_rpc, "Fast-RPC")
                  if fast_rpc != chain_cfg.rpc_url else w3_alchemy)

    # ── On-chain pozisyon yükle ───────────────────────────────────────────────
    if not args.skip_init and cluster_state.burst_states:
        await init_all_targets(w3_alchemy, cluster_state)
    elif not cluster_state.burst_states:
        logger.warning("[INIT] targets.json'da hedef yok — targets_json_watcher yeni hedefleri algılayacak.")
    else:
        logger.warning("[INIT] --skip-init: Pozisyon fetch atlandı.")

    # Banner: LT/Bns/CF init sonrası on-chain rezervden dolu olur.
    log_cluster_banner(cluster_state, fast_rpc)

    # ── Başlangıç oracle fiyatları (HTTP — WSS bağlanmadan önce) ─────────────
    logger.info("[INIT] Başlangıç oracle fiyatları HTTP'den yükleniyor...")

    cs = AsyncWeb3.to_checksum_address

    fetched_feeds: Set[str] = set()
    
    # Adım 1: Önce WETH feed çekilmeli (stETH/weETH oranlarını USD'ye çervirmek için)
    weth_price = 2500.0
    for feed_addr in cluster_state.all_feed_addresses:
        token_syms = cluster_state.feed_map.get(feed_addr.lower(), [])
        if "WETH" in token_syms:
            try:
                feed  = w3_alchemy.eth.contract(address=cs(feed_addr), abi=CHAINLINK_ROUND_ABI)
                _, ans, *_ = await feed.functions.latestRoundData().call()
                weth_price = ans / CHAINLINK_DECIMALS
            except Exception:
                pass
            break

    # Adım 2: Tüm fiyatları çek
    for feed_addr in cluster_state.all_feed_addresses:
        if feed_addr in fetched_feeds:
            continue
        fetched_feeds.add(feed_addr)

        token_syms = cluster_state.feed_map.get(feed_addr.lower(), [])
        try:
            feed  = w3_alchemy.eth.contract(address=cs(feed_addr), abi=CHAINLINK_ROUND_ABI)
            _, ans, *_ = await feed.functions.latestRoundData().call()
            price = ans / CHAINLINK_DECIMALS
            
            # Eğer feed wstETH/ETH veya weETH/ETH (18-dec oranı) ise, fiyat milyon dolara fırlar (örn: 1.18e10)
            if price > 10**6:
                price = (ans / 10**18) * weth_price
                
            for sym in token_syms:
                cluster_state.oracle.prices[sym] = price
                logger.info("  %-6s = $%.4f", sym, price)
        except Exception as exc:
            # LST PriceCapAdapters often omit latestRoundData and only expose latestAnswer
            try:
                feed_alt = w3_alchemy.eth.contract(address=cs(feed_addr), abi=[{'inputs':[],'name':'latestAnswer','outputs':[{'name':'','type':'int256'}],'stateMutability':'view','type':'function'}])
                ans = await feed_alt.functions.latestAnswer().call()
                price = ans / CHAINLINK_DECIMALS
                if price > 10**6:
                    price = (ans / 10**18) * weth_price
                for sym in token_syms:
                    cluster_state.oracle.prices[sym] = price
                    logger.info("  %-6s = $%.4f (latestAnswer fallback)", sym, price)
            except Exception as exc2:
                logger.warning("  %-6s feed yüklenemedi: HTTP Oracle Failure", token_syms)

    # ── liq_ratio ve burst eşiklerini hesapla ─────────────────────────────────
    o = cluster_state.oracle
    logger.info("[INIT] Tasfiye eşikleri hesaplanıyor...")

    for bs in cluster_state.burst_states:
        if bs.confirmed:
            continue

        bs.target.recalculate(o)
        bs.reset_thresholds()

        t = bs.target
        if getattr(t, 'in_memory_hf', 999.0) < 999.0:
            current_hf = t.in_memory_hf
            logger.info(
                "  %-14s | RAM HF=%.6f | kâr=$%.2f",
                t.label, current_hf, t.estimated_profit_usd,
            )
        else:
            logger.warning("  %-14s | HF hesaplanamadı (pozisyon sıfır?)", t.label)

    # ── Price Queue (Oracle Hub → Burst Engine) ───────────────────────────────
    price_queue: asyncio.Queue = asyncio.Queue(maxsize=128)

    # ── Paralel Task'lar ──────────────────────────────────────────────────────
    wss_url = chain_cfg.wss_url

    tasks = [
        asyncio.create_task(
            oracle_hub(cluster_state, wss_url, price_queue),
            name="oracle-hub",
        ),
        asyncio.create_task(
            burst_fire_engine(
                state=cluster_state,
                w3=w3_alchemy,
                private_key=private_key,
                executor=executor or "",
            ),
            name="burst-engine",
        ),
        asyncio.create_task(
            state_change_watcher(
                state=cluster_state,
                w3_alchemy=w3_alchemy,
                wss_url=wss_url,
                trigger_lock=asyncio.Lock()
            ),
            name="state-watcher",
        ),
        asyncio.create_task(
            motor2_hf_monitor(
                cluster_state, w3_fast, private_key,
                gas_limit, gas_mult, executor or None,
                float(os.getenv("POLL_INTERVAL", "0.4")),
            ),
            name="motor2-hf",
        ),
        asyncio.create_task(
            targets_json_watcher(
                state=cluster_state,
                w3=w3_alchemy,
            ),
            name="targets-watcher",
        ),
    ]

    logger.info("=" * 76)
    logger.info("  5 paralel task başlatıldı (%s):", chain_tag)
    for t in tasks:
        logger.info("    ▶ %s", t.get_name())
    logger.info("=" * 76)

    prices_str = " | ".join(f"{k}=${v:.2f}" for k, v in o.prices.items())
    tg.send(
        f"🚀 <b>CLUSTER SNIPER v4 [{chain_tag}] BAŞLADI</b>\n"
        f"🎯 {len(cluster_state.burst_states)} hedef (targets.json)\n"
        f"🔄 Güncelleme: {TARGETS_POLL_INTERVAL}s aralıkla\n"
        f"💹 <code>{prices_str}</code>\n"
        f"⚙️ Dry Run: {'EVET' if args.dry else 'HAYIR'}"
    )

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    for t in done:
        exc = t.exception()
        if exc:
            logger.error("Task '%s' hata: %s", t.get_name(), exc)

    for t in pending:
        t.cancel()

    await asyncio.gather(*pending, return_exceptions=True)
    logger.info("Cluster Sniper [%s] sonlandı.", chain_tag)


if __name__ == "__main__":
    asyncio.run(main())
===
"""
cluster_sniper.py — Multi-Chain Aave V3 Cluster Dual-Engine Sniper v4
=====================================================================

watcher.py → targets.json → cluster_sniper.py köprüsü.
config.py, telegram_utils.py ortak olarak kullanılır.

Kullanım:
    python cluster_sniper.py --chain ARB          # Arbitrum hedefleri
    python cluster_sniper.py --chain BASE --dry   # Base, TX göndermez
    python cluster_sniper.py --chain OP            # Optimism hedefleri

──────────────────────────────────────────────────────────────────────────────
v3 → v4 REFACTOR: targets.json Entegrasyonu
──────────────────────────────────────────────────────────────────────────────

DEĞİŞİKLİKLER:
  1. kurban.txt kaldırıldı. Hedef kaynağı artık watcher.py'nin yazdığı
     targets.json dosyasıdır. Zorunlu alanlar: chain, address, hf, debt_usd,
     collateral_usd, updated_at. İsteğe bağlı (watcher OptiPair): debt_asset,
     collateral_asset, bonus_pct, effective_close_factor.

  2. targets_json_watcher(): Her 45 saniyede targets.json'u okur,
     yeni hedefler tespit edilir → on-chain pozisyon keşfi yapılır →
     Motor 1/2'ye eklenir. Kaldırılan hedefler devre dışı bırakılır.

  3. Auto-Pair Discovery: init_all_targets artık en büyük borç ve
     en büyük teminat tokenını otomatik seçer. Dossier gereksiz.

  4. E-mode kontrolü kaldırıldı: Basit yapı, gereksiz hantallık.
     Normal LT/bonus değerleri CHAIN_RESERVES'dan kullanılır.

──────────────────────────────────────────────────────────────────────────────
ZORUNLU .env DEĞİŞKENLERİ
──────────────────────────────────────────────────────────────────────────────

  SNIPER_PRIVATE_KEY=0x...     İşlem imzalama key'i
  ARB_RPC=https://...          Alchemy HTTP endpoint (Arbitrum)
  ALCHEMY_WSS_URL=wss://...    Alchemy WSS (Oracle Hub + State watch)
  FAST_RPC=https://...         Motor 2 için premium RPC (QuickNode vb.)
                               Boşsa ARB_RPC kullanılır.

OPSİYONEL:
  BURST_M1=0.001               Bullet 1 marjı (%0.1)
  BURST_M2=0.0005              Bullet 2 marjı (%0.05)
  GAS_LIMIT=900000
  GAS_MULTIPLIER=1.2
  POLL_INTERVAL=0.4
  TARGETS_POLL=45              targets.json okuma aralığı (saniye)
  AAVE_EXECUTOR=0x...          Flash loan executor (opsiyonel)
  UNISWAP_POOL=0x...           Executor'ın kullanacağı Uniswap V3 havuzu
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import websockets
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.middleware import ExtraDataToPOAMiddleware

from config import (
    POOL_ABI, MULTICALL3_ABI, DATA_PROVIDER_ABI, ERC20_ABI,
    LIQUIDATION_BONUS_MAP, DEFAULT_BONUS,
    FLASH_LOAN_FEE, DEX_SLIPPAGE,
    WAD, USD_DECIMALS, MANUAL_LT_VALUES,
    ChainConfig, load_chains,
    ASSET_CLASS,
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


# ─────────────────────────────────────────────────────────────────────────────
# TARGETS.JSON OKUYUCU — watcher.py Köprüsü (v4)
# ─────────────────────────────────────────────────────────────────────────────
# watcher.py hedefleri targets.json'a yazar. cluster_sniper bu dosyayı
# periyodik olarak okuyarak güncel hedef listesini alır.
# ─────────────────────────────────────────────────────────────────────────────

TARGETS_JSON_PATH = os.getenv("TARGETS_JSON", "targets.json")
TARGETS_POLL_INTERVAL = int(os.getenv("TARGETS_POLL", "45"))


def read_targets_json(chain_tag: str) -> Dict[str, Dict]:
    """
    targets.json'dan seçilen ağın hedeflerini okur.

    Dönüş:
      {"0xABC...": {"hf": 1.02, "debt_usd": 5000, "collateral_usd": 6000}, ...}
    """
    try:
        with open(TARGETS_JSON_PATH, "r", encoding="utf-8") as f:
            records = json.load(f)
    except FileNotFoundError:
        logger.warning("[TARGETS] targets.json bulunamadı — boş hedef listesi.")
        return {}
    except json.JSONDecodeError as exc:
        logger.error("[TARGETS] targets.json parse hatası: %s", exc)
        return {}

    result: Dict[str, Dict] = {}
    for rec in records:
        if rec.get("chain", "").upper() != chain_tag.upper():
            continue
        addr = rec.get("address", "")
        if not addr:
            continue
        row: Dict[str, Any] = {
            "hf":             rec.get("hf", 999.0),
            "debt_usd":       rec.get("debt_usd", 0.0),
            "collateral_usd": rec.get("collateral_usd", 0.0),
            "updated_at":     rec.get("updated_at", ""),
        }
        for key in ("collateral_asset", "debt_asset", "bonus_pct", "effective_close_factor"):
            if key in rec and rec[key] is not None:
                row[key] = rec[key]
        result[addr] = row

    n_pair = sum(
        1
        for v in result.values()
        if v.get("collateral_asset") and v.get("debt_asset")
    )
    logger.info(
        "[TARGETS] %s: %d hedef (targets.json), %d kayıtta parite (BORÇ/TEMİNAT) alanı var.",
        chain_tag,
        len(result),
        n_pair,
    )
    return result

# ─────────────────────────────────────────────────────────────────────────────
# ZİNCİR BAZLI AAVE RESERVE TABLOLARI (v3)
# ─────────────────────────────────────────────────────────────────────────────
# Her zincirin kendi reserve listesi, Chainlink feed'leri ve token adresleri.
# Multicall3 adresi EIP-2470 standardı → tüm zincirlerde aynı.
#
# Not: Bu tablolar SADECE cluster_sniper'ın ihtiyaç duyduğu alanları içerir:
#   symbol, asset (underlying), atoken, vtoken, decimals, lt, feed
# ─────────────────────────────────────────────────────────────────────────────

CHAIN_RESERVES: Dict[str, List[Dict]] = {
    # ── Arbitrum ─────────────────────────────────────────────────────────────
    "ARB": [
        {"symbol": "WETH",   "asset": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "atoken": "0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8", "vtoken": "0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351", "decimals": 18, "lt": 0.84, "bonus": 0.05, "feed": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"},
        {"symbol": "WBTC",   "asset": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "atoken": "0x078f358208685046a11C85e8ad32895DED33A249", "vtoken": "0x92b42c66840C7AD907b4BF74879FF3eF7c529473", "decimals": 8,  "lt": 0.78, "bonus": 0.07, "feed": "0x6ce185860a4963106506C203335A2910413708e9"},
        {"symbol": "USDC",   "asset": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "atoken": "0x724dc807b04555b71ed48a6896b6F41593b8C637", "vtoken": "0xf611aEb5013fD2c0511c9CD55c7dc5C1140741A6", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3"},
        {"symbol": "USDC.e", "asset": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", "atoken": "0x625E7708f30cA75bfd92586e17077590C60eb4cD", "vtoken": "0xFCCf3cAbbe80101232d343252614b6A3eE81C989", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3"},
        {"symbol": "USDT",   "asset": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "atoken": "0x6ab707Aca953eDAeFBc4fD23bA73294241490620", "vtoken": "0xfb00AC187a8Eb5AFAE4eACE434F493Eb62672df7", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7"},
        {"symbol": "ARB",    "asset": "0x912CE59144191C1204E64559FE8253a0e49E6548", "atoken": "0x6533afac2E7BCCB20dca161449A13A32D391fb00", "vtoken": "0x44705f578135cC5d703b4c9c122528C73Eb87145", "decimals": 18, "lt": 0.63, "bonus": 0.10, "feed": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6"},
        {"symbol": "DAI",    "asset": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "atoken": "0x82E64f49Ed5EC1bC6e43DAD4FC8Af9bb3A2312EE", "vtoken": "0x8619d80FB0141ba7F184CbF22fd724116D9f7ffC", "decimals": 18, "lt": 0.77, "bonus": 0.05, "feed": "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB"},
        {"symbol": "wstETH", "asset": "0x5979D7b546E38E414F7E9822514be443A4800529", "atoken": "0x513c7E3a9c69cA3e22550eF58AC1C0088e918FFf", "vtoken": "0x77CA01483f379E58174739308945f044e1a764dc", "decimals": 18, "lt": 0.79, "bonus": 0.072, "feed": "0xb523AE262D20A936BC152e6023996e46FDC2A95D", "feed_type": "ETH_RATIO"},
        {"symbol": "weETH",  "asset": "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe", "atoken": "0x8437d7c167dfb82ed4cb79cd44b7a32a1dd95c77", "vtoken": "0x3ca5fa07689f266e907439afd1fbb59c44fe12f6", "decimals": 18, "lt": 0.77, "bonus": 0.075, "feed": "0x258a576895DC50c990500775d6591ff2D52059f2", "feed_type": "ETH_RATIO"},
        {"symbol": "LINK",   "asset": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4", "atoken": "0x191c10Aa4AF7C30e871E70C95dB0E4eb77237530", "vtoken": "0x953A573793604aF8d41F306FEb8274190dB4aE0e", "decimals": 18, "lt": 0.75, "bonus": 0.10, "feed": "0x86E53CF1B870786351Da77A57575e79CB55812CB"},
        {"symbol": "AAVE",   "asset": "0xba5DdD1f9d7F570dc94a51479a000E3BCE967196", "atoken": "0xe3241b2Bcc3D81580A18bc86105F678AE66380cE", "vtoken": "0x0000000000000000000000000000000000000000", "decimals": 18, "lt": 0.73, "bonus": 0.10, "feed": "0xf97eEAac36bdd096bb2445c7582F9095bfCE04C7"},
    ],
    # ── Base ──────────────────────────────────────────────────────────────────
    "BASE": [
        {"symbol": "WETH",     "asset": "0x4200000000000000000000000000000000000006", "atoken": "0xD4a0e0b9149BCee3C920d2E00b5dE09138fd8bb7", "vtoken": "0x24e6e0795b3c7c71D965fCc4f371803d1c1DcA1E", "decimals": 18, "lt": 0.83, "bonus": 0.05,  "feed": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70"},
        {"symbol": "cbBTC",    "asset": "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf", "atoken": "0xBdb9300b7CDE636d9cD4AFF00f6F009fFBBc8EE6", "vtoken": "0x05e08702028de6AaD395DC6478b554a56920b9AD", "decimals": 8,  "lt": 0.78, "bonus": 0.07,  "feed": "0x07DA0E54543a844a80ABE69c8A12F22B3aA59f9D"},
        {"symbol": "USDC",     "asset": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "atoken": "0x4e65fE4DbA92790696d040ac24Aa414708F5c0AB", "vtoken": "0x59dca05b6c26dBd64b5381374aAaC5CD05644C28", "decimals": 6,  "lt": 0.78, "bonus": 0.05,  "feed": "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B"},
        {"symbol": "wstETH",   "asset": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452", "atoken": "0x99CBC45ea5bb7eF3a5BC08FB1B7E56bB2442Ef0D", "vtoken": "0x41A7C3f5904ad176dACbb1D99101F59ef0811DC1", "decimals": 18, "lt": 0.79, "bonus": 0.06,  "feed": "0xB88BAc61a4Ca37C43a3725912B1f472c9A5bc061", "feed_type": "ETH_RATIO"},
        {"symbol": "weETH",    "asset": "0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A", "atoken": "0x7C307e128efA31F540F2E2d976C995E0B65F51F6", "vtoken": "0x8D2e3F1f4b38AA9f1ceD22ac06019c7561B03901", "decimals": 18, "lt": 0.77, "bonus": 0.075, "feed": "0x0A0fFc2952B682eb36cde5Bdb03169C2c5F5305c", "feed_type": "ETH_RATIO"},
        {"symbol": "tBTC",     "asset": "0x236aa50979D5f3De3Bd1Eeb40E81137F22ab794b", "atoken": "0xbcFFB4B3beADc989Bd1458740952aF6EC8fBE431", "vtoken": "0x182cDEEC1D52ccad869d621bA422F449FA5809f5", "decimals": 18, "lt": 0.78, "bonus": 0.075, "feed": "0x07DA0E54543a844a80ABE69c8A12F22B3aA59f9D"},
        {"symbol": "EURC",     "asset": "0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42", "atoken": "0x90DA57E0A6C0d166Bf15764E03b83745Dc90025B", "vtoken": "0x03D01595769333174036832e18fA2f17C74f8161", "decimals": 6,  "lt": 0.78, "bonus": 0.05,  "feed": "0xDAe398520e2B67cd3f27aeF9Cf14D93D927f8250"},
        {"symbol": "syrupUSDC","asset": "0x660975730059246A68521a3e2FBD4740173100f5", "atoken": "0xD7424238CcbE7b7198Ab3cFE232e0271E22da7bd", "vtoken": "0x57B8C05ee2cD9d0143eBC21FBD9288C39B9F716c", "decimals": 6,  "lt": 0.92, "bonus": 0.04,  "feed": "0x5e9eCce4aCBBc8172A40f7b8A7c086A4bb5CDeac"},
        {"symbol": "wrsETH",   "asset": "0xEDfa23602D0EC14714057867A78d01e94176BEA0", "atoken": "0x80a94C36747CF51b2FbabDfF045f6D22c1930eD1", "vtoken": "0xe9541C77a111bCAa5dF56839bbC50894eba7aFcb", "decimals": 18, "lt": 0.75, "bonus": 0.075, "feed": "0x87445df51b4A2E5BbD2F0F8564C5bfe65cC3dB61", "feed_type": "ETH_RATIO"},
        {"symbol": "AAVE",     "asset": "0x63706e401c06ac8513145b7687A14804d17f814b", "atoken": "0x67EAF2BeE4384a2f84Da9Eb8105C661C123736BA", "vtoken": "0xcEC1Ea95dDEF7CFC27D3D9615E05b035af460978", "decimals": 18, "lt": 0.70, "bonus": 0.10,  "feed": "0xAAEbc0287EC3BF3fe56fCA1E4bcC793b219De551"},
        {"symbol": "GHO",      "asset": "0x6Bb7a212910682DCFdbd5BCBb3e28FB4E8da10Ee", "atoken": "0x067ae75628177FD257c2B1e500993e1a0baBcBd1", "vtoken": "0x38e59ADE183BbEb94583d44213c8f3297e9933e9", "decimals": 18, "lt": 0.00, "bonus": 0.00,  "feed": "0xfc421aD3C883Bf9E7C4f42dE845C4e4405799e73"},
    ],
    # ── Optimism ──────────────────────────────────────────────────────────────
    "OP": [
        {"symbol": "WETH",   "asset": "0x4200000000000000000000000000000000000006", "atoken": "0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8", "vtoken": "0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351", "decimals": 18, "lt": 0.83, "bonus": 0.05, "feed": "0x13e3Ee699D1909E989722E753853AE30b17e08c5"},
        {"symbol": "WBTC",   "asset": "0x68f180fcCe6836688e9084f035309E29Bf0A2095", "atoken": "0x078f358208685046a11C85e8ad32895DED33A249", "vtoken": "0x92b42c66840C7AD907b4BF74879FF3eF7c529473", "decimals": 8,  "lt": 0.78, "bonus": 0.075, "feed": "0xD702DD976Fb76Fffc2D3963D037dfDae5b04E593"},
        {"symbol": "USDC",   "asset": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85", "atoken": "0x38d693cE1dF5AaDF7bC62043aE5EF4B05deC7805", "vtoken": "0x5D557B07776D12967914379C71a1310e917C7b8c", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0x16a9FA2FDa030272Ce99B29CF780dFA30361E0f3"},
        {"symbol": "USDT",   "asset": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58", "atoken": "0x6ab707Aca953eDAeFBc4fD23bA73294241490620", "vtoken": "0xfb00AC187a8Eb5AFAE4eACE434F493Eb62672df7", "decimals": 6,  "lt": 0.78, "bonus": 0.05, "feed": "0xECef79E109e997bCA29c1c0897ec9d7b03647F5E"},
        {"symbol": "wstETH", "asset": "0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb", "atoken": "0xc45A479877e1e9Dfe9FcD4056c699575a1045dAA", "vtoken": "0x34e2eD44EF7466D5f9E0b782B5c08b57475e7907", "decimals": 18, "lt": 0.79, "bonus": 0.072, "feed": "0x698B585CbC4407e2D54aa898B2600B53C68958f7", "feed_type": "ETH_RATIO"},
    ],
}


def _build_chain_data(chain_tag: str) -> tuple:
    """
    Seçilen zincire göre FEED_MAP, FEED_TYPE_MAP, ALL_FEED_ADDRESSES ve ALL_AAVE_TOKENS
    oluşturur. Eski modül-düzeyindeki global'lerin runtime karşılığı.

    [v5] feed_type_map eklendi: {feed_addr_lower: "ETH_RATIO" | "USD"}
    LST tokenlarının (wstETH, weETH, wrsETH) feed'leri ETH oranı verir,
    diğerleri doğrudan USD fiyatı.
    """
    reserves = CHAIN_RESERVES.get(chain_tag.upper(), [])

    feed_map: Dict[str, List[str]] = {}
    feed_type_map: Dict[str, str] = {}   # feed_addr.lower() → "ETH_RATIO" | "USD"
    for r in reserves:
        f = r["feed"].lower()
        feed_map.setdefault(f, []).append(r["symbol"])
        # feed_type: ilk kaydeden kazanır (aynı feed paylaşan tokenlar aynı tipe sahip)
        if f not in feed_type_map:
            feed_type_map[f] = r.get("feed_type", "USD")

    all_feed_addresses = list(set(r["feed"] for r in reserves))

    all_aave_tokens = set()
    for r in reserves:
        all_aave_tokens.add(r["atoken"])
        if r["vtoken"] != "0x0000000000000000000000000000000000000000":
            all_aave_tokens.add(r["vtoken"])

    return reserves, feed_map, feed_type_map, all_feed_addresses, list(all_aave_tokens)



# ─────────────────────────────────────────────────────────────────────────────
# SABİTLER
# ─────────────────────────────────────────────────────────────────────────────

# Chainlink AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)
CHAINLINK_ANSWER_UPDATED = (
    "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
)
CHAINLINK_DECIMALS = 10 ** 8  # Chainlink fiyatları 8 ondalıklıdır

# Multicall3: EIP-2470 standardı, tüm zincirlerde aynı adres
MULTICALL3_ADDR = "0xcA11bde05977b3631167028862bE2a173976CA11"

# Modül-düzeyi runtime değişkenleri — main() tarafından set edilir
# build_and_sign_bullet() bu değerlere ihtiyaç duyar ama parametreye erişimi yok
_ACTIVE_CHAIN_ID:  int = 42161  # Varsayılan Arbitrum, main()'de güncellenir
_ACTIVE_POOL_ADDR: str = ""     # main()'de chain_cfg.pool_address ile set edilir

# Uniswap V3 Fee Tier (executor kontratın swap yaparken kullanacağı komisyon katmanı)
# 100 = %0.01, 500 = %0.05, 3000 = %0.30, 10000 = %1.00
# .sol kontratındaki executeSnipe() fonksiyonuna uint24 olarak geçilir.
DEFAULT_UNISWAP_FEE_TIER = int(os.getenv("UNISWAP_FEE_TIER", "500"))  # %0.05 varsayılan

# Parite bazlı optimal fee tier haritası
# Uniswap V3 Arbitrum'da likiditenin yoğunlaştığı havuzlar:
UNISWAP_FEE_TIER_MAP: Dict[str, int] = {
    "WBTC/USDC":  500,    # %0.05 — Ana stablecoin havuzu
    "WBTC/USDT":  500,    # %0.05
    "WETH/USDC":  500,    # %0.05 — Ana stablecoin havuzu
    "WETH/USDT":  500,    # %0.05
    "ARB/WETH":   3000,   # %0.30 — Volatil parite, daha geniş spread
    "WETH/ARB":   3000,   # %0.30 (tersi)
}

# Aave V3 liquidationCall ABI
LIQUIDATION_CALL_ABI = [{
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
}]

# getUserAccountData ABI (Motor 2 — HF okuma)
GET_ACCOUNT_DATA_ABI = [{
    "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
    "name": "getUserAccountData",
    "outputs": [
        {"internalType": "uint256", "name": "totalCollateralBase",         "type": "uint256"},
        {"internalType": "uint256", "name": "totalDebtBase",               "type": "uint256"},
        {"internalType": "uint256", "name": "availableBorrowsBase",        "type": "uint256"},
        {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
        {"internalType": "uint256", "name": "ltv",                         "type": "uint256"},
        {"internalType": "uint256", "name": "healthFactor",                "type": "uint256"},
    ],
    "stateMutability": "view",
    "type": "function",
}]

# Chainlink latestRoundData ABI (HTTP başlangıç fetch için)
CHAINLINK_ROUND_ABI = [{
    "inputs": [],
    "name": "latestRoundData",
    "outputs": [
        {"name": "roundId",    "type": "uint80"},
        {"name": "answer",     "type": "int256"},
        {"name": "startedAt",  "type": "uint256"},
        {"name": "updatedAt",  "type": "uint256"},
        {"name": "answeredIn", "type": "uint80"},
    ],
    "stateMutability": "view",
    "type": "function",
}]

# WSS bağlantı parametreleri
WSS_PING_INTERVAL   = 20
WSS_PING_TIMEOUT    = 10
WSS_CLOSE_TIMEOUT   = 5
WSS_MAX_MSG_SIZE    = 2 ** 20
WSS_INITIAL_BACKOFF = 2
WSS_MAX_BACKOFF     = 30

# Volatilite Etki Filtresi — watcher.py ile aynı eşik
STABLE_HEAVY_VOLATILE_RATIO = 0.10


# ─────────────────────────────────────────────────────────────────────────────
# VERİ YAPILARI
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OracleState:
    """
    [v2] Dinamik fiyat sözlüğü.

    Eski: arb_usd, weth_usd, wsteth_usd + iki türetilmiş oran → hardcode
    Yeni: prices = {"WETH": 2500.0, "WBTC": 65000.0, "USDC": 1.0, ...}

    oracle_hub tüm Chainlink güncelleme eventlerinde ilgili alanı günceller.
    burst_fire_engine, her hedef için anlık oranı: prices[coll] / prices[debt]
    şeklinde dinamik hesaplar.
    """
    prices:       Dict[str, float] = field(default_factory=dict)
    last_updated: float            = 0.0

    def get(self, symbol: str) -> float:
        """Güvenli fiyat okuma. Bilinmeyen sembol → 0.0"""
        return self.prices.get(symbol, 0.0)

    

    def is_ready(self, coll_symbol: str, debt_symbol: str) -> bool:
        """Her iki fiyat da mevcut mu?"""
        return self.prices.get(coll_symbol, 0.0) > 0 and \
               self.prices.get(debt_symbol, 0.0) > 0



@dataclass
class HFEntry:
    symbol: str
    asset: str
    amount: float
    lt: float
    price_key: str

@dataclass
class ClusterTarget:
    address:          str
    label:            str

    coll_token:       str   = ""
    coll_address:     str   = ""
    coll_atoken:      str   = ""
    coll_decimals:    int   = 18
    coll_amount:      float = 0.0
    coll_lt:          float = 0.0
    coll_bonus:       float = 0.0

    debt_token:       str   = ""
    debt_address:     str   = ""
    debt_vtoken:      str   = ""
    debt_decimals:    int   = 18
    debt_amount:      float = 0.0

    close_factor:     float = 0.5

    hf_collaterals: Dict[str, HFEntry] = field(default_factory=dict)
    hf_debts: Dict[str, HFEntry] = field(default_factory=dict)
    in_memory_hf:         float = 999.0
    estimated_profit_usd: float = 0.0
    capped_debt_amount:   float = 0.0

    # ── Ghost Offset (Motor 2 → Motor 1 köprüsü) ──────────────────────────
    # Motor 2 on-chain getUserAccountData ile gerçek HF/borç/teminat alır.
    # Motor 1'in bildiği (CHAIN_RESERVES'daki) tokenlarla aradaki farkı
    # bu alanlar üzerinden Motor 1'e iletir. Motor 1 ağa ASLA sormaz.
    # Negatif değerler KASITLIDIR: Motor 1 oracle gecikmesiyle Aave'nin
    # internal fiyatından yüksek hesapladığında, negatif offset bunu tıraşlar.
    missing_coll_usd_lt: float = 0.0   # Bilinmeyen teminatların LT-ağırlıklı USD farkı
    missing_debt_usd:    float = 0.0   # Bilinmeyen borçların USD farkı
    is_motor2_synced:    bool  = False  # Motor 2 en az bir kez veri yazdı mı?

    def compute_hf(self, prices: Dict[str, float]) -> float:
        """
        [v5] Kurşun Geçirmez HF Hesaplama — Ghost Offset Entegrasyonu.

        1. is_motor2_synced False ise → 999.0 (Race condition önlemi)
        2. Bilinen tokenları Oracle fiyatıyla çarp
        3. Motor 2'den gelen missing_coll/debt offset'lerini EKLE
        4. Toplam borç ≤ 0 ise → 999.0 (Sıfıra bölünme koruması)
        5. Ağa HİÇBİR çağrı yapmadan sonucu döndür
        """
        # ── 1. Race condition koruması ─────────────────────────────────────
        if not self.is_motor2_synced:
            self.in_memory_hf = 999.0
            return self.in_memory_hf

        # ── 2. Bilinen tokenları Oracle'dan fiyatla ───────────────────────
        known_coll_usd_lt = 0.0
        known_debt_usd = 0.0

        for entry in self.hf_collaterals.values():
            price = prices.get(entry.price_key, 0.0)
            if price == 0.0 and entry.amount > 0:
                self.in_memory_hf = 999.0
                return self.in_memory_hf
            known_coll_usd_lt += entry.amount * price * entry.lt

        for entry in self.hf_debts.values():
            price = prices.get(entry.price_key, 0.0)
            if price == 0.0 and entry.amount > 0:
                self.in_memory_hf = 999.0
                return self.in_memory_hf
            known_debt_usd += entry.amount * price

        # ── 3. Ghost Offset ekleme (sadece + işlemi, ağa çağrı YOK) ──────
        total_coll_usd_lt = known_coll_usd_lt + self.missing_coll_usd_lt
        total_debt_usd = known_debt_usd + self.missing_debt_usd

        # ── 4. Sıfıra bölünme koruması ────────────────────────────────────
        if total_debt_usd <= 0:
            self.in_memory_hf = 999.0
        else:
            self.in_memory_hf = total_coll_usd_lt / total_debt_usd

        return self.in_memory_hf

    def recalculate(self, oracle: OracleState) -> None:
        if self.debt_amount <= 0:
            self.in_memory_hf = 999.0
            self.estimated_profit_usd = 0.0
            self.capped_debt_amount = 0.0
            return

        debt_price   = oracle.get(self.debt_token)
        covered_usd  = self.debt_amount * self.close_factor * debt_price
        
        # Kapasite kontrolü: seçili coll_token için RAM'de ne kadar bakiye varsa
        available_coll = self.hf_collaterals[self.coll_token].amount if self.coll_token in self.hf_collaterals else 0.0
        coll_price = oracle.get(self.coll_token)
        max_coll_usd = available_coll * coll_price
        
        # %0.05 güven marjı (safe margin: 0.9995) ekleyerek revert koruması
        max_coverable_usd = (max_coll_usd / (1 + self.coll_bonus)) * 0.9995
        
        if max_coverable_usd > 0 and covered_usd > max_coverable_usd:
            covered_usd = max_coverable_usd
            
        self.capped_debt_amount = covered_usd / debt_price if debt_price > 0 else 0.0
        
        gross        = covered_usd * self.coll_bonus
        flash_fee    = covered_usd * FLASH_LOAN_FEE
        slippage     = covered_usd * DEX_SLIPPAGE
        self.estimated_profit_usd = gross - flash_fee - slippage - 0.05
        
        self.compute_hf(oracle.prices)

@dataclass
class BurstState:
    target:            ClusterTarget
    bullet1_threshold: float = 1.0010
    bullet2_threshold: float = 1.0005
    bullet3_threshold: float = 1.0000
    bullet1_sent:      bool  = False
    bullet2_sent:      bool  = False
    bullet3_sent:      bool  = False
    confirmed:         bool  = False
    base_nonce:        int   = -1
    last_fire_ts:      float = 0.0

    def reset_thresholds(self) -> None:
        m1 = float(os.getenv("BURST_M1", "0.001"))
        m2 = float(os.getenv("BURST_M2", "0.0005"))
        self.bullet1_threshold = 1.0 + m1
        self.bullet2_threshold = 1.0 + m2
        self.bullet3_threshold = 1.0

        pass

    @property
    def all_sent(self) -> bool:
        return self.bullet1_sent and self.bullet2_sent and self.bullet3_sent

    @property
    def any_sent(self) -> bool:
        return self.bullet1_sent or self.bullet2_sent or self.bullet3_sent


class NonceManager:
    """
    Merkezi atomik nonce yöneticisi.

    Flash crash senaryosunda birden fazla hedefe aynı anda mermi atıldığında
    nonce çakışmasını önler. Her `reserve()` çağrısı atomik olarak bir sonraki
    nonce'u döndürür. İlk kullanımda RPC'den çekilen nonce ile başlatılır.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._current: int = -1       # -1 = henüz başlatılmadı
        self._initialized = False

    async def initialize(self, w3: AsyncWeb3, account: str) -> None:
        """RPC'den güncel nonce'u çekerek başlat. Sadece bir kez çalışır."""
        async with self._lock:
            if self._initialized:
                return
            self._current = await w3.eth.get_transaction_count(
                AsyncWeb3.to_checksum_address(account), "latest"
            )
            self._initialized = True
            logger.info("[NONCE] Başlangıç nonce = %d", self._current)

    async def reserve(self) -> int:
        """Atomik olarak bir sonraki nonce'u döndürür."""
        async with self._lock:
            if not self._initialized:
                raise RuntimeError("NonceManager henüz başlatılmadı!")
            nonce = self._current
            self._current += 1
            return nonce

    async def sync(self, w3: AsyncWeb3, account: str) -> None:
        """RPC'den güncel nonce ile yeniden senkronize et (revert sonrası)."""
        async with self._lock:
            self._current = await w3.eth.get_transaction_count(
                AsyncWeb3.to_checksum_address(account), "latest"
            )
            logger.info("[NONCE] Yeniden senkronize = %d", self._current)


@dataclass
class ClusterSniperState:
    """Tüm kümenin ortak çalışma zamanı durumu. [v3 — Multi-Chain]"""
    oracle:             OracleState
    burst_states:       List[BurstState]
    chain_cfg:          Optional[ChainConfig] = None
    chain_tag:          str   = "ARB"
    chain_id:           int   = 42161
    dry_run:            bool  = False

    # Merkezi nonce yöneticisi — tüm bullet'lar paylaşır
    nonce_manager:      NonceManager = field(default_factory=NonceManager)

    # _build_chain_data() sonuçları — runtime'da set edilir
    reserves:           List[Dict]          = field(default_factory=list)
    feed_map:           Dict[str, List[str]] = field(default_factory=dict)
    feed_type_map:      Dict[str, str]      = field(default_factory=dict)  # feed_addr → "ETH_RATIO"|"USD"
    all_feed_addresses: List[str]           = field(default_factory=list)
    all_aave_tokens:    List[str]           = field(default_factory=list)

    def get_burst(self, address: str) -> Optional[BurstState]:
        for bs in self.burst_states:
            if bs.target.address.lower() == address.lower():
                return bs
        return None


# ─────────────────────────────────────────────────────────────────────────────
# WEB3 YARDIMCILARI [DEĞİŞMEDİ]
# ─────────────────────────────────────────────────────────────────────────────

async def build_w3(rpc_url: str, label: str = "") -> AsyncWeb3:
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    if not await w3.is_connected():
        raise ConnectionError(f"RPC bağlantısı başarısız ({label}): {rpc_url[:60]}")
    chain_id = await w3.eth.chain_id
    logger.info("[W3] %s bağlandı | Chain ID: %d", label or rpc_url[:30], chain_id)
    return w3


def _decode_topic_int256(topic_hex: str) -> int:
    raw = bytes.fromhex(topic_hex.lstrip("0x").zfill(64))
    return int.from_bytes(raw, byteorder="big", signed=True)


# ─────────────────────────────────────────────────────────────────────────────
# ON-CHAIN VERİ ÇEKME
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_user_reserve(
    w3:                    AsyncWeb3,
    asset:                 str,
    user:                  str,
    data_provider_address: str,
) -> Tuple[int, int, bool]:
    """
    PoolDataProvider.getUserReserveData(asset, user) ile
    (currentATokenBalance, currentVariableDebt, usageAsCollateralEnabled) döndürür.

    [v3] data_provider_address artık parametre olarak alınır —
    her zincirin kendi DataProvider adresi ChainConfig'den gelir.
    [v5] usageAsCollateralEnabled de döndürülür — RAM HF hesabı
    için collateral-enabled olmayan varlıklar hf_collaterals'a eklenmez.
    """
    cs = AsyncWeb3.to_checksum_address
    dp = w3.eth.contract(address=cs(data_provider_address), abi=DATA_PROVIDER_ABI)
    try:
        result = await dp.functions.getUserReserveData(
            cs(asset), cs(user)
        ).call()
        atoken_bal    = result[0]  # currentATokenBalance
        variable_debt = result[2]  # currentVariableDebt
        coll_enabled  = result[8]  # usageAsCollateralEnabled
        return atoken_bal, variable_debt, coll_enabled
    except Exception as exc:
        logger.error(
            "[FETCH] getUserReserveData başarısız | asset=%s user=%s: %s",
            asset[:12], user[:12], exc,
        )
        return 0, 0, False


async def init_all_targets(w3: AsyncWeb3, cluster: ClusterSniperState) -> None:
    """
    [v4] PoolDataProvider.getUserReserveData ile pozisyon yükler.
    Auto-pair discovery: En büyük borç ve teminat tokenını otomatik seçer.
    """
    dp_addr  = cluster.chain_cfg.data_provider_address
    reserves = cluster.reserves

    logger.info("[INIT] %d hedefin pozisyonları yükleniyor...", len(cluster.burst_states))

    for bs in cluster.burst_states:
        t = bs.target

        for r in reserves:
            c_raw, d_raw, coll_enabled = await fetch_user_reserve(w3, r["asset"], t.address, dp_addr)

            if c_raw > 0 and coll_enabled:
                amount = c_raw / (10 ** r["decimals"])
                t.hf_collaterals[r["symbol"]] = HFEntry(
                    symbol=r["symbol"], asset=r["asset"], amount=amount,
                    lt=r["lt"], price_key=r["symbol"]
                )

            if d_raw > 0:
                amount = d_raw / (10 ** r["decimals"])
                t.hf_debts[r["symbol"]] = HFEntry(
                    symbol=r["symbol"], asset=r["asset"], amount=amount,
                    lt=0.0, price_key=r["symbol"]
                )

        # ── Auto-Pair Discovery ────────────────────────────────────────────
        # Henüz coll/debt token belirlenmemişse (targets.json kaynaklı),
        # on-chain veriden en büyük USD değerli çifti otomatik seç.
        if not t.coll_token and t.hf_collaterals:
            best_coll_sym = max(
                t.hf_collaterals,
                key=lambda s: t.hf_collaterals[s].amount * cluster.oracle.prices.get(s, 0.0),
            )
            best_coll_entry = t.hf_collaterals[best_coll_sym]
            coll_res = _resolve_reserve(best_coll_sym, reserves)
            if coll_res:
                t.coll_token    = best_coll_sym
                t.coll_address  = coll_res["asset"]
                t.coll_atoken   = coll_res["atoken"]
                t.coll_decimals = coll_res["decimals"]
                t.coll_lt       = coll_res["lt"]
                t.coll_bonus    = coll_res.get("bonus", LIQUIDATION_BONUS_MAP.get(best_coll_sym, DEFAULT_BONUS))

        if not t.debt_token and t.hf_debts:
            best_debt_sym = max(
                t.hf_debts,
                key=lambda s: t.hf_debts[s].amount * cluster.oracle.prices.get(s, 0.0),
            )
            best_debt_entry = t.hf_debts[best_debt_sym]
            debt_res = _resolve_reserve(best_debt_sym, reserves)
            if debt_res:
                t.debt_token    = best_debt_sym
                t.debt_address  = debt_res["asset"]
                t.debt_vtoken   = debt_res["vtoken"]
                t.debt_decimals = debt_res["decimals"]

        if t.coll_token and t.debt_token:
            t.label = f"{t.debt_token}/{t.coll_token}"



        # ── Auto-Pair Token Adresi Doğrulaması ─────────────────────────
        # Pozisyon kapanmış veya resolve başarısız olduysa adresler "" kalır.
        # Geçersiz adresle Tx inşa etmeye çalışmak 'Unknown format' hatasına
        # yol açar. Bu hedefi ölü ilan et ve atla.
        label = t.label or t.address[:12]
        _invalid_addr = False
        if not t.debt_address or not AsyncWeb3.is_address(t.debt_address):
            logger.warning(
                "[INIT] %s | debt_address geçersiz ('%s') — hedef ölü ilan edildi.",
                label, t.debt_address,
            )
            _invalid_addr = True
        if not t.coll_address or not AsyncWeb3.is_address(t.coll_address):
            logger.warning(
                "[INIT] %s | coll_address geçersiz ('%s') — hedef ölü ilan edildi.",
                label, t.coll_address,
            )
            _invalid_addr = True
        if _invalid_addr:
            bs.confirmed = True
            continue

        coll_raw = int(t.hf_collaterals[t.coll_token].amount * (10 ** t.coll_decimals)) if t.coll_token in t.hf_collaterals else 0
        debt_raw = int(t.hf_debts[t.debt_token].amount * (10 ** t.debt_decimals)) if t.debt_token in t.hf_debts else 0

        if coll_raw == 0 or debt_raw == 0:
            logger.warning(
                "[INIT] %s (%s): Teminat veya borç SIFIR — atlanıyor. "
                "coll_raw=%d debt_raw=%d",
                t.label or t.address[:12], t.address[:12], coll_raw, debt_raw,
            )
            bs.confirmed = True
            continue

        t.coll_amount = coll_raw / (10 ** t.coll_decimals)
        t.debt_amount = debt_raw / (10 ** t.debt_decimals)

        logger.info(
            "[INIT] %-14s | Teminat: %.6f %s | Borç: %.6f %s",
            t.label, t.coll_amount, t.coll_token,
            t.debt_amount, t.debt_token,
        )

    logger.info("[INIT] Tüm pozisyonlar yüklendi.")


# ─────────────────────────────────────────────────────────────────────────────
# TX İMZALAMA VE GÖNDERİM [DEĞİŞMEDİ]
# ─────────────────────────────────────────────────────────────────────────────

async def build_and_sign_bullet(
    w3:           AsyncWeb3,
    target:       ClusterTarget,
    nonce:        int,
    gas_limit:    int,
    base_fee:     int,
    gas_mult:     float,
    bullet_index: int,
    private_key:  str,
    executor:     Optional[str] = None,
) -> Optional[object]:
    """
    Tek bir bullet için TX oluşturur ve imzalar.
    Bullet'lar farklı gas premium'larıyla (1.05/1.10/1.20) inşa edilir.

    İKİ YOLLU LİKİDASYON:
      executor doluysa → AaveFlashloanSniper.sol kontratının executeSnipe()
                         fonksiyonu çağrılır. Bu yol Balancer flashloan ile
                         borç tokenini bedavaya alıp, Aave liquidationCall
                         yapıp, ganimetleri Uniswap'ta swap edip kârı sahibine
                         geri gönderir. Cüzdanda borç tokeni olmasına GEREK YOK.

      executor boşsa  → Doğrudan Aave Pool'da liquidationCall yapılır.
                         Bu yol cüzdanda yeterli borç tokeni gerektirir.

    .sol Kontrat Fonksiyon İmzası:
      executeSnipe(
          address _collateralAsset,   ← Aave'den alınacak teminat token
          address _debtAsset,         ← Borç token (flashloan ile alınacak)
          address _targetUser,        ← Tasfiye edilecek cüzdan
          uint256 _debtToCover,       ← Kapatılacak borç miktarı (wei)
          uint24  _uniswapFeeTier,    ← Uniswap V3 swap havuz komisyonu
          uint256 _minProfitAmount    ← Minimum kâr (anti-sandwich koruma)
      )
    """
    cs           = AsyncWeb3.to_checksum_address
    gas_premiums = [1.05, 1.10, 1.20]
    gas_premium  = gas_premiums[min(bullet_index, 2)]
    max_fee      = int(base_fee * gas_mult * gas_premium)
    priority_fee = int(0.01e9 * gas_premium)

    debt_to_cover_wei = int(
        target.capped_debt_amount * (10 ** target.debt_decimals)
    )

    account = AsyncWeb3.to_checksum_address(
        w3.eth.account.from_key(private_key).address
    )

    try:
        if executor:
            # ─────────────────────────────────────────────────────────────
            # FLASHLOAN YOLU: AaveFlashloanSniper.sol → executeSnipe()
            # ─────────────────────────────────────────────────────────────
            # ABI — .sol kontratındaki executeSnipe() fonksiyonuyla BİREBİR eşleşir
            exec_abi = [{
                "inputs": [
                    {"name": "_collateralAsset", "type": "address"},
                    {"name": "_debtAsset",       "type": "address"},
                    {"name": "_targetUser",      "type": "address"},
                    {"name": "_debtToCover",     "type": "uint256"},
                    {"name": "_uniswapFeeTier",  "type": "uint24"},
                    {"name": "_minProfitAmount", "type": "uint256"},
                ],
                "name": "executeSnipe",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            }]

            # Parite bazlı Uniswap V3 fee tier seç
            # Ör: WBTC/USDC → 500 (%0.05), ARB/WETH → 3000 (%0.30)
            pair_key = f"{target.coll_token}/{target.debt_token}"
            fee_tier = UNISWAP_FEE_TIER_MAP.get(pair_key, DEFAULT_UNISWAP_FEE_TIER)

            # Minimum kâr koruması — ŞİMDİLİK DEVRE DIŞI
            # Mevcut hedefler $20-50 kâr seviyesinde, sandwich botlar bu
            # kadar düşük miktarlarla ilgilenmez. minProfitAmount > 0 koymak
            # gereksiz revert riski yaratır. Büyük cüzdanlara geçince aktif edilecek.
            # Kontrat hâlâ require(amountOut >= amountToRepay) ile flashloan
            # borcunun karşılandığını garanti eder.
            min_profit_amount = 0

            exec_contract = w3.eth.contract(address=cs(executor), abi=exec_abi)
            tx = await exec_contract.functions.executeSnipe(
                cs(target.coll_address),    # _collateralAsset
                cs(target.debt_address),    # _debtAsset
                cs(target.address),         # _targetUser
                debt_to_cover_wei,          # _debtToCover
                fee_tier,                   # _uniswapFeeTier (uint24)
                min_profit_amount,          # _minProfitAmount (uint256)
            ).build_transaction({
                "from": account, "nonce": nonce,
                "gas": gas_limit, "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": priority_fee,
                "chainId": _ACTIVE_CHAIN_ID,
            })

            logger.info(
                "[TX] executeSnipe hazır | %s | borç=%d wei | fee=%d | minProfit=%d",
                target.label, debt_to_cover_wei, fee_tier, min_profit_amount,
            )
        else:
            # ─────────────────────────────────────────────────────────────
            # DİREKT YOLU: Flashloan olmadan Aave Pool.liquidationCall()
            # Cüzdanda yeterli borç tokeni olmalı!
            # ─────────────────────────────────────────────────────────────
            pool = w3.eth.contract(address=cs(_ACTIVE_POOL_ADDR), abi=LIQUIDATION_CALL_ABI)
            tx = await pool.functions.liquidationCall(
                cs(target.coll_address),
                cs(target.debt_address),
                cs(target.address),
                debt_to_cover_wei,
                False,
            ).build_transaction({
                "from": account, "nonce": nonce,
                "gas": gas_limit, "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": priority_fee,
                "chainId": _ACTIVE_CHAIN_ID,
            })

        return w3.eth.account.sign_transaction(tx, private_key)
    except Exception as exc:
        logger.error("[TX] TX inşa hatası (bullet %d, %s): %s",
                     bullet_index, target.label, exc)
        return None


async def send_bullet(
    w3:           AsyncWeb3,
    signed_tx:    object,
    target:       ClusterTarget,
    bullet_index: int,
    bs:           BurstState,
    dry_run:      bool = False,
) -> Optional[str]:
    labels = ["🔫 ÖNCÜ", "🔥 SICAK", "💀 ÖLÜMCÜL"]
    label  = labels[min(bullet_index, 2)]

    if dry_run:
        sim_hash = f"0xDRY_RUN_BULLET{bullet_index}_{target.address[:8]}"
        logger.warning("[DRY] %s bullet %d | %s | %.4f %s",
                       label, bullet_index, target.label,
                       target.debt_amount * target.close_factor, target.debt_token)
        return sim_hash

    try:
        t0 = time.perf_counter()
        tx_hash = await w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        tx_hex  = tx_hash.hex()
        t1 = time.perf_counter()
        tx_ms = (t1 - t0) * 1000.0
        logger.warning("[FIRE] %s | %s | Bullet %d | TX: %s | (TX Ağ Gönderim: %.1f ms)",
                       label, target.label, bullet_index, tx_hex, tx_ms)
        tg.send(
            f"{label} <b>[{target.label}] Bullet {bullet_index + 1}/3</b>\n"
            f"👤 <code>{target.address}</code>\n"
            f"💎 {target.debt_amount * target.close_factor:.6f} {target.debt_token}\n"
            f"🔗 <code>{tx_hex}</code>"
        )
        return tx_hex
    except Exception as exc:
        logger.error("[FIRE] Bullet %d gönderim hatası (%s): %s",
                     bullet_index, target.label, exc)
        return None


async def monitor_receipt(
    w3:           AsyncWeb3,
    tx_hex:       str,
    bs:           BurstState,
    bullet_index: int,
) -> None:
    try:
        receipt = await asyncio.wait_for(
            w3.eth.wait_for_transaction_receipt(
                bytes.fromhex(tx_hex.lstrip("0x")), poll_latency=0.3
            ),
            timeout=30.0,
        )
        target = bs.target
        if receipt["status"] == 1:
            bs.confirmed = True
            logger.warning(
                "[✅] LİKİDASYON ONAYLANDI! | %s | Gas: %d | Blok: %d",
                target.label, receipt["gasUsed"], receipt["blockNumber"],
            )
            tg.send(
                f"✅ <b>LİKİDASYON ONAYLANDI — {target.label}</b>\n"
                f"👤 <code>{target.address}</code>\n"
                f"⚡ Bullet: {bullet_index + 1}/3\n"
                f"💰 Kâr: <code>${target.estimated_profit_usd:,.2f}</code>\n"
                f"⛽ Gas: {receipt['gasUsed']:,} | Blok: {receipt['blockNumber']}\n"
                f"🔗 <code>{tx_hex}</code>"
            )
        else:
            logger.info("[REVERT] Bullet %d | %s | Blok: %d | ~$0.02",
                        bullet_index, target.label, receipt["blockNumber"])
    except asyncio.TimeoutError:
        logger.warning("[TIMEOUT] Bullet %d | %s", bullet_index, bs.target.label)
    except Exception as exc:
        logger.debug("[RECEIPT] Hata (bullet %d, %s): %s", bullet_index, bs.target.label, exc)


async def execute_burst(
    w3:            AsyncWeb3,
    bs:            BurstState,
    bullet_index:  int,
    private_key:   str,
    gas_limit:     int,
    gas_mult:      float,
    executor:      Optional[str],
    dry_run:       bool,
    nonce_manager: Optional[NonceManager] = None,
) -> None:
    """
    Tek bir bullet'ı imzalar ve gönderir.
    [v5] Merkezi NonceManager ile nonce çakışması önlenir.
    """
    target = bs.target
    if bs.confirmed:
        return

    try:
        account = AsyncWeb3.to_checksum_address(
            w3.eth.account.from_key(private_key).address
        )
    except Exception as e:
        logger.error("[BURST] Private Key hatası: .env dosyasındaki SNIPER_PRIVATE_KEY hatalı veya eksik. Lütfen geçerli bir key girin.")
        return

    # ── Merkezi nonce yönetimi ─────────────────────────────────────────────
    if nonce_manager is not None:
        # NonceManager henüz başlatılmadıysa ilk kullanımda başlat
        try:
            await nonce_manager.initialize(w3, account)
        except Exception as exc:
            logger.error("[BURST] NonceManager başlatılamadı: %s", exc)
            return
        try:
            nonce = await nonce_manager.reserve()
        except Exception as exc:
            logger.error("[BURST] Nonce rezerve edilemedi (%s): %s", target.label, exc)
            return
    else:
        # Fallback: eski per-target nonce (geriye uyumluluk)
        if bs.base_nonce == -1:
            try:
                bs.base_nonce = await w3.eth.get_transaction_count(account, "latest")
            except Exception as exc:
                logger.error("[BURST] Nonce okunamadı (%s): %s", target.label, exc)
                return
        nonce = bs.base_nonce + bullet_index

    # ── Tx İnşa İzolasyonu ─────────────────────────────────────────────────
    # Hatalı veri (boş adres, geçersiz format) geldiğinde tüm döngüyü
    # çökertmek yerine sadece bu hedefi atla ve diğerlerine devam et.
    try:
        try:
            block    = await w3.eth.get_block("latest")
            base_fee = block.get("baseFeePerGas", 100_000_000)
        except Exception:
            base_fee = 100_000_000

        t_b0 = time.perf_counter()
        signed = await build_and_sign_bullet(
            w3, target, nonce, gas_limit, base_fee, gas_mult,
            bullet_index, private_key, executor,
        )
        t_b1 = time.perf_counter()
        build_ms = (t_b1 - t_b0) * 1000.0
        if not signed:
            return
    except Exception as e:
        logger.error(f"Tx inşa hatası atlandı: {e}")
        return
        
    logger.info("[BURST] %s | Mermi inşa ve imzalama: %.1f ms | nonce=%d", target.label, build_ms, nonce)

    tx_hex = await send_bullet(w3, signed, target, bullet_index, bs, dry_run)
    if not tx_hex:
        return

    if not dry_run:
        asyncio.create_task(
            monitor_receipt(w3, tx_hex, bs, bullet_index),
            name=f"receipt-{target.label}-b{bullet_index}",
        )

    bs.last_fire_ts = time.time()


# ─────────────────────────────────────────────────────────────────────────────
# ORACLE HUB [v2 — Dinamik Feed Aboneliği]
# ─────────────────────────────────────────────────────────────────────────────

async def oracle_hub(
    state:       ClusterSniperState,
    wss_url:     str,
    price_queue: asyncio.Queue,
) -> None:
    """
    Motor 1'in gözleri: CHAIN_RESERVES'dan toplanan TÜM Chainlink
    feed'lerini tek bir WSS aboneliğiyle dinler.

    FEED_MAP'teki tüm benzersiz feed adresleri → ALL_FEED_ADDRESSES

    Her AnswerUpdated event'inde:
      1. Hangi feed → hangi token: FEED_MAP ile çöz
      2. oracle.prices[token_symbol] = yeni_fiyat
      3. price_queue'ya tüm fiyatların anlık kopyasını at

    burst_fire_engine, snapshot'taki prices dict'ten her hedef için
    coll_usd/debt_usd oranını dinamik hesaplar.
    """
    tag     = "ORACLE-HUB"
    backoff = WSS_INITIAL_BACKOFF

    # Zincire özgü feed verileri state'ten okunur
    FEED_MAP           = state.feed_map
    ALL_FEED_ADDRESSES = state.all_feed_addresses

    logger.info("[%s] %d Chainlink feed dinleniyor: %s",
                tag, len(ALL_FEED_ADDRESSES),
                [FEED_MAP.get(a.lower(), "?") for a in ALL_FEED_ADDRESSES])

    while True:
        try:
            async with websockets.connect(
                wss_url,
                ping_interval = WSS_PING_INTERVAL,
                ping_timeout  = WSS_PING_TIMEOUT,
                close_timeout = WSS_CLOSE_TIMEOUT,
                max_size      = WSS_MAX_MSG_SIZE,
            ) as ws:

                # Tüm benzersiz feed adresleri tek abonelikte
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1,
                    "method":  "eth_subscribe",
                    "params":  [
                        "logs",
                        {
                            "address": ALL_FEED_ADDRESSES,
                            "topics":  [CHAINLINK_ANSWER_UPDATED],
                        },
                    ],
                }))

                raw_resp = await asyncio.wait_for(ws.recv(), timeout=10.0)
                resp     = json.loads(raw_resp)
                if "error" in resp:
                    raise ValueError(f"eth_subscribe hatası: {resp['error']}")

                sub_id = resp.get("result", "")
                logger.info("[%s] ✅ Bağlandı. %d feed | sub_id=%s...",
                            tag, len(ALL_FEED_ADDRESSES), sub_id[:12])
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

                    result     = params.get("result", {})
                    event_addr = result.get("address", "").lower()
                    topics     = result.get("topics", [])

                    # FEED_MAP'ten hangi token'ın güncellendiğini bul
                    token_symbols = FEED_MAP.get(event_addr)
                    if not token_symbols or len(topics) < 2:
                        continue

                    try:
                        raw_price = _decode_topic_int256(topics[1])

                        # [v5] Deterministik LST fiyat çözünürlüğü
                        # feed_type_map: "ETH_RATIO" → oran × WETH_USD, "USD" → standart 8-dec
                        feed_type = state.feed_type_map.get(event_addr, "USD")
                        if feed_type == "ETH_RATIO":
                            ratio = raw_price / 10**18
                            weth_price = state.oracle.prices.get("WETH", 0.0)
                            if weth_price <= 0:
                                continue  # WETH fiyatı yoksa LST hesaplanamaz
                            price_usd = ratio * weth_price
                        else:
                            price_usd = raw_price / CHAINLINK_DECIMALS

                    except Exception as dec_exc:
                        logger.debug("[%s] Decode hatası: %s", tag, dec_exc)
                        continue

                    if price_usd <= 0:
                        continue

                    # Fiyatı güncelle
                    for token_symbol in token_symbols:
                        state.oracle.prices[token_symbol] = price_usd
                        logger.debug("[%s] %s = $%.4f", tag, token_symbol, price_usd)
                    state.oracle.last_updated = time.time()

                    # Kuyruğa tüm güncel fiyatların kopyasını at
                    # burst_fire_engine bu dict'ten ratio'ları hesaplar
                    if price_queue.qsize() >= 64:
                        try:
                            price_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    try:
                        price_queue.put_nowait({
                            "prices": dict(state.oracle.prices),
                            "ts":     state.oracle.last_updated,
                        })
                    except asyncio.QueueFull:
                        pass

        except asyncio.CancelledError:
            logger.info("[%s] Task iptal edildi.", tag)
            return
        except Exception as exc:
            logger.warning("[%s] WSS koptu: %s. %ds reconnect...", tag, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WSS_MAX_BACKOFF)


# ─────────────────────────────────────────────────────────────────────────────
# STATE WATCHER [v2 — Dinamik Token Adresleri]
# ─────────────────────────────────────────────────────────────────────────────

async def state_change_watcher(
    state:        ClusterSniperState,
    w3_alchemy:   AsyncWeb3,
    wss_url:      str,
    trigger_lock: asyncio.Lock,
) -> None:
    tag     = "STATE-WATCHER"
    backoff = WSS_INITIAL_BACKOFF

    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    def _addr_to_topic(addr: str) -> str:
        return "0x" + "0" * 24 + addr.lower().lstrip("0x")

    # Zincire özgü Aave token adresleri state'ten okunur
    ALL_AAVE_TOKENS = state.all_aave_tokens
    RESERVES        = state.reserves

    target_topic_map: Dict[str, List[BurstState]] = {}
    for bs in state.burst_states:
        if not bs.confirmed:
            topic = _addr_to_topic(bs.target.address)
            target_topic_map.setdefault(topic, []).append(bs)

    logger.info("[%s] %d hedef, %d Aave token Transfer izlemesi.",
                tag, len(target_topic_map), len(ALL_AAVE_TOKENS))

    while True:
        try:
            async with websockets.connect(
                wss_url,
                ping_interval=10,
                ping_timeout=10,
                max_size=2**20,
            ) as ws:
                logger.info("[%s] WSS Bağlandı.", tag)
                
                sub_request = {
                    "id": 2,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": ALL_AAVE_TOKENS,
                            "topics": [transfer_topic]
                        }
                    ]
                }
                await ws.send(json.dumps(sub_request))
                
                async for message in ws:
                    data = json.loads(message)
                    if "params" not in data:
                        continue
                        
                    result = data["params"]["result"]
                    topics = result.get("topics", [])
                    if not topics or len(topics) < 3:
                        continue
                        
                    from_topic = topics[1]
                    to_topic = topics[2]
                    
                    affected = {}
                    if from_topic in target_topic_map:
                        for bs in target_topic_map[from_topic]:
                            affected[bs.target.address] = bs
                    if to_topic in target_topic_map:
                        for bs in target_topic_map[to_topic]:
                            affected[bs.target.address] = bs
                            
                    if not affected:
                        continue
                        
                    for bs in affected.values():
                        if bs.confirmed:
                            continue
                        t = bs.target
                        
                        ev_addr = result.get("address", "").lower()
                        matched_res = None
                        for r in RESERVES:
                            if ev_addr == r["atoken"].lower() or ev_addr == r["vtoken"].lower():
                                matched_res = r
                                break
                                
                        if not matched_res:
                            continue

                        c_raw, d_raw, _ = await fetch_user_reserve(
                            w3_alchemy, matched_res["asset"], t.address,
                            state.chain_cfg.data_provider_address,
                        )
                        
                        sym = matched_res["symbol"]
                        if c_raw > 0:
                            amount = c_raw / (10 ** matched_res["decimals"])
                            t.hf_collaterals[sym] = HFEntry(
                                symbol=sym, asset=matched_res["asset"], amount=amount, 
                                lt=matched_res["lt"], price_key=sym
                            )
                        elif sym in t.hf_collaterals:
                            del t.hf_collaterals[sym]
                            
                        if d_raw > 0:
                            amount = d_raw / (10 ** matched_res["decimals"])
                            t.hf_debts[sym] = HFEntry(
                                symbol=sym, asset=matched_res["asset"], amount=amount, 
                                lt=0.0, price_key=sym
                            )
                        elif sym in t.hf_debts:
                            del t.hf_debts[sym]

                        if sym == t.coll_token:
                            t.coll_amount = c_raw / (10 ** t.coll_decimals)
                        if sym == t.debt_token:
                            t.debt_amount = d_raw / (10 ** t.debt_decimals)

                        old_hf = t.in_memory_hf
                        t.compute_hf(state.oracle.prices)
                        t.recalculate(state.oracle)
                        bs.reset_thresholds()
                        
                        logger.info(
                            "[%s] %s | HF: %.6f → %.6f | Kâr: $%.2f",
                            tag, t.label, old_hf, t.in_memory_hf,
                            t.estimated_profit_usd,
                        )

        except Exception as e:
            logger.error("[%s] WSS Hata: %s | Yeniden bağlanılıyor...", tag, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WSS_MAX_BACKOFF)

# ─────────────────────────────────────────────────────────────────────────────
# Motor 1: Burst Fire Engine (Kör Nişancı)
# ─────────────────────────────────────────────────────────────────────────────

async def burst_fire_engine(
    state:       ClusterSniperState,
    w3:          AsyncWeb3,
    private_key: str,
    executor:    str,
) -> None:
    tag = "Motor1(Burst)"
    if state.dry_run:
        logger.info("[%s] DRY_RUN = True. Tx atılmayacak.", tag)

    w3_alchemy = AsyncWeb3(AsyncHTTPProvider(state.chain_cfg.rpc_url))
    base_gas_limit = 2000000

    while True:
        try:
            prices = dict(state.oracle.prices)
            nm = state.nonce_manager  # Merkezi nonce yöneticisi

            for bs in state.burst_states:
                if bs.confirmed:
                    continue

                t = bs.target

                # ── Race Condition Koruması (Token Adresi) ─────────────
                # targets_json_watcher hedefi listeye ekler ama
                # init_all_targets 4-5 sn sürer. Bu boşlukta debt_token
                # ve coll_token henüz "" (boş) kalır. Boş adresle TX
                # inşa etmeye çalışmak çökmeye yol açar → atla.
                if not t.debt_token or not t.coll_token:
                    continue

                current_hf = t.compute_hf(prices)

                # ── Race Condition Koruması (HF) ───────────────────────
                # HF == 0.0 veya None → oracle henüz güncellenmemiş demek.
                # Bu durumu tasfiye eşiğinin altı sanıp ateş etmeyi önle.
                if current_hf is None or current_hf == 0.0:
                    continue

                if current_hf > bs.bullet1_threshold:
                    continue

                if not bs.bullet1_sent and current_hf <= bs.bullet1_threshold:
                    logger.warning(
                        "[%s] 🔫 BULLET 1 ÖNCÜ | %-14s | HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet1_threshold
                    )
                    bs.bullet1_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 0, private_key, base_gas_limit, 1.0, executor, state.dry_run, nm),
                        name=f"burst-{t.label}-b0",
                    )

                if (bs.bullet1_sent and not bs.bullet2_sent and current_hf <= bs.bullet2_threshold):
                    logger.warning(
                        "[%s] 🔥 BULLET 2 SICAK | %-14s | HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet2_threshold,
                    )
                    bs.bullet2_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 1, private_key, base_gas_limit, 1.0, executor, state.dry_run, nm),
                        name=f"burst-{t.label}-b1",
                    )

                if (bs.bullet2_sent and not bs.bullet3_sent and current_hf <= bs.bullet3_threshold):
                    logger.warning(
                        "[%s] 💀 BULLET 3 ÖLÜMCÜL | %-14s | HF=%.6f ≤ eşik=%.6f",
                        tag, t.label, current_hf, bs.bullet3_threshold,
                    )
                    bs.bullet3_sent = True
                    asyncio.create_task(
                        execute_burst(w3, bs, 2, private_key, base_gas_limit, 1.0, executor, state.dry_run, nm),
                        name=f"burst-{t.label}-b2",
                    )

            await asyncio.sleep(0.001)

        except Exception as e:
            logger.error("[%s] Motor hatası: %s", tag, e)
            await asyncio.sleep(1)


async def motor2_hf_monitor(
    state:         ClusterSniperState,
    w3_fast:       AsyncWeb3,
    private_key:   str,
    gas_limit:     int,
    gas_mult:      float,
    executor:      Optional[str],
    poll_interval: float = 0.4,
) -> None:
    """
    Tüm hedeflerin HF'sini tek Multicall ile okur.
    HF < 1.00 → burst başlat veya sigorta mermisi. [DEĞİŞMEDİ]
    """
    tag = "MOTOR2-HF"
    logger.info("[%s] Multicall HF polling başladı (%.1fs)", tag, poll_interval)

    pool_addr  = state.chain_cfg.pool_address
    cs         = AsyncWeb3.to_checksum_address
    multicall  = w3_fast.eth.contract(address=cs(MULTICALL3_ADDR), abi=MULTICALL3_ABI)
    pool_dummy = w3_fast.eth.contract(address=cs(pool_addr),       abi=GET_ACCOUNT_DATA_ABI)

    while True:
        await asyncio.sleep(poll_interval)

        active = [bs for bs in state.burst_states if not bs.confirmed]
        if not active:
            logger.info("[%s] Tüm hedefler tamamlandı — Motor 2 durdu.", tag)
            return

        try:
            calls = [
                (cs(pool_addr),
                 pool_dummy.encode_abi("getUserAccountData",
                                       args=[cs(bs.target.address)]))
                for bs in active
            ]
            t_mc0 = time.perf_counter()
            results = await multicall.functions.tryAggregate(False, calls).call()
            t_mc1 = time.perf_counter()
            mc_ms = (t_mc1 - t_mc0) * 1000.0

            for i, (success, return_data) in enumerate(results):
                if not success or not return_data:
                    continue

                bs = active[i]
                try:
                    decoded = abi_decode(
                        ["uint256","uint256","uint256","uint256","uint256","uint256"],
                        return_data,
                    )
                    hf_wei = decoded[5]
                    if hf_wei >= (2 ** 256 - 1):
                        continue
                    hf = hf_wei / WAD
                except Exception:
                    continue

                # ── Race Condition Koruması ─────────────────────────────
                # HF == 0.0 → on-chain veri henüz geçerli değil, atla.
                if hf == 0.0:
                    continue

                # ── Ghost Offset Hesaplama ──────────────────────────────
                # Motor 2 istihbarat görevi: on-chain gerçek değerlerle
                # Motor 1'in bildiği değerler arasındaki farkı hesapla.
                # Bu fark (missing_*) Motor 1'e yazılır, Motor 1 ağa
                # ASLA sormadan bu offset'i toplama olarak kullanır.
                t = bs.target
                real_debt_usd = decoded[1] / USD_DECIMALS  # totalDebtBase (8-dec)
                real_coll_usd_lt = hf * real_debt_usd      # HF = coll_lt / debt

                # Motor 1'in bildiği değerleri güncel oracle fiyatlarıyla hesapla
                prices = state.oracle.prices
                known_coll_usd_lt = sum(
                    e.amount * prices.get(e.price_key, 0.0) * e.lt
                    for e in t.hf_collaterals.values()
                )
                known_debt_usd = sum(
                    e.amount * prices.get(e.price_key, 0.0)
                    for e in t.hf_debts.values()
                )

                # Ghost Offset = fark (NEGATİF OLABİLİR — kasıtlıdır)
                # Negatif: Motor 1 oracle gecikmesiyle Aave'den yüksek hesaplıyor → tıraşla
                # Pozitif: Bilinmeyen tokenlar (AERO, dust) → ekle
                t.missing_coll_usd_lt = real_coll_usd_lt - known_coll_usd_lt
                t.missing_debt_usd    = real_debt_usd - known_debt_usd
                t.is_motor2_synced    = True

                logger.debug(
                    "[%s] %s | HF: %.6f | ghost_coll=%.2f ghost_debt=%.2f",
                    tag, t.label, hf, t.missing_coll_usd_lt, t.missing_debt_usd,
                )

                if hf >= 1.10 and bs.any_sent and not getattr(state, "benchmark", False):
                    logger.info("[%s] 🟢 %s HF güvenli (%.4f) — tasfiye tamamlandı.",
                                tag, bs.target.label, hf)
                    bs.confirmed = True
                    continue

                benchmark_limit = 999.0 if getattr(state, "benchmark", False) else 1.00
                if hf < benchmark_limit:
                    t = bs.target

                    # ── Race Condition Koruması (Token Adresi) ─────────
                    # debt_token/coll_token henüz init edilmediyse TX
                    # inşa etme — boş adresle çökmeyi önle.
                    if not t.debt_token or not t.coll_token:
                        continue

                    if bs.all_sent and not getattr(state, "benchmark", False):
                        continue

                    logger.warning(
                        "[%s] 🎯 HF=%.4f < 1.00 ONAYLANDI | %s | b1=%s b2=%s b3=%s (Multicall gecikmesi: %.1f ms)",
                        tag, hf, t.label,
                        bs.bullet1_sent, bs.bullet2_sent, bs.bullet3_sent, mc_ms
                    )

                    for bullet_idx in range(3):
                        flags = [bs.bullet1_sent, bs.bullet2_sent, bs.bullet3_sent]
                        if not flags[bullet_idx] or getattr(state, "benchmark", False):
                            if bullet_idx == 0:
                                bs.bullet1_sent = True
                            elif bullet_idx == 1:
                                bs.bullet2_sent = True
                            else:
                                bs.bullet3_sent = True

                            asyncio.create_task(
                                execute_burst(
                                    w3_fast, bs, bullet_idx, private_key,
                                    gas_limit, gas_mult * 1.1,
                                    executor, state.dry_run,
                                    state.nonce_manager,
                                ),
                                name=f"m2-burst-{t.label}-b{bullet_idx}",
                            )
                            await asyncio.sleep(0.05)

        except asyncio.CancelledError:
            logger.info("[%s] Task iptal edildi.", tag)
            return
        except Exception as exc:
            logger.error("[%s] Multicall hatası: %s", tag, exc)


# ─────────────────────────────────────────────────────────────────────────────
# TARGETS.JSON WATCHER — Periyodik Güncelleme (v4)
# ─────────────────────────────────────────────────────────────────────────────

async def targets_json_watcher(
    state:       ClusterSniperState,
    w3:          AsyncWeb3,
) -> None:
    """
    Her TARGETS_POLL_INTERVAL saniyede targets.json'u okur.
    Yeni hedefler → on-chain keşif + cluster'a ekle.
    Kaldırılan hedefler → confirmed olarak işaretle.
    """
    tag = "TARGETS-WATCHER"
    logger.info(
        "[%s] Başladı — %ds aralıkla targets.json kontrol edilecek.",
        tag, TARGETS_POLL_INTERVAL,
    )

    known_addresses: Set[str] = {
        bs.target.address.lower() for bs in state.burst_states
    }

    while True:
        await asyncio.sleep(TARGETS_POLL_INTERVAL)

        try:
            fresh = read_targets_json(state.chain_tag)
            fresh_lower = {addr.lower(): addr for addr in fresh}

            # ── Yeni hedefler ──────────────────────────────────────────────
            new_addrs = set(fresh_lower.keys()) - known_addresses
            if new_addrs:
                logger.info("[%s] %d YENİ hedef tespit edildi.", tag, len(new_addrs))

                reserves = state.reserves
                new_burst_states: List[BurstState] = []

                for addr_low in new_addrs:
                    raw_addr = fresh_lower[addr_low]
                    try:
                        address = AsyncWeb3.to_checksum_address(raw_addr)
                    except Exception:
                        continue

                    target = ClusterTarget(
                        address = address,
                        label   = address[:12],
                    )

                    # ── OptiPair pair bilgisini pre-populate et ──────────
                    tdata = fresh.get(raw_addr, {})
                    coll_sym = tdata.get("collateral_asset")
                    debt_sym = tdata.get("debt_asset")

                    if coll_sym:
                        coll_res = _resolve_reserve(coll_sym, reserves)
                        if coll_res:
                            canonical_coll = coll_res["symbol"]
                            target.coll_token    = canonical_coll
                            target.coll_address  = coll_res["asset"]
                            target.coll_atoken   = coll_res["atoken"]
                            target.coll_decimals = coll_res["decimals"]
                            target.coll_lt       = coll_res["lt"]
                            target.coll_bonus    = coll_res.get(
                                "bonus", LIQUIDATION_BONUS_MAP.get(canonical_coll, DEFAULT_BONUS),
                            )
                    if debt_sym:
                        debt_res = _resolve_reserve(debt_sym, reserves)
                        if debt_res:
                            canonical_debt = debt_res["symbol"]
                            target.debt_token    = canonical_debt
                            target.debt_address  = debt_res["asset"]
                            target.debt_vtoken   = debt_res["vtoken"]
                            target.debt_decimals = debt_res["decimals"]

                    if tdata.get("bonus_pct") is not None and target.coll_token:
                        target.coll_bonus = tdata["bonus_pct"] / 100.0
                    if tdata.get("effective_close_factor") is not None:
                        target.close_factor = tdata["effective_close_factor"]

                    if target.coll_token and target.debt_token:
                        target.label = f"{target.debt_token}/{target.coll_token}"

                    bs = BurstState(target=target)
                    new_burst_states.append(bs)
                    state.burst_states.append(bs)
                    known_addresses.add(addr_low)

                # Yeni hedefler için on-chain pozisyon keşfi
                if new_burst_states:
                    temp_state = ClusterSniperState(
                        oracle=state.oracle,
                        burst_states=new_burst_states,
                        chain_cfg=state.chain_cfg,
                        chain_tag=state.chain_tag,
                        reserves=state.reserves,
                        feed_map=state.feed_map,
                        feed_type_map=state.feed_type_map,
                        all_feed_addresses=state.all_feed_addresses,
                        all_aave_tokens=state.all_aave_tokens,
                    )
                    await init_all_targets(w3, temp_state)

                    for bs in new_burst_states:
                        if not bs.confirmed:
                            bs.target.recalculate(state.oracle)
                            bs.reset_thresholds()

                    valid = sum(1 for bs in new_burst_states if not bs.confirmed)
                    logger.info(
                        "[%s] %d yeni hedef eklendi (%d geçerli, %d atlandı).",
                        tag, len(new_burst_states), valid,
                        len(new_burst_states) - valid,
                    )

            # ── Kaldırılan hedefler ────────────────────────────────────────
            removed_addrs = known_addresses - set(fresh_lower.keys())
            if removed_addrs:
                for bs in state.burst_states:
                    if bs.target.address.lower() in removed_addrs and not bs.confirmed:
                        bs.confirmed = True
                        logger.info(
                            "[%s] KALDIRILDI | %s | targets.json'dan silindi.",
                            tag, bs.target.label,
                        )
                known_addresses -= removed_addrs

            active_count = sum(1 for bs in state.burst_states if not bs.confirmed)
            logger.debug(
                "[%s] Kontrol tamamlandı | Aktif: %d | Toplam: %d",
                tag, active_count, len(state.burst_states),
            )

        except asyncio.CancelledError:
            logger.info("[%s] Task iptal edildi.", tag)
            return
        except Exception as exc:
            logger.error("[%s] Hata: %s", tag, exc)


# ─────────────────────────────────────────────────────────────────────────────
# CLUSTER YÜKLEME [v4 — targets.json]
# ─────────────────────────────────────────────────────────────────────────────

# watcher.py OptiPairEngine ile aynı normalleştirme tablosu.
# On-chain semboller Unicode veya bridge ekleri içerebilir;
# CHAIN_RESERVES'daki kanonik isimlerle eşleştirmek gerekir.
_SYMBOL_NORMALIZE: Dict[str, str] = {
    "USD₮0": "USDT", "USDT0": "USDT", "USDt": "USDT",
    "USDT.e": "USDT", "USDTe": "USDT",
    "USDC0": "USDC", "USDCn": "USDC",
}


def _resolve_reserve(symbol: str, reserves: List[Dict]) -> Optional[Dict]:
    """
    Reserve tablosundan sembolle eşleşen kaydı döndürür.
    Önce orijinal sembolü dener, bulamazsa normalleştirilmiş
    halini dener (USD₮0 → USDT, USDCn → USDC vb.)
    """
    for r in reserves:
        if r["symbol"] == symbol:
            return r
    # Normalleştirilmiş sembol ile tekrar dene
    normalized = _SYMBOL_NORMALIZE.get(symbol)
    if normalized:
        for r in reserves:
            if r["symbol"] == normalized:
                return r
    return None


def load_cluster_from_targets_json(chain_tag: str, chain_cfg: ChainConfig) -> ClusterSniperState:
    """
    [v4] Hedefleri targets.json'dan yükler.

    [v4.1] OptiPair Entegrasyonu:
      targets.json'daki collateral_asset / debt_asset / bonus_pct alanları
      ClusterTarget'a pre-populate edilir. init_all_targets bu alanları
      dolu görüp auto-pair discovery'yi atlar → watcher.py'nin OptiPair
      motoru tarafından seçilen optimal çift korunur.

      Eski davranış: pair bilgisi okunuyor ama kullanılmıyordu →
      init_all_targets kendi "en büyük bakiye" keşfini yapıyordu → YANLIŞ ÇİFT.
    """
    oracle       = OracleState()
    burst_states: List[BurstState] = []

    targets_data = read_targets_json(chain_tag)

    chain_reserves, chain_feed_map, chain_feed_type_map, chain_feed_addrs, chain_aave_tokens = _build_chain_data(chain_tag)

    for raw_addr, tdata in targets_data.items():
        try:
            address = AsyncWeb3.to_checksum_address(raw_addr)
        except Exception:
            logger.warning("[LOAD] Geçersiz adres: %s", raw_addr)
            continue

        target = ClusterTarget(
            address    = address,
            label      = address[:12],
        )

        # ── OptiPair Pair Bilgisini Pre-Populate Et ────────────────────────
        # targets.json'daki collateral_asset / debt_asset sembollerini
        # CHAIN_RESERVES tablosundan çözerek ClusterTarget alanlarını doldur.
        # init_all_targets "if not t.coll_token" kontrolüyle bu alanları
        # dolu görüp auto-discovery'yi atlayacak.
        coll_sym = tdata.get("collateral_asset")
        debt_sym = tdata.get("debt_asset")

        if coll_sym:
            coll_res = _resolve_reserve(coll_sym, chain_reserves)
            if coll_res:
                # Kanonik sembol kullan (USD₮0 → USDT gibi)
                canonical_coll = coll_res["symbol"]
                target.coll_token    = canonical_coll
                target.coll_address  = coll_res["asset"]
                target.coll_atoken   = coll_res["atoken"]
                target.coll_decimals = coll_res["decimals"]
                target.coll_lt       = coll_res["lt"]
                target.coll_bonus    = coll_res.get(
                    "bonus", LIQUIDATION_BONUS_MAP.get(canonical_coll, DEFAULT_BONUS),
                )
            else:
                logger.warning(
                    "[LOAD] %s | collateral_asset='%s' CHAIN_RESERVES'da bulunamadı — "
                    "init_all_targets auto-discovery yapacak.",
                    address[:12], coll_sym,
                )

        if debt_sym:
            debt_res = _resolve_reserve(debt_sym, chain_reserves)
            if debt_res:
                # Kanonik sembol kullan
                canonical_debt = debt_res["symbol"]
                target.debt_token    = canonical_debt
                target.debt_address  = debt_res["asset"]
                target.debt_vtoken   = debt_res["vtoken"]
                target.debt_decimals = debt_res["decimals"]
            else:
                logger.warning(
                    "[LOAD] %s | debt_asset='%s' CHAIN_RESERVES'da bulunamadı — "
                    "init_all_targets auto-discovery yapacak.",
                    address[:12], debt_sym,
                )

        # OptiPair bonus ve close factor
        if tdata.get("bonus_pct") is not None and target.coll_token:
            target.coll_bonus = tdata["bonus_pct"] / 100.0
        if tdata.get("effective_close_factor") is not None:
            target.close_factor = tdata["effective_close_factor"]

        # Label: pair doluysa sembol etiketini koy
        if target.coll_token and target.debt_token:
            target.label = f"{target.debt_token}/{target.coll_token}"

        burst_states.append(BurstState(target=target))

    if not burst_states:
        logger.warning("[%s] targets.json'da bu ağ için hedef bulunamadı. Watcher bekleniyor...", chain_tag)

    pre_paired = sum(
        1 for bs in burst_states
        if bs.target.coll_token and bs.target.debt_token
    )
    logger.info(
        "[LOAD] %s: %d hedef yüklendi, %d/%d pair pre-populated (OptiPair).",
        chain_tag, len(burst_states), pre_paired, len(burst_states),
    )

    state = ClusterSniperState(
        oracle=oracle,
        burst_states=burst_states,
        chain_cfg=chain_cfg,
        chain_tag=chain_tag,
        reserves=chain_reserves,
        feed_map=chain_feed_map,
        feed_type_map=chain_feed_type_map,
        all_feed_addresses=chain_feed_addrs,
        all_aave_tokens=chain_aave_tokens,
    )
    return state


def log_cluster_banner(state: ClusterSniperState, fast_rpc: str) -> None:
    logger.info("=" * 76)
    logger.info("  AAVE V3 %s CLUSTER DUAL-ENGINE SNIPER v4 — %d HEDEF",
                state.chain_tag, len(state.burst_states))
    logger.info("  Kaynak: targets.json (watcher.py köprüsü)")
    logger.info("  Güncelleme Aralığı: %ds", TARGETS_POLL_INTERVAL)
    logger.info("  Dry Run: %s", "EVET — TX GÖNDERİLMEZ" if state.dry_run else "HAYIR")
    logger.info("  Fast RPC: %s...", fast_rpc[:55])
    logger.info("─" * 76)
    logger.info("  %-14s | %-42s | TEMİNAT       | BORÇ",
                "LABEL", "ADRES")
    logger.info("─" * 76)
    for bs in state.burst_states:
        t = bs.target
        if bs.confirmed or not t.coll_token:
            logger.info(
                "  %-14s | %s | (on-chain yok / atlandı)",
                (t.label or t.address[:12]), t.address[:12],
            )
            continue
        logger.info(
            "  %-14s | %s | %-6s LT=%.0f%% Bns=%.0f%% | %-6s CF=%.0f%%",
            t.label, t.address[:12],
            t.coll_token, t.coll_lt * 100, t.coll_bonus * 100,
            t.debt_token, t.close_factor * 100,
        )
    logger.info("─" * 76)
    logger.info("  İzlenen Chainlink Feed'leri (%d benzersiz):", len(state.all_feed_addresses))
    for feed_addr in state.all_feed_addresses:
        symbol = state.feed_map.get(feed_addr.lower(), "?")
        logger.info("    %-6s → %s", symbol, feed_addr)
    logger.info("=" * 76)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN [v3 — Multi-Chain]
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Aave V3 Multi-Chain Cluster Sniper v4")
    p.add_argument("--chain",     type=str, default="ARB",
                   choices=["ARB", "BASE", "OP"],
                   help="Hedef ağ (varsayılan: ARB)")
    p.add_argument("--dry",       action="store_true", help="TX imzalamaz")
    p.add_argument("--skip-init", action="store_true", help="On-chain fetch atla")
    p.add_argument("--benchmark", action="store_true",
                   help="Test icin baglanir, motor2'yi devamli atesler. (DRY RUN aktif olur)")
    return p.parse_args()


async def main() -> None:
    global _ACTIVE_CHAIN_ID, _ACTIVE_POOL_ADDR

    args = parse_args()
    chain_tag = args.chain.upper()

    # ── ChainConfig çözümleme ─────────────────────────────────────────────────
    all_chains = load_chains()
    chain_cfg  = next((c for c in all_chains if c.tag == chain_tag), None)
    if not chain_cfg:
        logger.error("❌ %s ağı için ChainConfig bulunamadı! .env'de %s_RPC tanımlı mı?", chain_tag, chain_tag)
        sys.exit(1)

    # ── Env değişkenleri ──────────────────────────────────────────────────────
    private_key  = os.getenv("SNIPER_PRIVATE_KEY", "")
    # Chain-bazli FAST_RPC: once {CHAIN}_FAST_RPC, yoksa FAST_RPC, yoksa normal RPC
    fast_rpc     = os.getenv(f"{chain_tag}_FAST_RPC",
                             os.getenv("FAST_RPC", chain_cfg.rpc_url))
    gas_limit    = int(os.getenv("GAS_LIMIT",       "900000"))
    gas_mult     = float(os.getenv("GAS_MULTIPLIER", "1.2"))
    # Chain-bazli EXECUTOR: once {CHAIN}_EXECUTOR, yoksa AAVE_EXECUTOR
    executor     = os.getenv(f"{chain_tag}_EXECUTOR",
                             os.getenv("AAVE_EXECUTOR", ""))

    errors = []
    if not chain_cfg.rpc_url:
        errors.append(f"{chain_tag}_RPC eksik")
    if not chain_cfg.wss_url:
        errors.append(f"{chain_tag}_WSS eksik")
    if not private_key and not args.dry:
        errors.append("SNIPER_PRIVATE_KEY eksik")
    if errors:
        for e in errors:
            logger.error("❌ %s", e)
        sys.exit(1)

    # ── Cluster yükle (targets.json'dan) ──────────────────────────────────────
    cluster_state = load_cluster_from_targets_json(chain_tag, chain_cfg)
    cluster_state.dry_run = args.dry or args.benchmark
    cluster_state.benchmark = getattr(args, "benchmark", False)

    # ── Modül-düzeyi runtime değişkenleri set et ──────────────────────────────
    w3_temp = await build_w3(chain_cfg.rpc_url, f"{chain_tag}-ChainID")
    _ACTIVE_CHAIN_ID  = await w3_temp.eth.chain_id
    _ACTIVE_POOL_ADDR = chain_cfg.pool_address
    cluster_state.chain_id = _ACTIVE_CHAIN_ID

    # ── Web3 bağlantıları ─────────────────────────────────────────────────────
    w3_alchemy = w3_temp  # Zaten bağlandık
    w3_fast    = (await build_w3(fast_rpc, "Fast-RPC")
                  if fast_rpc != chain_cfg.rpc_url else w3_alchemy)

    # ── On-chain pozisyon yükle ───────────────────────────────────────────────
    if not args.skip_init and cluster_state.burst_states:
        await init_all_targets(w3_alchemy, cluster_state)
    elif not cluster_state.burst_states:
        logger.warning("[INIT] targets.json'da hedef yok — targets_json_watcher yeni hedefleri algılayacak.")
    else:
        logger.warning("[INIT] --skip-init: Pozisyon fetch atlandı.")

    # Banner: LT/Bns/CF init sonrası on-chain rezervden dolu olur.
    log_cluster_banner(cluster_state, fast_rpc)

    # ── Başlangıç oracle fiyatları (HTTP — WSS bağlanmadan önce) ─────────────
    logger.info("[INIT] Başlangıç oracle fiyatları HTTP'den yükleniyor...")

    cs = AsyncWeb3.to_checksum_address

    fetched_feeds: Set[str] = set()
    
    # Adım 1: Önce WETH feed çekilmeli (stETH/weETH oranlarını USD'ye çervirmek için)
    weth_price = 2500.0
    for feed_addr in cluster_state.all_feed_addresses:
        token_syms = cluster_state.feed_map.get(feed_addr.lower(), [])
        if "WETH" in token_syms:
            try:
                feed  = w3_alchemy.eth.contract(address=cs(feed_addr), abi=CHAINLINK_ROUND_ABI)
                _, ans, *_ = await feed.functions.latestRoundData().call()
                weth_price = ans / CHAINLINK_DECIMALS
            except Exception:
                pass
            break

    # Adım 2: Tüm fiyatları çek
    for feed_addr in cluster_state.all_feed_addresses:
        if feed_addr in fetched_feeds:
            continue
        fetched_feeds.add(feed_addr)

        token_syms = cluster_state.feed_map.get(feed_addr.lower(), [])
        feed_type  = cluster_state.feed_type_map.get(feed_addr.lower(), "USD")
        try:
            feed  = w3_alchemy.eth.contract(address=cs(feed_addr), abi=CHAINLINK_ROUND_ABI)
            _, ans, *_ = await feed.functions.latestRoundData().call()

            # [v5] Deterministik LST fiyat çözünürlüğü
            if feed_type == "ETH_RATIO":
                price = (ans / 10**18) * weth_price
            else:
                price = ans / CHAINLINK_DECIMALS

            for sym in token_syms:
                cluster_state.oracle.prices[sym] = price
                logger.info("  %-6s = $%.4f", sym, price)
        except Exception as exc:
            # LST PriceCapAdapters often omit latestRoundData and only expose latestAnswer
            try:
                feed_alt = w3_alchemy.eth.contract(address=cs(feed_addr), abi=[{'inputs':[],'name':'latestAnswer','outputs':[{'name':'','type':'int256'}],'stateMutability':'view','type':'function'}])
                ans = await feed_alt.functions.latestAnswer().call()
                if feed_type == "ETH_RATIO":
                    price = (ans / 10**18) * weth_price
                else:
                    price = ans / CHAINLINK_DECIMALS
                for sym in token_syms:
                    cluster_state.oracle.prices[sym] = price
                    logger.info("  %-6s = $%.4f (latestAnswer fallback)", sym, price)
            except Exception as exc2:
                logger.warning("  %-6s feed yüklenemedi: HTTP Oracle Failure", token_syms)

    # ── liq_ratio ve burst eşiklerini hesapla ─────────────────────────────────
    o = cluster_state.oracle
    logger.info("[INIT] Tasfiye eşikleri hesaplanıyor...")

    for bs in cluster_state.burst_states:
        if bs.confirmed:
            continue

        bs.target.recalculate(o)
        bs.reset_thresholds()

        t = bs.target
        if getattr(t, 'in_memory_hf', 999.0) < 999.0:
            current_hf = t.in_memory_hf
            logger.info(
                "  %-14s | RAM HF=%.6f | kâr=$%.2f",
                t.label, current_hf, t.estimated_profit_usd,
            )
        else:
            logger.warning("  %-14s | HF hesaplanamadı (pozisyon sıfır?)", t.label)

    # ── Price Queue (Oracle Hub → Burst Engine) ───────────────────────────────
    price_queue: asyncio.Queue = asyncio.Queue(maxsize=128)

    # ── Paralel Task'lar ──────────────────────────────────────────────────────
    wss_url = chain_cfg.wss_url

    tasks = [
        asyncio.create_task(
            oracle_hub(cluster_state, wss_url, price_queue),
            name="oracle-hub",
        ),
        asyncio.create_task(
            burst_fire_engine(
                state=cluster_state,
                w3=w3_alchemy,
                private_key=private_key,
                executor=executor or "",
            ),
            name="burst-engine",
        ),
        asyncio.create_task(
            state_change_watcher(
                state=cluster_state,
                w3_alchemy=w3_alchemy,
                wss_url=wss_url,
                trigger_lock=asyncio.Lock()
            ),
            name="state-watcher",
        ),
        asyncio.create_task(
            motor2_hf_monitor(
                cluster_state, w3_fast, private_key,
                gas_limit, gas_mult, executor or None,
                float(os.getenv("POLL_INTERVAL", "0.4")),
            ),
            name="motor2-hf",
        ),
        asyncio.create_task(
            targets_json_watcher(
                state=cluster_state,
                w3=w3_alchemy,
            ),
            name="targets-watcher",
        ),
    ]

    logger.info("=" * 76)
    logger.info("  5 paralel task başlatıldı (%s):", chain_tag)
    for t in tasks:
        logger.info("    ▶ %s", t.get_name())
    logger.info("=" * 76)

    prices_str = " | ".join(f"{k}=${v:.2f}" for k, v in o.prices.items())
    tg.send(
        f"🚀 <b>CLUSTER SNIPER v4 [{chain_tag}] BAŞLADI</b>\n"
        f"🎯 {len(cluster_state.burst_states)} hedef (targets.json)\n"
        f"🔄 Güncelleme: {TARGETS_POLL_INTERVAL}s aralıkla\n"
        f"💹 <code>{prices_str}</code>\n"
        f"⚙️ Dry Run: {'EVET' if args.dry else 'HAYIR'}"
    )

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    for t in done:
        exc = t.exception()
        if exc:
            logger.error("Task '%s' hata: %s", t.get_name(), exc)

    for t in pending:
        t.cancel()

    await asyncio.gather(*pending, return_exceptions=True)
    logger.info("Cluster Sniper [%s] sonlandı.", chain_tag)


if __name__ == "__main__":
    asyncio.run(main())
```

**Eski:** `price > 10**6` heuristic'i ile kırılgan LST tespiti
**Yeni:** `CHAIN_RESERVES`'a `"feed_type": "ETH_RATIO"` bayrağı eklendi

| Token | Zincirler | Feed Type |
|---|---|---|
| wstETH | ARB, BASE, OP | `ETH_RATIO` |
| weETH | ARB, BASE | `ETH_RATIO` |
| wrsETH | BASE | `ETH_RATIO` |

`_build_chain_data()` artık `feed_type_map` döndürüyor. Bu map `ClusterSniperState`'e taşınıyor ve hem WSS `oracle_hub()` hem de HTTP başlangıç fetch'i tarafından kullanılıyor.

### 2. Ghost Offset — Motor 2 İstihbarat Görevi

`ClusterTarget`'a eklenen yeni alanlar:
- `missing_coll_usd_lt: float = 0.0` — Bilinmeyen teminatların LT-ağırlıklı USD farkı
- `missing_debt_usd: float = 0.0` — Bilinmeyen borçların USD farkı  
- `is_motor2_synced: bool = False` — Motor 2 en az bir kez veri yazdı mı?

`motor2_hf_monitor()` her 0.4s Multicall'da:
```python
real_debt_usd = decoded[1] / USD_DECIMALS
real_coll_usd_lt = hf * real_debt_usd
# Motor 1'in bildiği değerler
known_coll_usd_lt = Σ(amount × price × lt)  # from hf_collaterals
known_debt_usd = Σ(amount × price)          # from hf_debts
# Ghost Offset (NEGATİF OLABİLİR — kasıtlıdır)
t.missing_coll_usd_lt = real_coll_usd_lt - known_coll_usd_lt
t.missing_debt_usd = real_debt_usd - known_debt_usd
t.is_motor2_synced = True
```

### 3. Motor 1 Kurşun Geçirmez `compute_hf()`

```
1. is_motor2_synced = False → return 999.0 (Race condition koruması)
2. known_coll/debt = Σ(Oracle fiyat × miktar)
3. total = known + missing (Ghost Offset, tek toplama)
4. total_debt ≤ 0 → return 999.0
5. HF = total_coll_lt / total_debt
```

### 4. Temizlik

| Kaldırılan Blok | Konum | Sebep |
|---|---|---|
| eMode LT Sync | `init_all_targets()` | Ghost Offset otomatik telafi eder |
| Stable-Heavy filtresi | `init_all_targets()` | Watcher.py zaten yapıyor |
| derived_lt (eMode) | `load_cluster_from_targets_json()` | Ghost Offset otomatik telafi eder |
| derived_lt (eMode) | `targets_json_watcher()` | Ghost Offset otomatik telafi eder |

## Doğrulama

- ✅ `py_compile` syntax check geçti
- ⏳ `--dry --chain BASE` ile canlı HF karşılaştırma bekleniyor
- ⏳ `--benchmark` modu ile ghost_coll/ghost_debt logları izlenecek
