#!/usr/bin/env python3
"""
Anvil fork laboratuvarında Aave V3 kullanıcılarının Health Factor (HF) değerini düşürmek.

Varsayılan strateji (önerilen):
  Her spike/crash öncesi zincirden gerçek Chainlink aggregator `latestRoundData().answer`
  ve `decimals()` okunur; yeni fiyat = eski × (1 ± birkaç yüzde baz puan). Böylece HF ~1.02 iken
  küçük bir sıçrama yetilir; `evm_increaseTime` ile faizi şişirdiğiniz dev borç × astronomik
  fiyat çarpımında oluşan uint256 taşmasından kaçınılır.

«execution reverted» / Web3 bazen InvalidAmount (0x2c5211c6):
  • AaveOracle **getAssetPrice** zincirde Chainlink kaynağından **latestAnswer()** okur —
    mock'ta yalnızca latestRoundData varsa çağrı düşer → zincir içi revert / CustomError.
  • Çözüm: Mock’a latestAnswer (+ AggregatorInterface’deki timestamp/round/getAnswer vb.) eklenir.

Taşma (uzun zaman sardırma + çok büyük fiyat):
  • --spike-bps düşük tutun veya fork’u sıfırlayın.

Motor 1 Chainlink WSS bu yöntemle otomatik güncellenmez (önceki gibi).

Ortam: .env içindeki ANVIL_RPC veya --rpc.
Dinamik mock için: pip install py-solc-x
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Set, Tuple

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.types import RPCEndpoint

# ── Arbitrum One — Aave V3 ───────────────────────────────────────────────────
POOL_ADDRESSES_PROVIDER_ARB = Web3.to_checksum_address(
    "0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb"
)
AAVE_POOL_ARB = Web3.to_checksum_address(
    "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
)
AAVE_ORACLE_ARB = Web3.to_checksum_address(
    "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7"
)

DEFAULT_ASSETS = {
    "WBTC": Web3.to_checksum_address("0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"),
    "USDC": Web3.to_checksum_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831"),
}

SOLC_VERSION = "0.8.20"

# Legacy bytecode — Chainlink AggregatorInterface uyumlu (AaveOracle latestAnswer kullanır).
MOCK_AGG_LEGACY_SPIKE_130K_RUNTIME = (
    "0x608060405234801561000f575f80fd5b506004361061007b575f3560e01c80638205bf6a116100595780638205bf6a146100d9578063b5ab58dc146100f7578063b633620c14610127578063feaf968c146101575761007b565b8063313ce5671461007f57806350d25bcd1461009d578063668a0f02146100bb575b5f80fd5b610087610179565b6040516100949190610229565b60405180910390f35b6100a5610181565b6040516100b2919061025a565b60405180910390f35b6100c361018e565b6040516100d0919061028b565b60405180910390f35b6100e16101b5565b6040516100ee919061028b565b60405180910390f35b610111600480360381019061010c91906102d2565b6101bc565b60405161011e919061025a565b60405180910390f35b610141600480360381019061013c91906102d2565b6101cb565b60405161014e919061028b565b60405180910390f35b61015f6101d4565b604051610170959493929190610321565b60405180910390f35b5f6008905090565b5f650bd2cc61d000905090565b5f7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff905090565b5f42905090565b5f650bd2cc61d0009050919050565b5f429050919050565b5f805f805f80650bd2cc61d000905069ffffffffffffffffffff81424269ffffffffffffffffffff95509550955095509550509091929394565b5f60ff82169050919050565b6102238161020e565b82525050565b5f60208201905061023c5f83018461021a565b92915050565b5f819050919050565b61025481610242565b82525050565b5f60208201905061026d5f83018461024b565b92915050565b5f819050919050565b61028581610273565b82525050565b5f60208201905061029e5f83018461027c565b92915050565b5f80fd5b6102b181610273565b81146102bb575f80fd5b50565b5f813590506102cc816102a8565b92915050565b5f602082840312156102e7576102e66102a4565b5b5f6102f4848285016102be565b91505092915050565b5f69ffffffffffffffffffff82169050919050565b61031b816102fd565b82525050565b5f60a0820190506103345f830188610312565b610341602083018761024b565b61034e604083018661027c565b61035b606083018561027c565b6103686080830184610312565b969550505050505056fea2646970667358221220833f86f74222139c452fdf79df436558cb859339c9efa8526dec556f4f4c988164736f6c63430008140033"
)

MOCK_AGG_LEGACY_CRASH_RUNTIME = (
    "0x608060405234801561000f575f80fd5b506004361061007b575f3560e01c80638205bf6a116100595780638205bf6a146100d9578063b5ab58dc146100f7578063b633620c14610127578063feaf968c146101575761007b565b8063313ce5671461007f57806350d25bcd1461009d578063668a0f02146100bb575b5f80fd5b610087610179565b6040516100949190610220565b60405180910390f35b6100a5610181565b6040516100b29190610251565b60405180910390f35b6100c361018b565b6040516100d09190610282565b60405180910390f35b6100e16101b2565b6040516100ee9190610282565b60405180910390f35b610111600480360381019061010c91906102c9565b6101b9565b60405161011e9190610251565b60405180910390f35b610141600480360381019061013c91906102c9565b6101c5565b60405161014e9190610282565b60405180910390f35b61015f6101ce565b604051610170959493929190610318565b60405180910390f35b5f6008905090565b5f620f4240905090565b5f7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff905090565b5f42905090565b5f620f42409050919050565b5f429050919050565b5f805f805f80620f4240905069ffffffffffffffffffff81424269ffffffffffffffffffff95509550955095509550509091929394565b5f60ff82169050919050565b61021a81610205565b82525050565b5f6020820190506102335f830184610211565b92915050565b5f819050919050565b61024b81610239565b82525050565b5f6020820190506102645f830184610242565b92915050565b5f819050919050565b61027c8161026a565b82525050565b5f6020820190506102955f830184610273565b92915050565b5f80fd5b6102a88161026a565b81146102b2575f80fd5b50565b5f813590506102c38161029f565b92915050565b5f602082840312156102de576102dd61029b565b5b5f6102eb848285016102b5565b91505092915050565b5f69ffffffffffffffffffff82169050919050565b610312816102f4565b82525050565b5f60a08201905061032b5f830188610309565b6103386020830187610242565b6103456040830186610273565b6103526060830185610273565b61035f6080830184610309565b969550505050505056fea2646970667358221220600c77357fcdc392b46ca764f484249c03e9a41ed9d4ffafeb6cd359d083804e64736f6c63430008140033"
)

ADDRESSES_PROVIDER_ABI = [
    {
        "inputs": [],
        "name": "getPool",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getPriceOracle",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

AAVE_ORACLE_SOURCES_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
        "name": "getSourceOfAsset",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address[]", "name": "assets", "type": "address[]"}],
        "name": "getAssetsPrices",
        "outputs": [{"internalType": "uint256[]", "name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]

AGGREGATOR_V3_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "latestAnswer",
        "outputs": [{"internalType": "int256", "name": "", "type": "int256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

POOL_USER_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
        "name": "getUserAccountData",
        "outputs": [
            {"internalType": "uint256", "name": "totalCollateralBase", "type": "uint256"},
            {"internalType": "uint256", "name": "totalDebtBase", "type": "uint256"},
            {"internalType": "uint256", "name": "availableBorrowsBase", "type": "uint256"},
            {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
            {"internalType": "uint256", "name": "ltv", "type": "uint256"},
            {"internalType": "uint256", "name": "healthFactor", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

_solc_installed = False


def _ensure_solc() -> None:
    global _solc_installed
    if _solc_installed:
        return
    try:
        from solcx import install_solc
    except ImportError as exc:
        raise RuntimeError(
            "py-solc-x yüklü değil. Çalıştırın: pip install py-solc-x\n"
            "Ya da --legacy-fixed-feed-mocks ile sabit bytecode kullanın (önerilmez)."
        ) from exc
    install_solc(SOLC_VERSION)
    _solc_installed = True


def compile_agg_mock_runtime(answer: int, decimals_val: int) -> str:
    if answer <= 0:
        raise ValueError(f"aggregator answer pozitif olmalı, gelen: {answer}")
    if answer >= 2**255:
        raise ValueError(f"answer int256 için çok büyük (taşma riski): {answer}")
    if decimals_val < 0 or decimals_val > 255:
        raise ValueError(f"decimals geçersiz: {decimals_val}")

    _ensure_solc()
    from solcx import compile_source

    src = f"""// SPDX-License-Identifier: MIT
pragma solidity {SOLC_VERSION};
contract MockAggDyn {{
    function latestAnswer() external view returns (int256) {{
        return int256(uint256({answer}));
    }}
    function latestTimestamp() external view returns (uint256) {{
        return block.timestamp;
    }}
    function latestRound() external view returns (uint256) {{
        return type(uint256).max;
    }}
    function getAnswer(uint256) external view returns (int256) {{
        return int256(uint256({answer}));
    }}
    function getTimestamp(uint256) external view returns (uint256) {{
        return block.timestamp;
    }}
    function latestRoundData() external view returns (uint80,int256,uint256,uint256,uint80) {{
        int256 a = int256(uint256({answer}));
        return (type(uint80).max, a, block.timestamp, block.timestamp, type(uint80).max);
    }}
    function decimals() external pure returns (uint8) {{
        return uint8({decimals_val});
    }}
}}
"""
    compiled = compile_source(src, output_values=["bin-runtime"], solc_version=SOLC_VERSION)
    blob = next(iter(compiled.values()))["bin-runtime"]
    return "0x" + blob


def _rpc(w3: Web3, method: str, params: list) -> object:
    prov = w3.provider
    if not hasattr(prov, "make_request"):
        raise RuntimeError("HTTPProvider veya uyumlu bir provider gerekir (make_request).")
    return prov.make_request(RPCEndpoint(method), params)  # type: ignore[arg-type]


def anvil_set_code(w3: Web3, address: str, code_hex: str) -> None:
    addr = Web3.to_checksum_address(address)
    if not code_hex.startswith("0x"):
        code_hex = "0x" + code_hex
    res = _rpc(w3, "anvil_setCode", [addr, code_hex])
    if isinstance(res, dict) and res.get("error"):
        raise RuntimeError(f"anvil_setCode başarısız: {res['error']}")


def evm_mine(w3: Web3, blocks: int = 1) -> None:
    for _ in range(max(1, blocks)):
        res = _rpc(w3, "evm_mine", [])
        if isinstance(res, dict) and res.get("error"):
            res2 = _rpc(w3, "evm_mine", [{}])
            if isinstance(res2, dict) and res2.get("error"):
                raise RuntimeError(f"evm_mine başarısız: {res2['error']}")


def read_hf(w3: Web3, user: str) -> Tuple[int, float]:
    pool = w3.eth.contract(address=AAVE_POOL_ARB, abi=POOL_USER_ABI)
    data = pool.functions.getUserAccountData(Web3.to_checksum_address(user)).call()
    hf_raw = int(data[5])
    if hf_raw == (1 << 256) - 1:
        human = float("inf")
    else:
        human = hf_raw / 1e18
    return hf_raw, human


def oracle_prices(w3: Web3, oracle: str, assets: List[str]) -> List[int]:
    oc = w3.eth.contract(address=Web3.to_checksum_address(oracle), abi=AAVE_ORACLE_SOURCES_ABI)
    return list(oc.functions.getAssetsPrices(assets).call())


def resolve_oracle(w3: Web3) -> str:
    ap = w3.eth.contract(address=POOL_ADDRESSES_PROVIDER_ARB, abi=ADDRESSES_PROVIDER_ABI)
    onchain = Web3.to_checksum_address(ap.functions.getPriceOracle().call())
    if onchain.lower() != AAVE_ORACLE_ARB.lower():
        print(
            f"[Uyarı] Zincirdeki oracle {onchain}, sabit ARB oracle {AAVE_ORACLE_ARB}. "
            "Zincirdeki adres kullanılacak.",
            file=sys.stderr,
        )
    return onchain


def read_agg_answer_decimals(w3: Web3, agg_addr: str) -> Tuple[int, int]:
    """
    AaveOracle.getAssetPrice zincirde kaynak için latestAnswer kullanır; spike öncesi okumayı
    onunla hizalı tutarız (latestRoundData ile tutarsızlık olmasın).
    """
    agg = w3.eth.contract(address=Web3.to_checksum_address(agg_addr), abi=AGGREGATOR_V3_ABI)
    dec = int(agg.functions.decimals().call())
    try:
        ans_i = int(agg.functions.latestAnswer().call())
    except Exception:
        _rid, ans, _sa, _ua, _air = agg.functions.latestRoundData().call()
        ans_i = int(ans)
    if ans_i <= 0:
        raise RuntimeError(f"Aggregator {agg_addr} fiyatı pozitif değil: {ans_i}")
    return ans_i, dec


def apply_feed_mocks(
    w3: Web3,
    oracle_addr: str,
    spike_assets: List[str],
    crash_assets: List[str],
    saved: Dict[str, str],
    *,
    spike_bps: int,
    crash_bps: int,
    legacy_fixed_feed_mocks: bool,
) -> None:
    oc = w3.eth.contract(address=oracle_addr, abi=AAVE_ORACLE_SOURCES_ABI)
    patched: Set[str] = set()

    for asset in spike_assets:
        ac = Web3.to_checksum_address(asset)
        src = Web3.to_checksum_address(oc.functions.getSourceOfAsset(ac).call())
        if src in patched:
            print(f"[!] Aggregator {src} zaten patchli; atlanıyor (asset={asset})", file=sys.stderr)
            continue
        code = w3.eth.get_code(src).hex()
        if src not in saved:
            saved[src] = code

        if legacy_fixed_feed_mocks:
            runtime = MOCK_AGG_LEGACY_SPIKE_130K_RUNTIME
            print(f"[+] Spike LEGACY sabit bytecode → {src} ({asset})")
        else:
            old_ans, dec = read_agg_answer_decimals(w3, src)
            # borç tarafını şişir: answer ↑
            new_ans = old_ans * (10_000 + spike_bps) // 10_000
            if new_ans <= old_ans:
                new_ans = old_ans + 1
            print(
                f"[+] Spike dinamik → {src} ({asset}) answer {old_ans} → {new_ans} "
                f"(+{spike_bps} bps), decimals={dec}"
            )
            runtime = compile_agg_mock_runtime(new_ans, dec)

        anvil_set_code(w3, src, runtime)
        patched.add(src)

    for asset in crash_assets:
        ac = Web3.to_checksum_address(asset)
        src = Web3.to_checksum_address(oc.functions.getSourceOfAsset(ac).call())
        if src in patched:
            raise RuntimeError(
                f"Aynı aggregator hem spike hem crash için kullanılıyor ({src}, asset={asset}). "
                "Farklı asset seçin veya mock sırasını değiştirin."
            )
        code = w3.eth.get_code(src).hex()
        if src not in saved:
            saved[src] = code

        if legacy_fixed_feed_mocks:
            runtime = MOCK_AGG_LEGACY_CRASH_RUNTIME
            print(f"[+] Crash LEGACY sabit bytecode → {src} ({asset})")
        else:
            old_ans, dec = read_agg_answer_decimals(w3, src)
            # teminat USD düşür: answer ↓
            new_ans = old_ans * (10_000 - crash_bps) // 10_000
            new_ans = max(1, new_ans)
            print(
                f"[+] Crash dinamik → {src} ({asset}) answer {old_ans} → {new_ans} "
                f"(-{crash_bps} bps), decimals={dec}"
            )
            runtime = compile_agg_mock_runtime(new_ans, dec)

        anvil_set_code(w3, src, runtime)
        patched.add(src)


def restore_codes(w3: Web3, saved: Dict[str, str]) -> None:
    for addr, code in saved.items():
        c = code if code.startswith("0x") else "0x" + code
        if c in ("0x", "0x0"):
            c = "0x"
        print(f"[*] Kod geri yükleniyor: {addr}")
        anvil_set_code(w3, addr, c)


def main() -> int:
    p = argparse.ArgumentParser(
        description="Anvil fork: Aave V3 HF düşürme (aggregator mock; varsayılan = zincir fiyatına küçük % ek)."
    )
    p.add_argument("--rpc", default=os.getenv("ANVIL_RPC", "http://127.0.0.1:8545"))
    p.add_argument("--user", required=True, help="Batırılacak Aave kullanıcı adresi")
    p.add_argument(
        "--spike-asset",
        action="append",
        default=[],
        metavar="ADDRESS",
        help="Borç tarafını şişirmek için underlying (ör. WBTC). Tekrarlanabilir.",
    )
    p.add_argument(
        "--crash-collateral",
        action="append",
        default=[],
        metavar="ADDRESS",
        help="Teminat fiyatını düşürmek için underlying (ör. USDC). Tekrarlanabilir.",
    )
    p.add_argument("--preset-wbtc-usdc", action="store_true", help="WBTC spike + USDC crash (ARB adresleri)")
    p.add_argument(
        "--skip-collateral-mock",
        action="store_true",
        help="Preset ile yalnızca WBTC spike (USDC crash yok).",
    )
    p.add_argument(
        "--spike-bps",
        type=int,
        default=400,
        help="Spike: latestRoundData.answer çarpanı, baz puan (100 bps=%%1). Varsayılan 400 (=%%4). evm_increaseTime sonrası düşük tutun.",
    )
    p.add_argument(
        "--crash-bps",
        type=int,
        default=2500,
        help="Crash: answer azaltma bps (2500=%%25). Varsayılan 2500.",
    )
    p.add_argument(
        "--legacy-fixed-feed-mocks",
        action="store_true",
        help="Eski sabit bytecode (130k BTC + agresif USDC). Taşma riski; sadece debug.",
    )
    p.add_argument("--mine-blocks", type=int, default=1)
    args = p.parse_args()

    if args.spike_bps < 0 or args.spike_bps > 5000:
        print("--spike-bps 0..5000 aralığında olmalı.", file=sys.stderr)
        return 2
    if args.crash_bps < 0 or args.crash_bps >= 10000:
        print("--crash-bps 0..9999 aralığında olmalı.", file=sys.stderr)
        return 2

    spike_list = list(args.spike_asset)
    crash_list = list(args.crash_collateral)
    if args.preset_wbtc_usdc:
        spike_list.append(DEFAULT_ASSETS["WBTC"])
        if not args.skip_collateral_mock:
            crash_list.append(DEFAULT_ASSETS["USDC"])

    if not spike_list and not crash_list:
        print(
            "En az --spike-asset veya --crash-collateral veya --preset-wbtc-usdc gerekli.",
            file=sys.stderr,
        )
        return 2

    w3 = Web3(Web3.HTTPProvider(args.rpc))
    if not w3.is_connected():
        print(f"RPC'ye bağlanılamadı: {args.rpc}", file=sys.stderr)
        return 1

    oracle_addr = resolve_oracle(w3)
    user = Web3.to_checksum_address(args.user)

    print(f"[*] RPC: {args.rpc}")
    print(f"[*] Oracle: {oracle_addr}")
    print(f"[*] Kullanıcı: {user}")
    if not args.legacy_fixed_feed_mocks:
        print(f"[*] Dinamik mock: spike +{args.spike_bps} bps, crash -{args.crash_bps} bps")

    saved: Dict[str, str] = {}

    try:
        before = read_hf(w3, user)
        print(
            f"[*] Önce HF (ham)={before[0]}  yaklaşık={before[1]:.6f}"
            if before[1] != float("inf")
            else "[*] Önce HF=∞ (borç yok veya max)"
        )

        sample_assets = sorted(
            {Web3.to_checksum_address(a) for a in spike_list + crash_list},
            key=lambda x: x.lower(),
        )
        if sample_assets:
            print(f"[*] Oracle fiyatları (önce): {oracle_prices(w3, oracle_addr, sample_assets)}")

        apply_feed_mocks(
            w3,
            oracle_addr,
            spike_list,
            crash_list,
            saved,
            spike_bps=args.spike_bps,
            crash_bps=args.crash_bps,
            legacy_fixed_feed_mocks=args.legacy_fixed_feed_mocks,
        )

        evm_mine(w3, args.mine_blocks)
        print(f"[*] {args.mine_blocks} blok madenciliği (WSS newHeads).")

        try:
            after = read_hf(w3, user)
        except ContractLogicError as cle:
            print(
                "\n[HATA] getUserAccountData revert.\n"
                "  • evm_increaseTime ile borç şiştiyse: --spike-bps 50 100 200 gibi çok küçük deneyin ve/veya\n"
                "    fork'u sıfırlayın (yeni anvil fork); test3.py + büyük sabit fiyat taşması üretir.\n"
                "  • --skip-collateral-mock veya düşük --crash-bps deneyin.\n"
                f"  Web3: {cle}",
                file=sys.stderr,
            )
            raise

        print(
            f"[*] Sonra HF (ham)={after[0]}  yaklaşık={after[1]:.6f}"
            if after[1] != float("inf")
            else "[*] Sonra HF=∞"
        )

        if sample_assets:
            try:
                print(f"[*] Oracle fiyatları (sonra): {oracle_prices(w3, oracle_addr, sample_assets)}")
            except ContractLogicError:
                print("[!] getAssetsPrices revert.", file=sys.stderr)

        if after[1] != float("inf") and after[1] < 1.0:
            print("[OK] HF < 1.0 (zincir).")
        elif after[1] != float("inf") and before[1] != float("inf") and after[1] >= 1.0:
            print(
                f"[!] HF hâlâ ≥ 1 ({after[1]:.6f}). --spike-bps artırın (örn. {args.spike_bps + 200}) veya crash ekleyin.",
                file=sys.stderr,
            )

        print("\nGeri yükleme (aynı RPC):")
        print("  import anvil_fork_force_hf as lab; from web3 import Web3")
        print(f"  lab.restore_codes(Web3(Web3.HTTPProvider('{args.rpc}')), {saved!r})")

    except Exception as exc:
        print(f"Hata: {exc}", file=sys.stderr)
        if saved:
            print("Kısmi mock var; restore_codes ile geri alın.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
