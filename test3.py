"""
Fork'ta zamanı yıllarca ileri sarma — WBTC borç indeksini şişirir.

DİKKAT: Bu durumdan hemen sonra anvil_fork_force_hf.py ile SABİT veya ÇOK BÜYÜK oracle
fiyatı kullanırsanız Pool.getUserAccountData içinde uint256 taşması (execution reverted)
görürsünüz. HF hack için:

  • Önce pip install py-solc-x
  • python anvil_fork_force_hf.py ... --preset-wbtc-usdc --skip-collateral-mock --spike-bps 150

HF zaten ~1 ise zaman sardırmadan da oracle ile küçük spike deneyebilirsiniz.
"""
from web3 import Web3

w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
VICTIM = w3.to_checksum_address("0xeE5793A380795Ea69AdA27c6b2229010b4a0d2F6")
WHALE = w3.to_checksum_address("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266") # Anvil'in kendi 10k ETH'lik Tanrı Hesabı
WETH_ADDR = w3.to_checksum_address("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
POOL_ADDR = w3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD")

print("1. Garanti olsun diye zamanı 5 yıl ileri sarıyoruz...")
w3.provider.make_request("evm_increaseTime", [157680000])

print("2. Tanrı hesabına (Whale) geçiliyor...")
w3.provider.make_request("anvil_impersonateAccount", [WHALE])

# Aave ve WETH Kontrat Bağlantıları
weth_abi = [
    {"inputs": [], "name": "deposit", "outputs": [], "stateMutability": "payable", "type": "function"},
    {"inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}], "name": "approve", "outputs": [{"name": "", "type": "bool"}], "stateMutability": "nonpayable", "type": "function"}
]
weth = w3.eth.contract(address=WETH_ADDR, abi=weth_abi)

pool_abi = [
    {"inputs": [{"name": "asset", "type": "address"}, {"name": "amount", "type": "uint256"}, {"name": "onBehalfOf", "type": "address"}, {"name": "referralCode", "type": "uint16"}], "name": "supply", "outputs": [], "stateMutability": "nonpayable", "type": "function"}
]
pool = w3.eth.contract(address=POOL_ADDR, abi=pool_abi)

print("3. Whale hesabı 1 Wei WETH hazırlıyor...")
weth.functions.deposit().transact({'from': WHALE, 'value': 1})
weth.functions.approve(POOL_ADDR, 1).transact({'from': WHALE})

print("4. ÖLÜMCÜL SİNYAL: Kurbana 1 Wei hediye ediliyor! (Aave Transfer Event'i fırlatılacak)...")
# Bu işlem kurban adına WETH yatırır. Aave mecburen WSS'ye "aToken Transfer" anonsu geçer!
tx_hash = pool.functions.supply(WETH_ADDR, 1, VICTIM, 0).transact({'from': WHALE})

print(f"💥 ZİL ÇALDI! Yeni Blok: {tx_hash.hex()}")
print("Hemen ana bota dön ve STATE-WATCHER'ın uyanışını izle!")