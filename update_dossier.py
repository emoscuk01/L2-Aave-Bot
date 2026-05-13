import re

with open('cluster_sniper.py', 'r', encoding='utf-8') as f:
    text = f.read()

start_idx = text.find('VICTIM_DOSSIER: Dict[str, Dict] = {')
end_idx = text.find('}\n\n# ─────────────────────────────────────────────────────────────────────────────\n# VICTIM_DOSSIER\'dan', start_idx) + 1

if start_idx == -1 or end_idx == 0:
    print('Error finding boundaries')
    exit(1)

NEW_DOSSIER = """VICTIM_DOSSIER: Dict[str, Dict] = {
    # ── Hedef 1: WETH teminat → USDT borç (Mevcut) ───────────────────────────
    "0x4b32Ad6D34d5E07ed48BF7E9ff87addCa6995b7D": {
        "label":         "WETH/USDT",
        "coll_token":    "WETH",
        "coll_address":  "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  
        "coll_atoken":   "0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8",  
        "coll_decimals": 18,
        "coll_lt":       0.825, 
        "coll_bonus":    0.05,  
        "coll_feed":     "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",  
        "debt_token":    "USDT",
        "debt_address":  "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",  
        "debt_vtoken":   "0xfb00AC187a8Eb5AFAE4eACE434F493Eb62672df7",  
        "debt_decimals": 6,
        "debt_feed":     "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",  
        "close_factor":  0.5,
    },

    # ── Hedef 2: WETH teminat → USDC borç (Mevcut) ───────────────────────────
    "0x9CD61CaB43075C6C4a56A95DacAdaD39Df555D25": {
        "label":         "WETH/USDC",
        "coll_token":    "WETH",
        "coll_address":  "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "coll_atoken":   "0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8",  
        "coll_decimals": 18,
        "coll_lt":       0.825,
        "coll_bonus":    0.05,
        "coll_feed":     "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",  
        "debt_token":    "USDC",
        "debt_address":  "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "debt_vtoken":   "0xFccf3cAbbe80101232d343252614b6A3eE81C989",  
        "debt_decimals": 6,
        "debt_feed":     "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",  
        "debt_address_alt": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  
        "debt_vtoken_alt":  "0x307ffe186F84a3bc2613D1eA417A5737D69A7007",  
        "close_factor":  0.5,
    },

    # ── Hedef 3: WBTC teminat → USDT borç (Yeni) ─────────────────────────────
    "0x772d7E965308dF7dB83aA995382c365728136fd4": {
        "label":         "WBTC/USDT",
        "coll_token":    "WBTC",
        "coll_address":  "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "coll_atoken":   "0x078f358208685046a11C85e8ad32895DED33A249",  
        "coll_decimals": 8,
        "coll_lt":       0.70,
        "coll_bonus":    0.07,
        "coll_feed":     "0x6ce185860a4963106506C203335A2910413708e9",  
        "debt_token":    "USDT",
        "debt_address":  "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "debt_vtoken":   "0xfb00AC187a8Eb5AFAE4eACE434F493Eb62672df7",  
        "debt_decimals": 6,
        "debt_feed":     "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",  
        "close_factor":  0.5,
    },

    # ── Hedef 4: USDT teminat → WETH borç (Yeni) ─────────────────────────────
    "0x8011d0c9DB37CBCF7cA781918B4AD0cB50A177D4": {
        "label":         "USDT/WETH",
        "coll_token":    "USDT",
        "coll_address":  "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "coll_atoken":   "0x6ab707Aca953eDAeFBc4fD23bA73294241490620",  
        "coll_decimals": 6,
        "coll_lt":       0.80,
        "coll_bonus":    0.05,
        "coll_feed":     "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",  
        "debt_token":    "WETH",
        "debt_address":  "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "debt_vtoken":   "0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351",  
        "debt_decimals": 18,
        "debt_feed":     "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",  
        "close_factor":  0.5,
    },

    # ── Hedef 5: USDT teminat → WETH borç (Yeni) ─────────────────────────────
    "0x11650b27213d5B05BE4aEd19Acf57Ac2C146ba5C": {
        "label":         "USDT/WETH",
        "coll_token":    "USDT",
        "coll_address":  "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "coll_atoken":   "0x6ab707Aca953eDAeFBc4fD23bA73294241490620",  
        "coll_decimals": 6,
        "coll_lt":       0.80,
        "coll_bonus":    0.05,
        "coll_feed":     "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",  
        "debt_token":    "WETH",
        "debt_address":  "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "debt_vtoken":   "0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351",  
        "debt_decimals": 18,
        "debt_feed":     "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",  
        "close_factor":  0.5,
    },

    # ── Hedef 6: WBTC teminat → USDC borç (Yeni) ─────────────────────────────
    "0xe8Cf93C7032673E30DAe0dFA7cA2879263E7De50": {
        "label":         "WBTC/USDC",
        "coll_token":    "WBTC",
        "coll_address":  "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",  
        "coll_atoken":   "0x078f358208685046a11C85e8ad32895DED33A249",  
        "coll_decimals": 8,    
        "coll_lt":       0.70, 
        "coll_bonus":    0.07, 
        "coll_feed":     "0x6ce185860a4963106506C203335A2910413708e9",  
        "debt_token":    "USDC",
        "debt_address":  "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  
        "debt_vtoken":   "0xFccf3cAbbe80101232d343252614b6A3eE81C989",  
        "debt_decimals": 6,    
        "debt_feed":     "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",  
        "debt_address_alt": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  
        "debt_vtoken_alt":  "0x307ffe186F84a3bc2613D1eA417A5737D69A7007",  
        "close_factor":  0.5,
    },

    # ── Hedef 7: WBTC teminat → USDC borç (Yeni) ─────────────────────────────
    "0x806Dd7084fcfA6b500E18ED9299503fb60aCE403": {
        "label":         "WBTC/USDC",
        "coll_token":    "WBTC",
        "coll_address":  "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",  
        "coll_atoken":   "0x078f358208685046a11C85e8ad32895DED33A249",  
        "coll_decimals": 8,    
        "coll_lt":       0.70, 
        "coll_bonus":    0.07, 
        "coll_feed":     "0x6ce185860a4963106506C203335A2910413708e9",  
        "debt_token":    "USDC",
        "debt_address":  "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  
        "debt_vtoken":   "0xFccf3cAbbe80101232d343252614b6A3eE81C989",  
        "debt_decimals": 6,    
        "debt_feed":     "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",  
        "debt_address_alt": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  
        "debt_vtoken_alt":  "0x307ffe186F84a3bc2613D1eA417A5737D69A7007",  
        "close_factor":  0.5,
    }
}"""

new_text = text[:start_idx] + NEW_DOSSIER + text[end_idx:]

with open('cluster_sniper.py', 'w', encoding='utf-8') as f:
    f.write(new_text)

print('Updated successfully')
