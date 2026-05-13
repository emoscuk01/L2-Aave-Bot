// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

// ============================================================================
// BAŞ MİMAR ONAYLI: AAVE V3 ARBITRUM DUAL-ENGINE SNIPER EXECUTOR
// Özellikler: Balancer %0 Fee Flashloan + Aave V3 Liquidation + Uniswap V3 Swap
// ============================================================================

// --- Arayüzler (Interfaces) ---

interface IERC20 {
    function totalSupply() external view returns (uint256);
    function balanceOf(address account) external view returns (uint256);
    function transfer(address recipient, uint256 amount) external returns (bool);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function transferFrom(address sender, address recipient, uint256 amount) external returns (bool);
}

interface IAavePool {
    function liquidationCall(
        address collateralAsset,
        address debtAsset,
        address user,
        uint256 debtToCover,
        bool receiveAToken
    ) external;
}

interface IBalancerVault {
    function flashLoan(
        address recipient,
        address[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    ) external;
}

interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }
    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
}

// --- Ana Kontrat ---

contract AaveFlashloanSniper {
    address public owner;

    // Arbitrum Mainnet Sabit Adresleri
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address constant AAVE_POOL = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant UNISWAP_ROUTER = 0xE592427A0AEce92De3Edee1F18E0157C05861564;

    // Sadece sahibin işlem yapmasını sağlayan kilit
    modifier onlyOwner() {
        require(msg.sender == owner, "Sadece Bas Mimar Ates Edebilir!");
        _;
    }

    constructor() {
        owner = msg.sender;
    }

    // 1. ADIM: Python Botu bu fonksiyonu tetikler
    function executeSnipe(
        address _collateralAsset,
        address _debtAsset,
        address _targetUser,
        uint256 _debtToCover,
        uint24 _uniswapFeeTier,
        uint256 _minProfitAmount
    ) external onlyOwner {
        
        // Balancer'a verileri paketleyip gönderiyoruz (Flashloan callback'te kullanmak için)
        bytes memory userData = abi.encode(
            _collateralAsset,
            _debtAsset,
            _targetUser,
            _debtToCover,
            _uniswapFeeTier,
            _minProfitAmount
        );

        address[] memory tokens = new address[](1);
        tokens[0] = _debtAsset;

        uint256[] memory amounts = new uint256[](1);
        amounts[0] = _debtToCover;

        // Balancer'dan borç tokenini bedavaya (%0 komisyon) çekiyoruz
        IBalancerVault(BALANCER_VAULT).flashLoan(
            address(this),
            tokens,
            amounts,
            userData
        );
    }

    // 2. ADIM: Balancer parayı gönderdikten hemen sonra bu fonksiyonu çağırır
    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external {
        require(msg.sender == BALANCER_VAULT, "Yetkisiz cagri!");

        // Paketlenmiş verileri açıyoruz
        (
            address collateralAsset,
            address debtAsset,
            address targetUser,
            uint256 debtToCover,
            uint24 uniswapFeeTier,
            uint256 minProfitAmount
        ) = abi.decode(userData, (address, address, address, uint256, uint24, uint256));

        // 2.1 Aave'ye borç tokeni için harcama yetkisi ver
        IERC20(debtAsset).approve(AAVE_POOL, debtToCover);

        // 2.2 İNFAZ: Aave'de kurbanı tasfiye et (Borcu öde, teminatı al)
        IAavePool(AAVE_POOL).liquidationCall(
            collateralAsset,
            debtAsset,
            targetUser,
            debtToCover,
            false // receiveAToken false -> Direkt underlying (gerçek) tokeni al
        );

        // 2.3 Ganimeti kontrol et
        uint256 collateralReceived = IERC20(collateralAsset).balanceOf(address(this));
        require(collateralReceived > 0, "Tasfiye basarisiz, teminat alinamadi!");

        // 2.4 Ganimeti (Teminatı) Uniswap'ta tekrar Borç Tokenine çevir (Borcu kapatmak için)
        IERC20(collateralAsset).approve(UNISWAP_ROUTER, collateralReceived);

        ISwapRouter.ExactInputSingleParams memory swapParams = ISwapRouter.ExactInputSingleParams({
            tokenIn: collateralAsset,
            tokenOut: debtAsset,
            fee: uniswapFeeTier,
            recipient: address(this),
            deadline: block.timestamp,
            amountIn: collateralReceived,
            amountOutMinimum: 0, // Korumayı kontratın sonunda bakiye ile yapacağız
            sqrtPriceLimitX96: 0
        });

        uint256 amountOut = ISwapRouter(UNISWAP_ROUTER).exactInputSingle(swapParams);

        // 2.5 Balancer'a olan borcumuzu hesapla (Sıfır fee ama yapı gereği ekliyoruz)
        uint256 amountToRepay = amounts[0] + feeAmounts[0];
        
        // Emniyet Kilidi (Anti-Sandwich / Slippage Koruması)
        require(amountOut >= amountToRepay + minProfitAmount, "KAYIP RISKI: Yeterli kar yok, islem REVERT edildi!");

        // 2.6 Balancer'a parayı iade et
        IERC20(debtAsset).transfer(BALANCER_VAULT, amountToRepay);

        // 2.7 Kalan Kârı Baş Mimara (Sana) gönder
        uint256 profit = IERC20(debtAsset).balanceOf(address(this));
        IERC20(debtAsset).transfer(owner, profit);
    }

    // Acil durum: İçeride takılı kalan tokenleri kurtarma fonksiyonu
    function withdrawToken(address _token) external onlyOwner {
        uint256 balance = IERC20(_token).balanceOf(address(this));
        IERC20(_token).transfer(owner, balance);
    }
}