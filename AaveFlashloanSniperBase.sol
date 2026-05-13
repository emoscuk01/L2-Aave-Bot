// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

// ============================================================================
// BASE MAINNET: AAVE V3 DUAL-ENGINE SNIPER EXECUTOR
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

interface ISwapRouter02 {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256 amountOut);
}

// --- Ana Kontrat ---

contract AaveFlashloanSniperBase {
    address public owner;

    // Base Mainnet Sabit Adresleri
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address constant AAVE_POOL = 0xA238Dd80C259a72e81d7e4664a9801593F98d1c5;
    address constant UNISWAP_ROUTER = 0x2626664c2603336E57B271c5C0b26F421741e481;

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

        (
            address collateralAsset,
            address debtAsset,
            address targetUser,
            uint256 debtToCover,
            uint24 uniswapFeeTier,
            uint256 minProfitAmount
        ) = abi.decode(userData, (address, address, address, uint256, uint24, uint256));

        IERC20(debtAsset).approve(AAVE_POOL, debtToCover);

        IAavePool(AAVE_POOL).liquidationCall(
            collateralAsset,
            debtAsset,
            targetUser,
            debtToCover,
            false
        );

        uint256 collateralReceived = IERC20(collateralAsset).balanceOf(address(this));
        require(collateralReceived > 0, "Tasfiye basarisiz, teminat alinamadi!");

        IERC20(collateralAsset).approve(UNISWAP_ROUTER, collateralReceived);

        ISwapRouter02.ExactInputSingleParams memory swapParams =
            ISwapRouter02.ExactInputSingleParams({
                tokenIn: collateralAsset,
                tokenOut: debtAsset,
                fee: uniswapFeeTier,
                recipient: address(this),
                amountIn: collateralReceived,
                amountOutMinimum: 0,
                sqrtPriceLimitX96: 0
            });

        uint256 amountOut = ISwapRouter02(UNISWAP_ROUTER).exactInputSingle(swapParams);

        uint256 amountToRepay = amounts[0] + feeAmounts[0];
        require(amountOut >= amountToRepay + minProfitAmount, "KAYIP RISKI: Yeterli kar yok, islem REVERT edildi!");

        IERC20(debtAsset).transfer(BALANCER_VAULT, amountToRepay);

        uint256 profit = IERC20(debtAsset).balanceOf(address(this));
        IERC20(debtAsset).transfer(owner, profit);
    }

    // Acil durum: İçeride takılı kalan tokenleri kurtarma fonksiyonu
    function withdrawToken(address _token) external onlyOwner {
        uint256 balance = IERC20(_token).balanceOf(address(this));
        IERC20(_token).transfer(owner, balance);
    }
}
