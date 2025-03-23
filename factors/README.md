# 因子开发模块结构
factors/
├── __init__.py
├── base_factor.py         # 因子基类
├── technical/
│   ├── __init__.py
│   ├── momentum.py       # 动量因子
│   └── volatility.py     # 波动率因子
├── fundamental/
│   ├── __init__.py
│   ├── financial.py      # 财务因子
│   └── valuation.py      # 估值因子
└── machine_learning/
    ├── __init__.py
    └── alpha_mining.py   # 自动因子挖掘 