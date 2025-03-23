# 数据模块结构
data/
├── __init__.py
├── data_source/
│   ├── __init__.py
│   ├── tushare_api.py     # Tushare数据接口
│   ├── rqdata_api.py      # RQData数据接口
│   └── custom_scraper.py  # 自定义爬虫
├── data_processor/
│   ├── __init__.py
│   ├── cleaner.py         # 数据清洗
│   └── transformer.py     # 数据转换
└── database/
    ├── __init__.py
    └── mongodb_handler.py # 数据库处理 