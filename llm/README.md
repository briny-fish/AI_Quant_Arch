# LLM集成模块结构
llm/
├── __init__.py
├── base_llm.py           # LLM基类
├── providers/
│   ├── __init__.py
│   ├── openai_gpt.py     # OpenAI GPT接口
│   └── llama_local.py    # 本地LLaMA接口
├── analyzers/
│   ├── __init__.py
│   ├── stock_analyzer.py # 股票分析器
│   └── signal_gen.py     # 信号生成器
└── prompts/
    ├── __init__.py
    └── templates.py      # 提示词模板 