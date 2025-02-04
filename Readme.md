# AI-Driven Quantitative Framework

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![VeighNa](https://img.shields.io/badge/VN.PY-3.0-green)](https://www.vnpy.com/)
[![Qlib](https://img.shields.io/badge/Qlib-0.8.0-orange)](https://github.com/microsoft/qlib)

An AI-driven quantitative framework for mid-to-low frequency trading, including backtesting, factor development, news analysis, and risk management.

---

## **Features**
- **Core Framework**: Built on VeighNa and Qlib.
- **Data Integration**: Supports tushare, RQData, and custom scrapers.
- **AI Models**: Automated factor mining (RD-Agent) and time-series prediction (LSTM, Transformer).
- **News Analysis**: Real-time news scraping and sentiment analysis.
- **Risk Management**: Dynamic position control and risk metrics (PyFolio).
- **Visualization**: Interactive dashboards with Streamlit.

---

## **Installation**
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ai-quant-framework.git
   cd ai-quant-framework
Install dependencies:

bash
复制
pip install -r requirements.txt
Configure data sources:

Update config/tushare_config.json with your API key.

Set up Redis for news caching (optional).

Usage
1. Backtesting
Run a backtest with the example strategy:

bash
复制
python backtest.py --strategy example_strategy
2. News Analysis
Start the news scraper and sentiment analyzer:

bash
复制
python news_analysis.py
3. Visualization
Launch the Streamlit dashboard:

bash
复制
streamlit run dashboard.py
Project Structure
复制
ai-quant-framework/
├── config/                  # Configuration files
├── data/                    # Data storage
├── factors/                 # Factor development
├── models/                  # AI models
├── news/                    # News analysis
├── strategies/              # Trading strategies
├── tests/                   # Unit tests
├── backtest.py              # Backtesting script
├── dashboard.py             # Streamlit dashboard
└── README.md