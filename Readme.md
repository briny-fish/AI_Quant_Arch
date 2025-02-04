# AI-Driven Quantitative Framework

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![VeighNa](https://img.shields.io/badge/VN.PY-3.0-green)](https://www.vnpy.com/)
[![Qlib](https://img.shields.io/badge/Qlib-0.8.0-orange)](https://github.com/microsoft/qlib)

An AI-driven quantitative framework for mid-to-low frequency trading, including backtesting, factor development, news analysis, risk management, and **semi-automated stock selection using large language models (LLMs)**.

---

## **Features**
- **Core Framework**: Built on VeighNa and Qlib.
- **Data Integration**: Supports tushare, RQData, and custom scrapers.
- **AI Models**: Automated factor mining (RD-Agent) and time-series prediction (LSTM, Transformer).
- **News Analysis**: Real-time news scraping and sentiment analysis.
- **Risk Management**: Dynamic position control and risk metrics (PyFolio).
- **Visualization**: Interactive dashboards with Streamlit.
- **LLM Integration**: Semi-automated stock analysis and selection using large language models (e.g., GPT-4, LLaMA).
- **Trading Signals**: Generate buy/sell signals based on LLM analysis and quantitative models.

---

## **Installation**
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ai-quant-framework.git
   cd ai-quant-framework
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure data sources:
   - Update `config/tushare_config.json` with your API key
   - Set up Redis for news caching (optional)

4. Set up LLM:
   - Install your preferred LLM (e.g., OpenAI GPT-4 or Hugging Face LLaMA)
   - Update `config/llm_config.json` with your API key or local model path

## **Usage**

1. Backtesting
   ```bash
   python backtest.py --strategy example_strategy
   ```

2. News Analysis
   ```bash
   python news_analysis.py
   ```

3. LLM Stock Analysis
   ```bash
   python llm_analysis.py --model gpt-4 --symbol AAPL
   ```

4. Visualization
   ```bash
   streamlit run dashboard.py
   ```

## **Project Structure**
ai-quant-framework/
├── config/ # Configuration files
├── data/ # Data storage
├── factors/ # Factor development
├── models/ # AI models
├── news/ # News analysis
├── strategies/ # Trading strategies
├── tests/ # Unit tests
├── llm/ # Large language model integration
├── backtest.py # Backtesting script
├── llm_analysis.py # LLM-based stock analysis
├── dashboard.py # Streamlit dashboard
└── README.md

## **LLM Integration**

### 1. Stock Analysis
The framework integrates large language models (LLMs) to provide semi-automated stock analysis. The LLM analyzes financial data, news sentiment, and technical indicators to generate insights and recommendations.

### 2. Buy/Sell Signals
The LLM generates buy/sell signals based on a combination of quantitative models and qualitative analysis. These signals can be used to guide trading decisions.

## **License**
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## **Acknowledgments**
- VeighNa for the core trading framework
- Qlib for AI-driven factor mining
- tushare for financial data
- OpenAI and Hugging Face for large language models