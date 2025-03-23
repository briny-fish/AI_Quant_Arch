from llm import StockAnalyzer, SignalGenerator
from data import DataSource

class LLMAnalysis:
    def __init__(self):
        self.analyzer = StockAnalyzer()
        self.signal_gen = SignalGenerator()
        
    def analyze(self, symbol: str):
        pass 