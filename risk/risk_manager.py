from typing import List

class RiskManager:
    def __init__(self, params: dict):
        self.max_position_size = params.get('max_position_size', 0.3)
        self.max_drawdown = params.get('max_drawdown', 0.1)
        self.stop_loss = params.get('stop_loss', 0.05)
        
    def check_position_limit(self, strategy, symbol: str, quantity: int, price: float) -> bool:
        """检查仓位限制"""
        pass
        
    def check_drawdown(self, strategy) -> bool:
        """检查回撤限制"""
        pass

    def check_portfolio_risk(self) -> bool:
        pass
    
    def apply_stop_loss(self) -> List[str]:
        pass 