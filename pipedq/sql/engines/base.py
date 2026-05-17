from abc import ABC, abstractmethod
from typing import List, Any
from ..models import SqlRule, RuleResult

class BaseSqlEngine(ABC):
    @abstractmethod
    def execute_rules(self, rules: List[SqlRule]) -> List[RuleResult]:
        """Execute a list of SQL data quality rules and return the results."""
        pass
        
    @abstractmethod
    def execute_ddl(self, statement: str) -> None:
        """Execute an arbitrary DDL statement (like CREATE TABLE)."""
        pass
