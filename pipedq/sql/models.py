from dataclasses import dataclass
from typing import Optional

@dataclass
class SqlRule:
    rule_id: str
    table_name: str
    sql_where_clause: str
    column_name: Optional[str] = None
    threshold_pct: float = 100.0
    store_failed_rows: bool = False
    is_mandatory: bool = True

@dataclass
class RuleResult:
    rule_id: str
    table_name: str
    column_name: Optional[str]
    total_count: int
    passed_count: int
    failed_count: int
    passed_pct: float
    is_passed: bool
    is_mandatory: bool
