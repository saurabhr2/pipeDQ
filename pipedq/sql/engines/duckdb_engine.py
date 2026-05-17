import logging
import json
from typing import List

try:
    import duckdb
except ImportError:
    pass

from .base import BaseSqlEngine
from ..models import SqlRule, RuleResult

logger = logging.getLogger("pipedq.sql.duckdb")

class DuckDbSqlEngine(BaseSqlEngine):
    def __init__(self, conn=None, failed_rows_table: str = None):
        """
        Initialize the DuckDB Engine.
        If conn is not provided, an in-memory duckdb connection is created.
        """
        if 'duckdb' not in globals():
            raise ImportError("duckdb is not installed. Please install duckdb to use DuckDbSqlEngine.")
            
        self.conn = conn if conn else duckdb.connect(':memory:')
        self.failed_rows_table = failed_rows_table

    def execute_rules(self, rules: List[SqlRule]) -> List[RuleResult]:
        logger.info(f"Starting execution of {len(rules)} SQL rules using DuckDB.")

        rules_by_table = {}
        for rule in rules:
            rules_by_table.setdefault(rule.table_name, []).append(rule)

        results = []
        for table_name, table_rules in rules_by_table.items():
            logger.info(f"Evaluating {len(table_rules)} rules for table: {table_name}")
            table_results = self._evaluate_table(table_name, table_rules)
            results.extend(table_results)

        logger.info(f"Finished executing {len(rules)} rules.")
        return results

    def execute_ddl(self, statement: str) -> None:
        logger.info(f"Executing DDL: {statement}")
        self.conn.execute(statement)

    def _evaluate_table(self, table_name: str, rules: List[SqlRule]) -> List[RuleResult]:
        aggs = ["COUNT(*) as total_count"]
        for rule in rules:
            passed_expr = f"SUM(CASE WHEN {rule.sql_where_clause} THEN 1 ELSE 0 END) as {rule.rule_id}_passed"
            aggs.append(passed_expr)

        query = f"SELECT {', '.join(aggs)} FROM {table_name}"
        logger.debug(f"Executing aggregation query: {query}")
        
        try:
            agg_row = self.conn.execute(query).fetchone()
        except duckdb.Error as e:
            logger.error(f"Failed to execute query on {table_name}: {e}")
            raise

        total_count = agg_row[0]
        if total_count is None:
            total_count = 0

        results = []
        failed_rules = []

        for i, rule in enumerate(rules):
            passed_count = agg_row[i + 1]
            if passed_count is None:
                passed_count = 0

            failed_count = total_count - passed_count
            passed_pct = (passed_count / total_count * 100.0) if total_count > 0 else 100.0
            is_passed = passed_pct >= rule.threshold_pct

            result = RuleResult(
                rule_id=rule.rule_id,
                table_name=rule.table_name,
                column_name=rule.column_name,
                total_count=total_count,
                passed_count=passed_count,
                failed_count=failed_count,
                passed_pct=passed_pct,
                is_passed=is_passed,
                is_mandatory=rule.is_mandatory
            )
            results.append(result)

            status = "PASSED" if is_passed else "FAILED"
            logger.info(f"Rule {rule.rule_id} on {table_name} evaluated to {status}. "
                        f"(Passed: {passed_count}/{total_count}, Pct: {passed_pct:.2f}%, Threshold: {rule.threshold_pct}%)")

            if rule.store_failed_rows and failed_count > 0:
                failed_rules.append(rule)

        if failed_rules:
            self._store_failed_rows(table_name, failed_rules)

        return results

    def _store_failed_rows(self, table_name: str, failed_rules: List[SqlRule]):
        if not self.failed_rows_table:
            logger.warning(f"Failed rows requested for table {table_name} but no failed_rows_table is configured. Skipping.")
            return

        logger.info(f"Extracting failed rows for {len(failed_rules)} rules on {table_name}")

        for rule in failed_rules:
            # DuckDB to_json creates a JSON string of a struct containing the row
            failed_condition = f"NOT ({rule.sql_where_clause}) OR ({rule.sql_where_clause}) IS NULL"
            
            # Using DuckDB's row_to_json to serialize the row
            insert_query = f"""
            INSERT INTO {self.failed_rows_table} (rule_id, table_name, failed_row_json, execution_ts)
            SELECT 
                '{rule.rule_id}', 
                '{table_name}', 
                row_to_json(t)::VARCHAR, 
                current_timestamp
            FROM {table_name} t
            WHERE {failed_condition}
            """
            
            try:
                self.conn.execute(insert_query)
                logger.info(f"Stored failed rows for rule {rule.rule_id} into {self.failed_rows_table}")
            except duckdb.Error as e:
                logger.error(f"Failed to insert failed rows for {rule.rule_id}: {e}")
