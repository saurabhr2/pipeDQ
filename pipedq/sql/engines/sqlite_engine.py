import logging
import sqlite3
import json
from typing import List, Any

from .base import BaseSqlEngine
from ..models import SqlRule, RuleResult

logger = logging.getLogger("pipedq.sql.sqlite")

class LocalSqliteEngine(BaseSqlEngine):
    def __init__(self, df: Any, failed_rows_table: str = None):
        """
        Initialize the SQLite Fallback Engine.
        Args:
            df: A Pandas or Polars DataFrame.
            failed_rows_table: The name of the table to store failed rows.
        """
        self.df = df
        self.failed_rows_table = failed_rows_table
        
        # Determine if it's pandas or polars
        self._is_pandas = False
        self._is_polars = False
        
        type_name = type(self.df).__name__
        if type_name == "DataFrame":
            module_name = type(self.df).__module__
            if module_name.startswith("pandas"):
                self._is_pandas = True
            elif module_name.startswith("polars"):
                self._is_polars = True
        
        if not (self._is_pandas or self._is_polars):
            raise ValueError("LocalSqliteEngine requires a Pandas or Polars DataFrame.")

        # Initialize in-memory sqlite connection
        self.conn = sqlite3.connect(':memory:')
        # Enable returning dicts instead of tuples for easier row serialization
        self.conn.row_factory = sqlite3.Row
        
    def _load_dataframe(self, table_name: str):
        """Loads the DataFrame into the SQLite in-memory database."""
        logger.debug(f"Loading DataFrame into SQLite table '{table_name}'...")
        if self._is_pandas:
            self.df.to_sql(table_name, self.conn, index=False, if_exists='replace')
        elif self._is_polars:
            # Polars supports writing to sqlite via adbc or sqlalchemy, 
            # but standard fallback is to convert to pandas first for sqlite3 built-in support
            try:
                # Some newer polars versions support write_database to sqlite3 directly
                self.df.write_database(table_name, f"sqlite:///:memory:", if_table_exists='replace')
            except Exception as e:
                logger.debug(f"Direct Polars write_database failed, falling back to Pandas conversion: {e}")
                pandas_df = self.df.to_pandas()
                pandas_df.to_sql(table_name, self.conn, index=False, if_exists='replace')

    def execute_rules(self, rules: List[SqlRule]) -> List[RuleResult]:
        logger.info(f"Starting execution of {len(rules)} SQL rules using SQLite Fallback Engine.")

        rules_by_table = {}
        for rule in rules:
            rules_by_table.setdefault(rule.table_name, []).append(rule)

        results = []
        for table_name, table_rules in rules_by_table.items():
            # Load the table into sqlite memory
            self._load_dataframe(table_name)
            
            logger.info(f"Evaluating {len(table_rules)} rules for table: {table_name}")
            table_results = self._evaluate_table(table_name, table_rules)
            results.extend(table_results)

        logger.info(f"Finished executing {len(rules)} rules.")
        return results

    def execute_ddl(self, statement: str) -> None:
        logger.info(f"Executing DDL: {statement}")
        cursor = self.conn.cursor()
        cursor.execute(statement)
        self.conn.commit()

    def _evaluate_table(self, table_name: str, rules: List[SqlRule]) -> List[RuleResult]:
        aggs = ["COUNT(*) as total_count"]
        for rule in rules:
            passed_expr = f"SUM(CASE WHEN {rule.sql_where_clause} THEN 1 ELSE 0 END) as '{rule.rule_id}_passed'"
            aggs.append(passed_expr)

        query = f"SELECT {', '.join(aggs)} FROM {table_name}"
        logger.debug(f"Executing aggregation query: {query}")
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            agg_row = cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Failed to execute query on {table_name}: {e}")
            raise

        total_count = agg_row["total_count"]
        if total_count is None:
            total_count = 0

        results = []
        failed_rules = []

        for rule in rules:
            passed_count = agg_row[f"{rule.rule_id}_passed"]
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
        cursor = self.conn.cursor()

        for rule in failed_rules:
            failed_condition = f"NOT ({rule.sql_where_clause}) OR ({rule.sql_where_clause}) IS NULL"
            
            # Fetch failed rows
            query = f"SELECT * FROM {table_name} WHERE {failed_condition}"
            try:
                cursor.execute(query)
                failed_rows = cursor.fetchall()
                
                # Insert into failed_rows_table
                for row in failed_rows:
                    row_dict = dict(row)
                    row_json = json.dumps(row_dict)
                    
                    # Ensure table exists (might be bootstrapped, but SQLite memory is a new connection)
                    # Note: Since the DB is in memory, the user would need to have bootstrapped it in the SAME connection,
                    # but we load the DF into a new connection. This means the sink table needs to be created if it doesn't exist.
                    create_sink = f"""
                    CREATE TABLE IF NOT EXISTS {self.failed_rows_table} (
                        rule_id TEXT,
                        table_name TEXT,
                        failed_row_json TEXT,
                        execution_ts DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                    cursor.execute(create_sink)
                    
                    insert_query = f"INSERT INTO {self.failed_rows_table} (rule_id, table_name, failed_row_json) VALUES (?, ?, ?)"
                    cursor.execute(insert_query, (rule.rule_id, table_name, row_json))
                    
                self.conn.commit()
                logger.info(f"Stored failed rows for rule {rule.rule_id} into {self.failed_rows_table}")
            except sqlite3.Error as e:
                logger.error(f"Failed to extract/insert failed rows for {rule.rule_id}: {e}")
