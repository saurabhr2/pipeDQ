import logging
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, count, sum, lit, current_timestamp, to_json, struct

from .base import BaseSqlEngine
from ..models import SqlRule, RuleResult

logger = logging.getLogger("pipedq.sql.pyspark")

class PySparkSqlEngine(BaseSqlEngine):
    def __init__(self, spark: SparkSession, failed_rows_table: str = None):
        self.spark = spark
        self.failed_rows_table = failed_rows_table

    def execute_rules(self, rules: List[SqlRule]) -> List[RuleResult]:
        logger.info(f"Starting execution of {len(rules)} SQL rules using PySpark.")

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
        self.spark.sql(statement)

    def _evaluate_table(self, table_name: str, rules: List[SqlRule]) -> List[RuleResult]:
        df = self.spark.table(table_name)

        aggs = [count("*").alias("total_count")]
        for rule in rules:
            passed_expr = f"CASE WHEN {rule.sql_where_clause} THEN 1 ELSE 0 END"
            aggs.append(sum(expr(passed_expr)).alias(f"{rule.rule_id}_passed"))

        logger.debug(f"Executing aggregation query for {table_name}")
        agg_row = df.agg(*aggs).collect()[0]

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
            self._store_failed_rows(df, table_name, failed_rules)

        return results

    def _store_failed_rows(self, df: DataFrame, table_name: str, failed_rules: List[SqlRule]):
        if not self.failed_rows_table:
            logger.warning(f"Failed rows requested for table {table_name} but no failed_rows_table is configured. Skipping.")
            return

        logger.info(f"Extracting failed rows for {len(failed_rules)} rules on {table_name}")

        all_failed_df = None

        for rule in failed_rules:
            failed_condition = f"NOT ({rule.sql_where_clause}) OR ({rule.sql_where_clause}) IS NULL"
            failed_df = df.filter(expr(failed_condition))

            formatted_df = failed_df.select(
                lit(rule.rule_id).alias("rule_id"),
                lit(table_name).alias("table_name"),
                to_json(struct("*")).alias("failed_row_json"),
                current_timestamp().alias("execution_ts")
            )

            if all_failed_df is None:
                all_failed_df = formatted_df
            else:
                all_failed_df = all_failed_df.unionAll(formatted_df)

        if all_failed_df:
            logger.info(f"Writing failed rows to sink table {self.failed_rows_table}")
            all_failed_df.write.mode("append").saveAsTable(self.failed_rows_table)
