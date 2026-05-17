import logging
from .engines.base import BaseSqlEngine

logger = logging.getLogger("pipedq.sql.config")

def create_metadata_tables(
    engine: BaseSqlEngine, 
    schema_name: str = "pipedq", 
    rules_table_name: str = "dq_rules",
    results_table_name: str = "dq_results",
    failed_rows_table_name: str = "dq_failed_rows"
):
    """
    Bootstraps the metadata schema and tables using standard generic SQL DDL.
    
    Args:
        engine: An instance of BaseSqlEngine (e.g. PySparkSqlEngine or DuckDbSqlEngine).
        schema_name: The name of the schema to create and use.
        rules_table_name: The name of the rules metadata table.
        results_table_name: The name of the results table.
        failed_rows_table_name: The name of the common failed rows table.
    """
    logger.info(f"Bootstrapping pipeDQ schema '{schema_name}'...")

    # Create schema
    engine.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    # Create Rules Table
    rules_ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{rules_table_name} (
        rule_id VARCHAR,
        table_name VARCHAR,
        column_name VARCHAR,
        sql_where_clause VARCHAR,
        threshold_pct FLOAT,
        store_failed_rows BOOLEAN,
        is_mandatory BOOLEAN
    )
    """
    logger.info(f"Creating rules table: {schema_name}.{rules_table_name}")
    engine.execute_ddl(rules_ddl)

    # Create Results Table
    results_ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{results_table_name} (
        rule_id VARCHAR,
        table_name VARCHAR,
        column_name VARCHAR,
        total_count BIGINT,
        passed_count BIGINT,
        failed_count BIGINT,
        passed_pct FLOAT,
        is_passed BOOLEAN,
        is_mandatory BOOLEAN,
        execution_ts TIMESTAMP
    )
    """
    logger.info(f"Creating results table: {schema_name}.{results_table_name}")
    engine.execute_ddl(results_ddl)
    
    # Create Failed Rows Table
    failed_rows_ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{failed_rows_table_name} (
        rule_id VARCHAR,
        table_name VARCHAR,
        failed_row_json VARCHAR,
        execution_ts TIMESTAMP
    )
    """
    logger.info(f"Creating failed rows table: {schema_name}.{failed_rows_table_name}")
    engine.execute_ddl(failed_rows_ddl)

    logger.info("Bootstrapping complete.")
