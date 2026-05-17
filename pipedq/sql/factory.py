from typing import Any, Optional
import logging

from ..env import is_pyspark_active
from .engines.base import BaseSqlEngine

logger = logging.getLogger("pipedq.sql.factory")

def get_sql_engine(
    conn: Optional[Any] = None, 
    failed_rows_table: Optional[str] = None,
    force_engine: Optional[str] = None
) -> BaseSqlEngine:
    """
    Returns the appropriate SqlEngine based on the environment.
    
    Args:
        conn: Optional connection object. If it's a SparkSession, PySpark engine is used.
              If it's a duckdb connection, DuckDB engine is used.
        failed_rows_table: Name of the table to store failed rows.
        force_engine: String to force a specific engine ("pyspark" or "duckdb").
        
    Returns:
        An instance of BaseSqlEngine.
    """
    if force_engine == "pyspark":
        from .engines.pyspark_engine import PySparkSqlEngine
        return PySparkSqlEngine(spark=conn, failed_rows_table=failed_rows_table)
    elif force_engine == "duckdb":
        from .engines.duckdb_engine import DuckDbSqlEngine
        return DuckDbSqlEngine(conn=conn, failed_rows_table=failed_rows_table)
    elif force_engine == "sqlite":
        from .engines.sqlite_engine import LocalSqliteEngine
        return LocalSqliteEngine(df=conn, failed_rows_table=failed_rows_table)
        
    # Auto-detect based on conn type
    if conn is not None:
        type_name = type(conn).__name__
        if type_name == "SparkSession":
            from .engines.pyspark_engine import PySparkSqlEngine
            return PySparkSqlEngine(spark=conn, failed_rows_table=failed_rows_table)
        elif type_name == "DuckDBPyConnection":
            from .engines.duckdb_engine import DuckDbSqlEngine
            return DuckDbSqlEngine(conn=conn, failed_rows_table=failed_rows_table)
        elif type_name == "DataFrame":
            # If conn is a DataFrame (Pandas or Polars)
            logger.info("DataFrame detected. Routing to LocalSqliteEngine.")
            from .engines.sqlite_engine import LocalSqliteEngine
            return LocalSqliteEngine(df=conn, failed_rows_table=failed_rows_table)
            
    # Auto-detect based on environment
    if is_pyspark_active():
        logger.info("Detected active PySpark session. Using PySparkSqlEngine.")
        from pyspark.sql import SparkSession
        from .engines.pyspark_engine import PySparkSqlEngine
        spark = conn if conn else SparkSession.getActiveSession()
        return PySparkSqlEngine(spark=spark, failed_rows_table=failed_rows_table)
    else:
        logger.info("PySpark not active. Falling back to pure Python DuckDbSqlEngine.")
        from .engines.duckdb_engine import DuckDbSqlEngine
        return DuckDbSqlEngine(conn=conn, failed_rows_table=failed_rows_table)
