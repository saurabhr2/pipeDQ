import pytest
from pyspark.sql import SparkSession
import duckdb

from pipedq.sql.models import SqlRule
from pipedq.sql.factory import get_sql_engine
from pipedq.sql.engines.duckdb_engine import DuckDbSqlEngine
from pipedq.sql.engines.pyspark_engine import PySparkSqlEngine
from pipedq.sql.engines.sqlite_engine import LocalSqliteEngine
from pipedq.sql.config import create_metadata_tables
import pandas as pd

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark") \
        .getOrCreate()

@pytest.fixture
def duck_conn():
    return duckdb.connect(':memory:')

def test_duckdb_engine_counts(duck_conn):
    duck_conn.execute("CREATE TABLE test_users (id INT, age INT)")
    duck_conn.execute("INSERT INTO test_users VALUES (1, 20), (2, 15), (3, 25)")
    
    rule = SqlRule(
        rule_id="R1",
        table_name="test_users",
        sql_where_clause="age >= 18"
    )

    engine = DuckDbSqlEngine(conn=duck_conn)
    results = engine.execute_rules([rule])

    assert len(results) == 1
    res = results[0]
    assert res.total_count == 3
    assert res.passed_count == 2
    assert res.failed_count == 1
    assert res.is_passed is False

def test_pyspark_engine_counts(spark):
    data = [
        {"id": 1, "age": 20},
        {"id": 2, "age": 15},
        {"id": 3, "age": 25},
    ]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_users_pyspark")

    rule = SqlRule(
        rule_id="R1",
        table_name="test_users_pyspark",
        sql_where_clause="age >= 18"
    )

    engine = PySparkSqlEngine(spark=spark)
    results = engine.execute_rules([rule])

    assert len(results) == 1
    res = results[0]
    assert res.total_count == 3
    assert res.passed_count == 2
    assert res.failed_count == 1
    assert res.is_passed is False

def test_duckdb_failed_rows(duck_conn):
    duck_conn.execute("CREATE TABLE test_users_fail (id INT, name VARCHAR)")
    duck_conn.execute("INSERT INTO test_users_fail VALUES (1, 'Alice'), (2, NULL)")

    duck_conn.execute("CREATE TABLE failed_sink (rule_id VARCHAR, table_name VARCHAR, failed_row_json VARCHAR, execution_ts TIMESTAMP)")

    rule = SqlRule(
        rule_id="R_NULL",
        table_name="test_users_fail",
        sql_where_clause="name IS NOT NULL",
        store_failed_rows=True
    )

    engine = DuckDbSqlEngine(conn=duck_conn, failed_rows_table="failed_sink")
    engine.execute_rules([rule])

    failed_rows = duck_conn.execute("SELECT * FROM failed_sink").fetchall()
    assert len(failed_rows) == 1
    assert failed_rows[0][0] == "R_NULL"
    assert "2" in failed_rows[0][2] # The JSON should contain id=2

def test_factory_duckdb(duck_conn):
    engine = get_sql_engine(conn=duck_conn)
    assert isinstance(engine, DuckDbSqlEngine)

def test_factory_pyspark(spark):
    engine = get_sql_engine(conn=spark)
    assert isinstance(engine, PySparkSqlEngine)

def test_bootstrap(duck_conn):
    engine = DuckDbSqlEngine(conn=duck_conn)
    create_metadata_tables(engine, schema_name="pipedq", rules_table_name="dq_rules", results_table_name="dq_results")
    
    # Check if tables exist
    tables = duck_conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='pipedq'").fetchall()
    table_names = [t[0] for t in tables]
    assert "dq_rules" in table_names
    assert "dq_results" in table_names
    assert "dq_failed_rows" in table_names

def test_sqlite_engine_pandas():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "age": [20, 15, 25]
    })
    
    rule = SqlRule(
        rule_id="R1",
        table_name="test_users",
        sql_where_clause="age >= 18"
    )

    engine = LocalSqliteEngine(df=df)
    results = engine.execute_rules([rule])

    assert len(results) == 1
    res = results[0]
    assert res.total_count == 3
    assert res.passed_count == 2
    assert res.failed_count == 1
    assert res.is_passed is False

def test_factory_pandas():
    df = pd.DataFrame({"id": [1, 2]})
    engine = get_sql_engine(conn=df)
    assert isinstance(engine, LocalSqliteEngine)
