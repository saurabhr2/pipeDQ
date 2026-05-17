import duckdb
from pipedq.sql.models import SqlRule
from pipedq.sql.factory import get_sql_engine
from pipedq.sql.config import create_metadata_tables

def main():
    print("--- Running pipeDQ Demo ---")
    
    # 1. Connect
    print("Connecting to DuckDB...")
    conn = duckdb.connect(':memory:')
    engine = get_sql_engine(conn=conn)

    # 2. Bootstrap tables
    print("Bootstrapping metadata tables...")
    create_metadata_tables(engine, schema_name="pipedq")

    # 3. Create dummy data
    print("Creating sample data...")
    conn.execute("CREATE TABLE users (id INT, age INT, email VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 25, 'test@example.com'), (2, 15, NULL)")

    # 4. Define Data Quality Rules
    rules = [
        SqlRule(rule_id="R1", table_name="users", sql_where_clause="age >= 18", is_mandatory=True),
        SqlRule(rule_id="R2", table_name="users", sql_where_clause="email IS NOT NULL")
    ]

    # 5. Execute
    print("\nExecuting rules...")
    results = engine.execute_rules(rules)

    # 6. Report
    print("\n--- Results ---")
    for res in results:
        status = "PASSED" if res.is_passed else "FAILED"
        print(f"[{res.rule_id}] {status} - Passed: {res.passed_count}/{res.total_count} ({res.passed_pct}%)")

if __name__ == "__main__":
    main()
