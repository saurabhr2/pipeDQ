# pipeDQ

A comprehensive, metadata-driven Python library for data quality checks and validation. 

`pipeDQ` is built to scale from your local laptop to massive distributed clusters. It dynamically detects your environment and intelligently routes execution to the fastest available backend:
- **PySpark**: For distributed processing over massive datasets using Spark SQL.
- **DuckDB**: For lightning-fast, zero-JVM execution over local Pandas DataFrames, Parquet, and CSV files.
- **SQLite (Fallback)**: If DuckDB isn't installed but you provide a Pandas or Polars DataFrame, `pipeDQ` seamlessly falls back to Python's built-in SQLite engine to evaluate your SQL rules in-memory.

## Features

* **Multi-Engine Architecture**: Write once, run anywhere. Seamlessly switch between PySpark and DuckDB without changing your code.
* **SQL-Driven Validation**: Define your business rules using standard boolean SQL `WHERE` clauses.
* **Single-Pass Evaluation**: `pipeDQ` optimizes rule execution by grouping checks and evaluating them in a single pass over your data.
* **Threshold Support**: Define acceptable passing percentages to distinguish between minor data anomalies and critical failures.
* **Bad Record Extraction**: Optionally capture and store the exact rows that fail validation.
* **Auto-Bootstrapping**: Easily create your metadata schema, Rules table, and Results sink with a single function call.

---

## Installation

```bash
# Install with basic dependencies (DuckDB will be used as the local engine)
pip install pipedq

# If you intend to use it in a PySpark environment, PySpark will be auto-detected!
```

---

## Quickstart

```python
import duckdb
from pipedq.sql.models import SqlRule
from pipedq.sql.factory import get_sql_engine
from pipedq.sql.config import create_metadata_tables

# 1. Connect to your database or SparkSession
conn = duckdb.connect(':memory:')
# conn = SparkSession.builder.getOrCreate() # Auto-detected by pipeDQ!

# 2. Get the optimal engine for your environment
engine = get_sql_engine(conn=conn)

# 3. Bootstrap metadata tables (Optional)
create_metadata_tables(engine, schema_name="pipedq")

# 4. Create dummy data
conn.execute("CREATE TABLE users (id INT, age INT, email VARCHAR)")
conn.execute("INSERT INTO users VALUES (1, 25, 'test@example.com'), (2, 15, NULL)")

# 5. Define Data Quality Rules
rules = [
    SqlRule(rule_id="R1", table_name="users", sql_where_clause="age >= 18", is_mandatory=True),
    SqlRule(rule_id="R2", table_name="users", sql_where_clause="email IS NOT NULL")
]

# 6. Execute!
results = engine.execute_rules(rules)

for res in results:
    print(f"[{res.rule_id}] Passed: {res.is_passed} ({res.passed_pct}%)")
```

---

## Documentation & Resources

*   🌍 **[Official Website & Documentation](https://saurabhr2.github.io/pipeDQ/)**
*   📚 **[pipeDQ GitHub Wiki](https://github.com/saurabhr2/pipeDQ/wiki)**

### Wiki Quick Links:
* 📖 **[Getting Started & Installation](https://github.com/saurabhr2/pipeDQ/wiki/Getting-Started)**
* 🍳 **[The Rule Cookbook (30+ Examples)](https://github.com/saurabhr2/pipeDQ/wiki/Rule-Cookbook)**
* 🏃 **[Dynamic Execution Guide](https://github.com/saurabhr2/pipeDQ/wiki/Execution-Dynamic-Rules)**
* 🏢 **[Metadata-Driven Execution (Enterprise)](https://github.com/saurabhr2/pipeDQ/wiki/Execution-Metadata-Driven)**
* ⚙️ **[Configuration Reference](https://github.com/saurabhr2/pipeDQ/wiki/Configuration-Reference)**
* 💻 **[Python API Reference](https://github.com/saurabhr2/pipeDQ/wiki/API-Reference)**
* 🚀 **[Engines & Environments Overview](https://github.com/saurabhr2/pipeDQ/wiki/Engines)**
* 🛠️ **[Advanced Use Cases](https://github.com/saurabhr2/pipeDQ/wiki/Advanced-Use-Cases)**

---

