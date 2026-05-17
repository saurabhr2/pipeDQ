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

## Rule Cookbook (25+ Common Examples)

`pipeDQ` rules use standard SQL boolean expressions. Here are 30 commonly used rule patterns you can drop directly into the `sql_where_clause` field of your `SqlRule`.

### 1. Completeness & Null Checks
| Rule ID | Description | SQL Where Clause |
| :--- | :--- | :--- |
| `NUL-01` | Column is not null | `first_name IS NOT NULL` |
| `NUL-02` | Neither of two columns is null | `first_name IS NOT NULL AND last_name IS NOT NULL` |
| `NUL-03` | At least one of two columns is populated | `email IS NOT NULL OR phone IS NOT NULL` |
| `NUL-04` | Avoid empty strings (along with nulls) | `TRIM(status) != '' AND status IS NOT NULL` |

### 2. Range & Boundary Checks
| Rule ID | Description | SQL Where Clause |
| :--- | :--- | :--- |
| `RNG-01` | Positive integer check | `age > 0` |
| `RNG-02` | Within a specific range (Inclusive) | `credit_score BETWEEN 300 AND 850` |
| `RNG-03` | Percentage bounds | `discount_rate >= 0.0 AND discount_rate <= 1.0` |
| `RNG-04` | Upper bound threshold | `salary <= 1000000` |

### 3. String Patterns & Formatting
| Rule ID | Description | SQL Where Clause |
| :--- | :--- | :--- |
| `STR-01` | Valid Email Format (Basic) | `email LIKE '%_@__%.__%'` |
| `STR-02` | Starts with specific prefix | `order_id LIKE 'ORD-%'` |
| `STR-03` | Exact length check | `LENGTH(ssn) = 9` |
| `STR-04` | Minimum length | `LENGTH(password) >= 8` |
| `STR-05` | String contains no spaces | `username NOT LIKE '% %'` |
| `STR-06` | Alpha-numeric characters only (Regex) | `REGEXP_MATCHES(user_code, '^[a-zA-Z0-9]+$')` * |
| `STR-07` | Capitalized first letter | `SUBSTRING(first_name, 1, 1) = UPPER(SUBSTRING(first_name, 1, 1))` |

*\* Note: Regex functions may vary slightly by engine (`RLIKE` in Spark, `REGEXP_MATCHES` in DuckDB).*

### 4. Categorical & Set Membership
| Rule ID | Description | SQL Where Clause |
| :--- | :--- | :--- |
| `CAT-01` | Value exists in strict ENUM set | `status IN ('PENDING', 'ACTIVE', 'CLOSED')` |
| `CAT-02` | Value does NOT exist in exclusion list | `country NOT IN ('ZZ', 'UNKNOWN')` |
| `CAT-03` | Valid boolean strings | `UPPER(is_active) IN ('TRUE', 'FALSE', 'T', 'F', 'YES', 'NO')` |

### 5. Date & Time Validations
| Rule ID | Description | SQL Where Clause |
| :--- | :--- | :--- |
| `DAT-01` | Date is not in the future | `transaction_date <= CURRENT_DATE` |
| `DAT-02` | Logical chronological order | `end_date >= start_date` |
| `DAT-03` | Age logic (Over 18 based on DOB) | `dob <= CURRENT_DATE - INTERVAL 18 YEAR` |
| `DAT-04` | Occurs during business week | `EXTRACT(DOW FROM event_date) BETWEEN 1 AND 5` |
| `DAT-05` | Timestamps within last 24 hours | `created_at >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR` |

### 6. Cross-Column Logical Rules
| Rule ID | Description | SQL Where Clause |
| :--- | :--- | :--- |
| `XCL-01` | Conditional nulls (If active, must have email) | `status != 'ACTIVE' OR email IS NOT NULL` |
| `XCL-02` | Value consistency | `total_amount = (subtotal + tax_amount)` |
| `XCL-03` | Shipping constraints | `shipping_date IS NULL OR shipping_date >= order_date` |
| `XCL-04` | Mutually exclusive flags | `NOT (is_student = TRUE AND is_retired = TRUE)` |

### 7. Complex Business Logic
| Rule ID | Description | SQL Where Clause |
| :--- | :--- | :--- |
| `BIZ-01` | Tier-based discounts | `(tier = 'VIP' AND discount <= 0.3) OR (tier = 'STD' AND discount <= 0.1)` |
| `BIZ-02` | Valid currency codes for amounts | `amount = 0 OR currency_code IN ('USD', 'EUR', 'GBP')` |
| `BIZ-03` | Valid product/category hierarchy | `(category='ELEC' AND type IN ('TV','PC')) OR (category!='ELEC')` |
| `BIZ-04` | Custom status transitions | `previous_status = 'PENDING' OR current_status != 'ACTIVE'` |
| `BIZ-05` | Data type parsing bounds (Safe Cast) | `CAST(age_string AS INT) >= 0` *(Engine will fail row if cast fails)* |

---

## Configuration Reference: `SqlRule`

When instantiating a `SqlRule`, you have several parameters to fine-tune the execution:

*   `rule_id` (str): Your internal identifier for the rule (e.g. `RQ-104`).
*   `table_name` (str): The name of the table or view to query.
*   `sql_where_clause` (str): The boolean condition that must evaluate to `TRUE` for a row to pass.
*   `column_name` (Optional[str]): A metadata tag to indicate which column this rule primarily targets.
*   `threshold_pct` (float, default `100.0`): What percentage of rows must pass for the entire rule to be marked as `PASSED`.
*   `store_failed_rows` (bool, default `False`): If true, all rows that evaluate to `FALSE` or `NULL` will be extracted and saved to your configured `failed_rows_table`.
*   `is_mandatory` (bool, default `True`): If false, this rule acts as a "warning" and won't necessarily break downstream pipelines.
