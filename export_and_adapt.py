import sqlite3
import csv
import os
from datetime import datetime
import sys

# Configuration
PROJECT_DIR = "gold-silver-analysis-v2"
DB_NAME = "mining_companies.db"
OUTPUT_DIR = "exported_csvs"
SCHEMA_OUTPUT = "schema_postgres.sql"
IMPORT_SCRIPT = "import_to_postgres.sql"

def find_project_root(start_path):
    current_path = os.path.abspath(start_path)
    while current_path != os.path.dirname(current_path):
        if os.path.basename(current_path) == PROJECT_DIR:
            return current_path
        current_path = os.path.dirname(current_path)
    raise FileNotFoundError(f"Could not find project directory '{PROJECT_DIR}'.")

script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
PROJECT_ROOT = find_project_root(script_dir)
DB_PATH = os.path.join(PROJECT_ROOT, DB_NAME)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, OUTPUT_DIR)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']

def clean_timestamp(value):
    if value and isinstance(value, str):
        try:
            # Normalize to PostgreSQL-compatible format: "YYYY-MM-DD HH:MI:SS+ZZ"
            dt = datetime.fromisoformat(value.replace('Z', '+00:00').replace(' ', 'T'))
            return dt.strftime('%Y-%m-%d %H:%M:%S%z')  # e.g., "2024-07-27 20:20:00+0000"
        except (ValueError, TypeError):
            return None  # Replace invalid with NULL
    return value

def clean_integer(value):
    if value is None:
        return None
    try:
        val = int(float(value))
        return val if -2**63 <= val <= 2**63 - 1 else None
    except (ValueError, TypeError):
        return None

def map_sqlite_to_postgres(sqlite_type, is_pk=False):
    type_map = {
        'INTEGER': 'BIGSERIAL' if is_pk else 'BIGINT',
        'TEXT': 'TEXT',
        'REAL': 'DOUBLE PRECISION',
        'DATETIME': 'TIMESTAMP WITH TIME ZONE',
        '': 'TEXT'
    }
    return type_map.get(sqlite_type.upper(), 'TEXT')

schema_sql = []
import_sql = []

# Ensure 'companies' is imported first due to foreign key dependencies
if 'companies' in tables:
    tables.remove('companies')
    tables.insert(0, 'companies')

for table in tables:
    print(f"Exporting table: {table}")
    cursor.execute(f"PRAGMA table_info({table});")
    columns = cursor.fetchall()
    
    csv_path = os.path.join(OUTPUT_DIR, f"{table}.csv")
    cursor.execute(f"SELECT * FROM {table};")
    rows = cursor.fetchall()
    
    column_names = [col[1] for col in columns]
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(column_names)
        cleaned_rows = []
        for row in rows:
            cleaned_row = []
            for col_name, value in zip(column_names, row):
                if col_name.endswith('_updated') or col_name.endswith('_date'):
                    value = clean_timestamp(value)
                elif 'shares' in col_name or 'id' in col_name:
                    value = clean_integer(value)
                cleaned_row.append(value)
            cleaned_rows.append(cleaned_row)
        writer.writerows(cleaned_rows)
    
    print(f"Exported {table} to {csv_path} with {len(cleaned_rows)} rows")
    
    # Schema generation
    table_sql = [f"CREATE TABLE IF NOT EXISTS {table} ("]
    pk_columns = [col[1] for col in columns if col[5] == 1]
    
    for col in columns:
        col_name, col_type, not_null, default, is_pk = col[1], col[2], col[3], col[4], col[5]
        pg_type = map_sqlite_to_postgres(col_type, is_pk)
        constraints = " NOT NULL" if not_null and not is_pk else ""
        if default is not None and not is_pk:
            constraints += f" DEFAULT {default}"
        table_sql.append(f"    {col_name} {pg_type}{constraints},")
    
    if pk_columns:
        table_sql.append(f"    CONSTRAINT {table}_pkey PRIMARY KEY ({', '.join(pk_columns)})")
    else:
        table_sql[-1] = table_sql[-1].rstrip(',')
    
    table_sql.append(");")
    schema_sql.append("\n".join(table_sql))
    
    # Import script with staging table and conditional foreign key filtering
    import_sql.append(f"CREATE TABLE IF NOT EXISTS staging_{table} (LIKE {table});")
    import_sql.append(f"TRUNCATE staging_{table};")
    import_sql.append(f"\COPY staging_{table} FROM '{csv_path}' WITH (FORMAT csv, HEADER true, NULL '');")
    if pk_columns:
        has_company_id = any(col[1] == 'company_id' for col in columns)
        if table != 'companies' and has_company_id:  # Only filter if company_id exists
            import_sql.append(f"""
            DELETE FROM staging_{table}
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT company_id FROM companies);
            """)
        import_sql.append(f"""
        INSERT INTO {table}
        SELECT * FROM staging_{table}
        ON CONFLICT ({', '.join(pk_columns)}) DO UPDATE SET
        {', '.join(f"{col[1]} = EXCLUDED.{col[1]}" for col in columns if col[1] not in pk_columns)};
        """)
    else:
        import_sql.append(f"INSERT INTO {table} SELECT * FROM staging_{table};")
    import_sql.append(f"DROP TABLE staging_{table};")

# Write schema
with open(os.path.join(PROJECT_ROOT, SCHEMA_OUTPUT), 'w', encoding='utf-8') as f:
    f.write(f"-- PostgreSQL Schema Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    f.write("\n\n".join(schema_sql))
    f.write("\n\n-- Foreign Key Constraints\n")
    f.write("ALTER TABLE financials ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;\n")
    f.write("ALTER TABLE capital_structure ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;\n")
    f.write("ALTER TABLE mineral_estimates ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;\n")
    f.write("ALTER TABLE production ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;\n")
    f.write("ALTER TABLE costs ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;\n")
    f.write("ALTER TABLE valuation_metrics ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;\n")
    f.write("ALTER TABLE company_urls ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;\n")
    f.write("ALTER TABLE stock_prices ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;\n")
    
    
# Write import script
with open(os.path.join(PROJECT_ROOT, IMPORT_SCRIPT), 'w', encoding='utf-8') as f:
    f.write("\n".join(import_sql))

conn.close()
print(f"PostgreSQL schema written to {os.path.join(PROJECT_ROOT, SCHEMA_OUTPUT)}")
print(f"Import script written to {os.path.join(PROJECT_ROOT, IMPORT_SCRIPT)}")
print("Export and schema generation complete!")