#!/usr/bin/env python3
"""Load CSV into Postgres.

Usage example:
  python3 load_csv_to_pg.py --host localhost --port 5432 --dbname mydb \
    --user myuser --password secret --csv permits.csv --table permits
"""
import argparse
import csv
import re
import sys
from psycopg2 import connect
from psycopg2 import sql


def sanitize_column(name: str) -> str:
    # Replace non-alphanumeric/underscore with underscore
    s = re.sub(r"[^0-9A-Za-z_]", "_", name.strip())
    if not s:
        s = "col"
    if re.match(r"^[0-9]", s):
        s = "c_" + s
    return s


def sanitize_identifier(name: str) -> str:
    # Simple sanitize for table/schema names
    s = re.sub(r"[^0-9A-Za-z_]", "_", name.strip())
    if not s:
        raise ValueError("Invalid identifier")
    return s


def main():
    p = argparse.ArgumentParser(description="Load CSV into Postgres table")
    p.add_argument("--host", required=True)
    p.add_argument("--port", default=5432, type=int)
    p.add_argument("--dbname", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--csv", default="data.csv", help="Path to CSV file")
    p.add_argument("--schema", default="public", help="DB schema (default: public)")
    p.add_argument("--table", default="data", help="Target table name")
    args = p.parse_args()

    csv_path = args.csv
    schema = sanitize_identifier(args.schema)
    table = sanitize_identifier(args.table)

    try:
        with open(csv_path, newline='', encoding='utf-8-sig') as f:
            # Use standard CSV dialect with proper quoting to handle multiline values
            reader = csv.reader(f, dialect='unix', quoting=csv.QUOTE_MINIMAL)
            try:
                header = next(reader)
            except StopIteration:
                print("CSV file is empty", file=sys.stderr)
                sys.exit(1)
    except FileNotFoundError:
        print(f"CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    columns = [sanitize_column(h) for h in header]

    conn = connect(host=args.host, port=args.port, dbname=args.dbname, user=args.user, password=args.password)
    try:
        with conn.cursor() as cur:
            # Create table if not exists with text columns matching CSV order
            col_defs = sql.SQL(', ').join(sql.SQL("{} text").format(sql.Identifier(c)) for c in columns)
            create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(schema), sql.Identifier(table), col_defs
            )
            cur.execute(create_sql)

            # Parse CSV and insert records using INSERT statements
            row_count = 0
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f, dialect='unix', quoting=csv.QUOTE_MINIMAL)
                next(reader)  # Skip header row
                
                col_identifiers = [sql.Identifier(c) for c in columns]
                insert_template = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(', ').join(col_identifiers),
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                
                for row in reader:
                    cur.execute(insert_template, row)
                    row_count += 1

        conn.commit()
        print(f"Loaded {row_count} rows from CSV '{csv_path}' into {schema}.{table}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
