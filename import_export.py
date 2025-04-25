import os
import sys
import csv
import argparse
import logging
import psycopg2
from datetime import datetime, timedelta
from psycopg2 import sql
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database connection parameters from Django settings
DB_PARAMS = {
    'dbname': 'hoteldb',
    'user': 'postgres',
    'password': 'postgres1234',
    'host': 'localhost',
    'port': '5432'
}

def connect_to_db():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logger.info("Successfully connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        sys.exit(1)

def import_from_csv_psycopg2(table_name, input_file, columns=None):
    """
    Import data from CSV to a table using psycopg2.
    
    Args:
        table_name (str): Name of the table to import into
        input_file (str): Path to the input CSV file
        columns (list, optional): List of column names to import
        
    Returns:
        int: Number of records imported
    """
    print(f"\nImporting from CSV to {table_name}...")
    
    # Read CSV file
    with open(input_file, 'r', newline='') as f:
        reader = csv.reader(f)
        header = next(reader)  # Get header row
        
        # If columns not specified, use all columns from CSV
        if columns is None:
            columns = header
        
        # Map CSV columns to table columns
        column_indices = [header.index(col) for col in columns if col in header]
        if len(column_indices) != len(columns):
            print("Error: Not all specified columns found in CSV")
            return 0
        
        # Prepare data for insertion
        values = []
        for row in reader:
            values.append(tuple(row[i] for i in column_indices))
    
    # Insert data into table
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            # Use execute_values for efficient bulk insert
            execute_values(
                cursor,
                f"""
                INSERT INTO {table_name} 
                ({', '.join(columns)})
                VALUES %s
                """,
                values
            )
            conn.commit()
            record_count = len(values)
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        return 0
    finally:
        conn.close()
    
    #elapsed_time = time.time() - start_time
    print(f"Imported {record_count} records")
    return record_count


def export_to_csv_psycopg2(table_name, output_file):
    """
    Export a table to CSV using psycopg2.
    
    Args:
        table_name (str): Name of the table to export
        output_file (str): Path to the output CSV file
        
    Returns:
        int: Number of records exported
    """
    print(f"\nExporting {table_name} to CSV...")
    
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            # Get column names
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            column_names = [desc[0] for desc in cursor.description]
            
            # Fetch all data
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            
            # Write to CSV
            with open(output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(column_names)
                writer.writerows(rows)
            
            record_count = len(rows)
    except Exception as e:
        print(f"Error: {e}")
        return 0
    finally:
        conn.close()
    
    print(f"Exported {record_count} records to {output_file}")
    return record_count

def delete_from_table(table_name):
    conn = connect_to_db()
    try:
        # First, get the data into memory
        with conn.cursor() as cursor:
            # Create a temporary table with the same structure
            cursor.execute(f"DELETE FROM {table_name} ")
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        return 0
    finally:
        conn.close()

    print(f"Deleted data from {table_name}")
    return 0

def select_from_table(table_name):
    conn = connect_to_db()
    
        # First, get the data into memory
    with conn.cursor() as cursor:
    # Create a temporary table with the same structure
        cursor.execute(f"SELECT * from {table_name}")
        print(cursor.fetchall())
    
            
    # cursor.execute(f"SELECT FROM {table_name}")
    # # conn.commit()
    
    
if __name__ == "__main__":
    option = 0
    while option != 4:
        print(" ")
        print("Please select to Import from csv or Export to csv :")
        print("1. Import from csv")
        print("2. Export to csv")
        print("3. Delete Data from table")
        print("4. Display Data from table")
        print("5. Exit")
        option = input("Enter your option : ")
        if option == "5":
            break
        elif option == "1":
            import_table = input("Please input the table name for data import : ")
            import_from_csv = input("Please input the csv file name for data import : ")
            import_from_csv_psycopg2(import_table, import_from_csv)
        elif option == "2":
            export_table = input("Please input the table name to export : ")
            export_to_csv = input("Please input the csv file name for data export : ")
            export_to_csv_psycopg2(export_table, export_to_csv)
        elif option == "3":
            delete_table = input("Please input the table name data all data : ")    
            delete_from_table(delete_table)
        elif option == "4":
            select_table = input("Please input the table name to display data : ")
            select_from_table(select_table)
            # conn.close()
    #export_to_csv_psycopg2("contacts_contact", "contacts_export1.csv")

