import pandas as pd
from sqlalchemy import create_engine, text

# --- Database Connection Details ---
# Make sure these match your docker-compose.yml settings
user = 'pipeline_user'
password = 'pipeline_pass'
host = 'localhost' 
port = '5432'
db = 'pipeline_db'
table_name = 'green_taxi_trips'

def test_database_connection():
    """
    Connects to the PostgreSQL database, checks for the table,
    and counts the number of rows.
    """
    print("--- Starting Database Connection Test ---")
    try:
        # Create a database engine
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        
        # Use a context manager to handle the connection
        with engine.connect() as connection:
            print("✅ Successfully connected to the database.")

            # Check if the table exists
            query_table_exists = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = '{table_name}'
            );
            """)
            result_exists = connection.execute(query_table_exists)
            table_exists = result_exists.scalar()

            if not table_exists:
                print(f"❌ ERROR: Table '{table_name}' does not exist.")
                print("  -> Please check the 'ingest' service logs to see if it ran correctly.")
                return

            print(f"✅ Table '{table_name}' found.")
            
            # Query to count the number of rows
            query_count = text(f"SELECT COUNT(1) FROM {table_name};")
            result_count = connection.execute(query_count)
            row_count = result_count.scalar()
            
            print(f"📊 Number of rows in table '{table_name}': {row_count}")

            if row_count > 0:
                print("\n🎉 TEST SUCCESSFUL: Data has been loaded into the table.")
            else:
                print("\n⚠️ TEST FAILED: The table exists but is empty.")
                print("  -> The ingest script might have failed to insert data. Check 'ingest' logs.")

    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: An error occurred.")
        print(f"   Error details: {e}")
        print("\n   Please check the following:")
        print("   1. Is the Docker container for the database running? (Use 'docker ps')")
        print("   2. Are the connection details in this script correct?")
        print("   3. Have you reset the docker environment (down, volume rm, up)?")

    print("--- Test Finished ---")


if __name__ == '__main__':
    test_database_connection()