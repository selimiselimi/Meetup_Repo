import snowflake.connector
import os

def ingest_json_to_snowflake(file_path, tablename):
    # 1. Establish Connection
    ctx = snowflake.connector.connect(
        user=os.getenv('snowflake_user'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('snowflake_account'),
        warehouse='COMPUTE_WH',
        database='MEETUP_DB',
        schema='RAW_DATA'
    )
    cs = ctx.cursor()

    try:
        # 2. Create target table with a VARIANT column
        cs.execute(f"CREATE OR REPLACE TABLE {tablename} (data VARIANT)")

        # 3. Upload (PUT) the local file to the table's internal stage
        # Use three slashes for absolute paths on Linux/Docker: file:///path/to/file
        cs.execute(f"PUT file://{file_path} @%{tablename}")

        # 4. Copy the staged file into the table
        # Snowflake handles the JSON parsing automatically
        cs.execute(f"COPY INTO {tablename} FROM @%{tablename} FILE_FORMAT = (TYPE = 'JSON')")

        print(f"Ingestion complete for {tablename}!")

    finally:
        cs.close()
        ctx.close()

def main():
    """
    Main function to iterate through files in the data directory and ingest them.
    """
    # Get the absolute path of the directory containing the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the path to the 'data' directory
    data_dir = os.path.join(script_dir, 'data')

    if not os.path.exists(data_dir):
        print(f"Error: Data directory not found at {data_dir}")
        return

    for filename in os.listdir(data_dir):
        if filename.endswith(".json") and os.path.isfile(os.path.join(data_dir, filename)):
            table_name = os.path.splitext(filename)[0]
            filename = os.path.join(data_dir, filename)
            print(f"--- Processing {table_name} ---")
            ingest_json_to_snowflake(filename,table_name)
            print(f"--- Finished processing {table_name} ---\n")

if __name__ == "__main__":
    main()
