import snowflake.connector
import os

def ingest_json_to_snowflake(file_path, tablename):
    # 1. Establish Connection
    ctx = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
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