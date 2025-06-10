import os
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'SuperstoreDB')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'data')
STAGE_NAME = "SUPERSTORE_STAGE"
LOCAL_FILE_PATH = "superstore_semantic_model.yaml"

def upload_semantic_model():
    """Connects to Snowflake and uploads the semantic model to the specified stage."""
    print("--- Starting Semantic Model Upload ---")
    
    if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]):
        print("❌ Error: Missing one or more required Snowflake environment variables.")
        return

    try:
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("✅ Successfully connected to Snowflake.")
        
        cursor = conn.cursor()
        
        # Use the correct database and schema
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        
        # Create the stage if it doesn't exist
        print(f"Creating stage '{STAGE_NAME}' if it does not exist...")
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME}")
        print(f"✅ Stage '{STAGE_NAME}' is ready.")

        # Upload the file using the PUT command
        # Note: The path needs to be in a format Snowflake understands (file://...)
        # For Windows, we need to handle the path carefully.
        formatted_path = os.path.abspath(LOCAL_FILE_PATH).replace('\\', '/')
        put_command = f"PUT file://{formatted_path} @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        
        print(f"\n--- COPY AND RUN THIS COMMAND IN SNOWFLAKE ---")
        print(put_command)
        print(f"-------------------------------------------------")
        
        # Commenting out direct execution to allow manual upload via console
        # print(f"Executing: {put_command}")
        # cursor.execute(put_command)
        # print(f"\n✅ Success! '{LOCAL_FILE_PATH}' has been uploaded to stage '{STAGE_NAME}'.")
        print(f"\nOnce you have run the command above in Snowflake, the file '{LOCAL_FILE_PATH}' will be uploaded to stage '{STAGE_NAME}'.")

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
    finally:
        if 'conn' in locals() and not conn.is_closed():
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    upload_semantic_model()
