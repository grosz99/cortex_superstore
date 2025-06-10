import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_snowflake_connection(initial_connection=False):
    """
    Create a connection to Snowflake using environment variables
    
    Args:
        initial_connection (bool): If True, connect without specifying database and warehouse
    """
    try:
        # Print connection parameters for debugging (without password)
        print(f"Connecting to Snowflake with account: {os.getenv('SNOWFLAKE_ACCOUNT')}, user: {os.getenv('SNOWFLAKE_USER')}")
        
        # Get password securely without exposing it in logs
        password = os.getenv('SNOWFLAKE_PASSWORD')
        if not password:
            raise ValueError("Snowflake password not found in environment variables")
            
        if initial_connection:
            # Connect without database and warehouse for initial setup
            conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=password
            )
            print("Successfully connected to Snowflake for initial setup!")
        else:
            # Regular connection with all parameters
            conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=password,
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'SuperstoreWarehouse'),
                database=os.getenv('SNOWFLAKE_DATABASE', 'SuperstoreDB'),
                schema=os.getenv('SNOWFLAKE_SCHEMA', 'data')
            )
            print("Successfully connected to Snowflake!")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        print("\nTroubleshooting tips:")
        print("1. Verify your account identifier format (no https:// or .snowflakecomputing.com)")
        print("2. Check if username and password are correct")
        print("3. Ensure your IP is allowlisted if IP restrictions are enabled")
        return None

def test_connection():
    """
    Test the Snowflake connection by running a simple query
    """
    # Use initial connection to avoid requiring database and warehouse
    conn = get_snowflake_connection(initial_connection=True)
    if conn:
        try:
            cursor = conn.cursor()
            
            # Test query to check connection and verify database setup
            cursor.execute("SELECT current_version()")
            version = cursor.fetchone()[0]
            print(f"Snowflake version: {version}")
            
            # Check if the InsuranceDB database exists
            cursor.execute("SHOW DATABASES LIKE 'InsuranceDB'")
            db_exists = cursor.fetchone() is not None
            print(f"InsuranceDB exists: {db_exists}")
            
            if db_exists:
                # Check if the required tables exist
                cursor.execute("SHOW TABLES IN InsuranceDB.data")
                tables = cursor.fetchall()
                table_names = [table[1] for table in tables]
                print(f"Tables in InsuranceDB.data: {table_names}")
                
                # Check if the Cortex Search service exists
                cursor.execute("SHOW SEARCH SERVICES")
                search_services = cursor.fetchall()
                service_names = [service[0] for service in search_services]
                print(f"Search services: {service_names}")
                
                if 'SUPPORT_DOCS_SEARCH' in service_names:
                    print("Cortex Search service 'SUPPORT_DOCS_SEARCH' is configured.")
                else:
                    print("Warning: Cortex Search service 'SUPPORT_DOCS_SEARCH' not found.")
            
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Error testing connection: {e}")
            if conn:
                conn.close()
            return False
    return False

if __name__ == "__main__":
    test_connection()
