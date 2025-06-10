import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from snowflake_connection import get_snowflake_connection

# Load environment variables
load_dotenv()

def setup_database():
    """
    Set up the Snowflake database with Superstore data
    """
    # Use initial connection mode to avoid requiring database and warehouse
    conn = get_snowflake_connection(initial_connection=True)
    if not conn:
        print("Failed to connect to Snowflake. Please check your credentials.")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Execute setup commands
        setup_commands = [
            "USE ROLE sysadmin",
            
            # Create database
            "CREATE DATABASE IF NOT EXISTS SuperstoreDB",
            
            # Create schema
            "CREATE SCHEMA IF NOT EXISTS SuperstoreDB.data",
            
            # Create warehouse
            """
            CREATE WAREHOUSE IF NOT EXISTS SuperstoreWarehouse
                WITH WAREHOUSE_SIZE = 'XSMALL'
                AUTO_SUSPEND = 300
                AUTO_RESUME = TRUE
                INITIALLY_SUSPENDED = TRUE
            """,
            
            # Set warehouse for use
            "USE WAREHOUSE SuperstoreWarehouse",
            
            # Create Orders table (from superstore.csv)
            """
            CREATE TABLE IF NOT EXISTS SuperstoreDB.data.Orders (
                Row_ID INT,
                Order_ID STRING,
                Order_Date DATE,
                Ship_Date DATE,
                Ship_Mode STRING,
                Customer_ID STRING,
                Customer_Name STRING,
                Segment STRING,
                Country STRING,
                City STRING,
                State STRING,
                Postal_Code STRING,
                Region STRING,
                Product_ID STRING,
                Category STRING,
                Sub_Category STRING,
                Product_Name STRING,
                Sales FLOAT,
                Quantity INT,
                Discount FLOAT,
                Profit FLOAT
            )
            """,
            
            # Create Customers table (from superstore_crm_customers.csv)
            """
            CREATE TABLE IF NOT EXISTS SuperstoreDB.data.Customers (
                Customer_ID STRING PRIMARY KEY,
                Customer_Since DATE,
                Email STRING,
                Phone STRING,
                Customer_Tier STRING,
                Account_Manager STRING
            )
            """,
            
            # Create Products table (from superstore_product_descriptions.csv)
            """
            CREATE TABLE IF NOT EXISTS SuperstoreDB.data.Products (
                Product_ID STRING PRIMARY KEY,
                Brand STRING,
                Warranty_Years INT,
                Material STRING,
                Release_Date DATE,
                Sustainability_Rating STRING
            )
            """,
            
            # Create stage for data files
            "CREATE STAGE IF NOT EXISTS SuperstoreDB.data.SUPERSTORE_STAGE FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)"
        ]
        
        # Execute each command
        for command in setup_commands:
            try:
                cursor.execute(command)
                print(f"Successfully executed: {command[:50]}...")
            except Exception as e:
                print(f"Error executing command: {command[:50]}...")
                print(f"Error details: {e}")
        
        # Upload local CSV files to Snowflake stage
        print("\nUploading CSV files to Snowflake stage...")
        
        # Get the current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_dir, 'data')
        
        # List of files to upload
        files_to_upload = [
            {'local_path': os.path.join(data_dir, 'superstore.csv'), 'stage_name': 'orders.csv'},
            {'local_path': os.path.join(data_dir, 'superstore_crm_customers.csv'), 'stage_name': 'customers.csv'},
            {'local_path': os.path.join(data_dir, 'superstore_product_descriptions.csv'), 'stage_name': 'products.csv'}
        ]
        
        # Upload each file to the stage
        for file_info in files_to_upload:
            try:
                # Use PUT command to upload file to stage
                put_command = f"PUT file://{file_info['local_path']} @SuperstoreDB.data.SUPERSTORE_STAGE/{file_info['stage_name']} OVERWRITE=TRUE AUTO_COMPRESS=TRUE"
                cursor.execute(put_command)
                print(f"Successfully uploaded {file_info['local_path']} to stage as {file_info['stage_name']}")
            except Exception as e:
                print(f"Error uploading {file_info['local_path']}: {e}")
        
        # Load data from stage into tables
        print("\nLoading data from stage into tables...")
        
        load_commands = [
            """
            COPY INTO SuperstoreDB.data.Orders
            FROM @SuperstoreDB.data.SUPERSTORE_STAGE/orders.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
            """,
            
            """
            COPY INTO SuperstoreDB.data.Customers
            FROM @SuperstoreDB.data.SUPERSTORE_STAGE/customers.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
            """,
            
            """
            COPY INTO SuperstoreDB.data.Products
            FROM @SuperstoreDB.data.SUPERSTORE_STAGE/products.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
            """
        ]
        
        for command in load_commands:
            try:
                cursor.execute(command)
                print(f"Successfully loaded data with: {command[:50]}...")
            except Exception as e:
                print(f"Error loading data: {command[:50]}...")
                print(f"Error details: {e}")
                
        # Create Cortex Search service for product descriptions
        print("\nSetting up Cortex Search service...")
        try:
            search_service_command = """
            CREATE OR REPLACE CORTEX SEARCH SERVICE superstore_product_search
              ON Product_Name, Material, Brand
              ATTRIBUTES Product_ID, Category, Sub_Category, Sustainability_Rating
              WAREHOUSE = SuperstoreWarehouse
              TARGET_LAG = '1 hour'
              AS (
                SELECT
                    o.Product_Name,
                    p.Material,
                    p.Brand,
                    o.Product_ID,
                    o.Category,
                    o.Sub_Category,
                    p.Sustainability_Rating
                FROM SuperstoreDB.data.Orders o
                JOIN SuperstoreDB.data.Products p ON o.Product_ID = p.Product_ID
              )
            """
            cursor.execute(search_service_command)
            print("Successfully created Cortex Search service for product search")
        except Exception as e:
            print(f"Error creating Cortex Search service: {e}")
            
        # Upload and register semantic model for Cortex Analyst
        print("\nUploading semantic model for Cortex Analyst...")
        try:
            # Create a stage for semantic models if it doesn't exist
            cursor.execute("CREATE STAGE IF NOT EXISTS SuperstoreDB.data.SEMANTIC_MODELS")
            
            # Upload the semantic model YAML file to the stage
            semantic_model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'superstore_semantic_model.yaml')
            put_command = f"PUT file://{semantic_model_path} @SuperstoreDB.data.SEMANTIC_MODELS/ OVERWRITE=TRUE AUTO_COMPRESS=TRUE"
            cursor.execute(put_command)
            print("Successfully uploaded semantic model YAML file to Snowflake stage")
            
            # Register the semantic model with Cortex Analyst
            try:
                register_command = """
                CREATE OR REPLACE CORTEX ANALYST MODEL superstore_analyst_model
                FROM @SuperstoreDB.data.SEMANTIC_MODELS/superstore_semantic_model.yaml
                WAREHOUSE = SuperstoreWarehouse
                """
                cursor.execute(register_command)
                print("Successfully registered semantic model with Cortex Analyst")
            except Exception as e:
                print(f"Error registering semantic model with Cortex Analyst: {e}")
                print("Note: This may be expected if your Snowflake account doesn't have Cortex Analyst enabled yet.")
                print("You can still use the Cortex Agent functionality through the API.")
        except Exception as e:
            print(f"Error uploading semantic model: {e}")
        
        cursor.close()
        conn.close()
        print("Database setup completed!")
        return True
    
    except Exception as e:
        print(f"Error setting up database: {e}")
        if conn:
            conn.close()
        return False

if __name__ == "__main__":
    setup_database()
