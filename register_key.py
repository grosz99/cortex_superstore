import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def register_public_key():
    """
    Register the public key with Snowflake user
    """
    # Get environment variables
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    
    print(f"Connecting to Snowflake account: {account}")
    print(f"User: {user}")
    
    # Read the public key
    try:
        with open('rsa_key.pub', 'r') as f:
            public_key = f.read()
            
        # Format the public key for SQL
        # Remove header and footer lines and join all lines
        public_key_lines = public_key.strip().split('\n')
        if public_key_lines[0] == '-----BEGIN PUBLIC KEY-----':
            public_key_lines = public_key_lines[1:-1]
        formatted_key = ''.join(public_key_lines)
        
        print("Public key loaded successfully")
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
        )
        
        # Create a cursor
        cursor = conn.cursor()
        
        # Execute SQL to register the public key
        alter_user_sql = f"ALTER USER {user} SET RSA_PUBLIC_KEY='{formatted_key}';"
        print(f"Executing SQL: {alter_user_sql}")
        
        cursor.execute(alter_user_sql)
        
        print("Public key registered successfully!")
        
        # Verify the key is registered
        cursor.execute(f"DESC USER {user};")
        user_desc = cursor.fetchall()
        print("\nUser description:")
        for row in user_desc:
            print(row)
        
        # Close the connection
        cursor.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"Error registering public key: {str(e)}")
        return False

if __name__ == "__main__":
    register_public_key()
