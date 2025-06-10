import os
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_keypair_auth():
    """
    Test key-pair authentication with Snowflake
    """
    # Get environment variables
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    
    print(f"Testing key-pair authentication for account: {account}")
    print(f"User: {user}")
    
    try:
        # Read the private key
        with open('rsa_key.p8', 'rb') as f:
            p_key = f.read()
            private_key = load_pem_private_key(
                p_key,
                password=None,
                backend=default_backend()
            )
        
        print("Private key loaded successfully")
        
        # Connect to Snowflake using key-pair authentication
        conn = snowflake.connector.connect(
            user=user,
            account=account,
            private_key=private_key,
            authenticator='SNOWFLAKE_JWT'
        )
        
        # Create a cursor
        cursor = conn.cursor()
        
        # Execute a simple query to verify connection
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_ACCOUNT()")
        result = cursor.fetchone()
        
        print("\nConnection successful!")
        print(f"Current user: {result[0]}")
        print(f"Current role: {result[1]}")
        print(f"Current account: {result[2]}")
        
        # Check if CORTEX_USER role is available
        cursor.execute("SHOW ROLES")
        roles = cursor.fetchall()
        cortex_role_exists = any(role[1] == 'CORTEX_USER' for role in roles)
        
        print(f"\nCORTEX_USER role exists: {cortex_role_exists}")
        
        if cortex_role_exists:
            # Try to use the CORTEX_USER role
            try:
                cursor.execute("USE ROLE CORTEX_USER")
                print("Successfully switched to CORTEX_USER role")
            except Exception as e:
                print(f"Error switching to CORTEX_USER role: {str(e)}")
        
        # Close the connection
        cursor.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"Error testing key-pair authentication: {str(e)}")
        return False

if __name__ == "__main__":
    test_keypair_auth()
