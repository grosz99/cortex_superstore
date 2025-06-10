import os
import datetime
import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def generate_jwt_token():
    """
    Generate a JWT token for Snowflake authentication following exact Snowflake requirements
    """
    # Get environment variables
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    
    print(f"Generating JWT token for account: {account}")
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
        
        # Format account identifier correctly - remove region and cloud provider if present
        account_parts = account.split('-')
        account_name = account_parts[0]  # Take just the account name part
        
        # Current time and expiration (1 hour from now)
        now = datetime.datetime.now(datetime.UTC)
        expiration = now + datetime.timedelta(hours=1)
        
        # Construct the proper account identifier
        qualified_username = f"{user}.{account_name}"
        
        # Create JWT payload according to Snowflake docs
        # https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#using-key-pair-authentication
        payload = {
            'iss': qualified_username,  # Format: username.account
            'sub': qualified_username,  # Format: username.account
            'iat': int(now.timestamp()),
            'exp': int(expiration.timestamp())
        }
        
        print(f"JWT Payload: {payload}")
        
        # Generate the JWT token with RS256 algorithm
        token = jwt.encode(
            payload,
            private_key,
            algorithm='RS256'
        )
        
        print("JWT token generated successfully!")
        print("\n=== JWT TOKEN FOR .env FILE ===")
        print(f"CORTEX_API_KEY={token}")
        
        # Update the .env file
        env_file_path = '.env'
        updated_lines = []
        
        with open(env_file_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith('CORTEX_API_KEY='):
                    updated_lines.append(f"CORTEX_API_KEY={token}\n")
                else:
                    updated_lines.append(line)
        
        with open(env_file_path, 'w') as f:
            f.writelines(updated_lines)
        
        print("\nUpdated .env file with new JWT token")
        
        return token
    
    except Exception as e:
        print(f"Error generating JWT token: {str(e)}")
        return None

if __name__ == "__main__":
    generate_jwt_token()
