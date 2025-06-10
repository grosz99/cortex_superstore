import os
import datetime
import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import jwt

def generate_key_pair():
    """Generate an RSA key pair and save to files"""
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    
    # Save private key to file
    with open('rsa_key.p8', 'wb') as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
    
    # Get public key
    public_key = private_key.public_key()
    
    # Save public key to file
    with open('rsa_key.pub', 'wb') as f:
        f.write(public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ))
    
    # Format public key for Snowflake
    public_key_str = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode('utf-8')
    
    # Remove header, footer and newlines for Snowflake
    public_key_snowflake = ''.join(public_key_str.strip().split('\n')[1:-1])
    
    return private_key, public_key_snowflake

def generate_jwt_token(private_key, account, username):
    """Generate a JWT token for Snowflake authentication"""
    # Use datetime.UTC instead of utcnow() to avoid deprecation warning
    now = datetime.datetime.now(datetime.UTC)
    expiration = now + datetime.timedelta(hours=1)  # Token valid for 1 hour
    
    # Format account identifier correctly - remove region and cloud provider if present
    account_parts = account.split('-')
    account_name = account_parts[0]  # Take just the account name part
    
    # Construct the proper account identifier
    qualified_username = f"{username}.{account_name}"
    
    payload = {
        'iss': qualified_username,  # Format: username.account
        'sub': qualified_username,  # Format: username.account
        'iat': int(now.timestamp()),
        'exp': int(expiration.timestamp())
    }
    
    print(f"JWT Payload: {payload}")
    
    # Generate the JWT token
    token = jwt.encode(
        payload,
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ),
        algorithm='RS256'
    )
    
    return token

def main():
    print("Generating RSA key pair...")
    private_key, public_key_snowflake = generate_key_pair()
    print("RSA key pair generated successfully!")
    print("\nPrivate key saved to: rsa_key.p8")
    print("Public key saved to: rsa_key.pub")
    
    print("\n=== INSTRUCTIONS FOR SNOWFLAKE ===")
    print("Run this SQL command in Snowflake to register your public key:")
    print(f"ALTER USER J99G SET RSA_PUBLIC_KEY='{public_key_snowflake}';")
    print("\nTo verify the key is registered, run:")
    print("DESC USER J99G;")
    
    # Generate JWT token
    account = os.getenv('SNOWFLAKE_ACCOUNT', 'laqwzde-umc37678')
    username = os.getenv('SNOWFLAKE_USER', 'J99G')
    
    print("\nGenerating JWT token for Cortex Agent API...")
    token = generate_jwt_token(private_key, account, username)
    print("JWT token generated successfully!")
    
    print("\n=== JWT TOKEN FOR .env FILE ===")
    print("Add this to your .env file:")
    print(f"CORTEX_API_KEY={token}")
    
    print("\n=== REQUIRED PERMISSIONS ===")
    print("Run these SQL commands in Snowflake to grant necessary permissions:")
    print("""
-- Grant CORTEX_USER role
CREATE ROLE IF NOT EXISTS CORTEX_USER;
GRANT ROLE CORTEX_USER TO USER J99G;

-- Grant usage on the database and schema
GRANT USAGE ON DATABASE SuperstoreDB TO ROLE CORTEX_USER;
GRANT USAGE ON SCHEMA SuperstoreDB.data TO ROLE CORTEX_USER;

-- Grant select on tables
GRANT SELECT ON ALL TABLES IN SCHEMA SuperstoreDB.data TO ROLE CORTEX_USER;

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE SuperstoreWarehouse TO ROLE CORTEX_USER;
    """)

if __name__ == "__main__":
    main()
