import os
import datetime
import jwt
import hashlib
import base64
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv, find_dotenv, set_key

# --- Configuration ---
PRIVATE_KEY_FILE = 'rsa_key.p8'
PUBLIC_KEY_FILE = 'rsa_key.pub'

def generate_key_pair():
    """Generates and saves a new RSA private and public key pair."""
    print("--- Step 1: Generating new RSA key pair ---")
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()

    # Save private key
    pem_private = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(PRIVATE_KEY_FILE, 'wb') as f:
        f.write(pem_private)
    print(f"Private key saved to {PRIVATE_KEY_FILE}")

    # Save public key
    pem_public = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    with open(PUBLIC_KEY_FILE, 'wb') as f:
        f.write(pem_public)
    print(f"Public key saved to {PUBLIC_KEY_FILE}")

def register_and_verify_key(user, account, password):
    """Registers the public key and verifies the fingerprint from Snowflake."""
    print("\n--- Step 2: Registering and Verifying Public Key ---")
    with open(PUBLIC_KEY_FILE, 'r') as f:
        public_key_str = f.read()
    
    public_key_stripped = ''.join(public_key_str.strip().split('\n')[1:-1])

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
    )
    
    try:
        # Register the key
        sql_alter = f"ALTER USER {user} SET RSA_PUBLIC_KEY='{public_key_stripped}';"
        print(f"Executing: ALTER USER {user} SET RSA_PUBLIC_KEY='...' ")
        conn.cursor().execute(sql_alter)
        print("Public key registered successfully.")

        # Verify the fingerprint
        sql_desc = f"DESCRIBE USER {user};"
        print(f"Executing: {sql_desc}")
        cursor = conn.cursor().execute(sql_desc)
        for row in cursor:
            if row[0] == 'RSA_PUBLIC_KEY_FP':
                fingerprint = row[1]
                print(f"Successfully retrieved fingerprint from Snowflake: {fingerprint}")
                return fingerprint
        raise ValueError("Could not retrieve public key fingerprint from Snowflake.")
    finally:
        conn.close()

def generate_jwt_token(fingerprint, user, account):
    """Generates the JWT token using the verified fingerprint."""
    print("\n--- Step 3: Generating JWT Token ---")
    
    # Load the private key from file
    print("Loading private key from file...")
    with open(PRIVATE_KEY_FILE, 'rb') as f:
        private_key_bytes = f.read()
        private_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=None,
            backend=default_backend()
        )
    print("Private key loaded.")

    account_identifier = account.upper()
    user_name = user.upper()
    qualified_username = f"{account_identifier}.{user_name}"

    now = datetime.datetime.now(datetime.timezone.utc)
    lifetime = datetime.timedelta(minutes=59)

    payload = {
        'iss': f"{qualified_username}.{fingerprint}",
        'sub': qualified_username,
        'iat': now,
        'exp': now + lifetime
    }
    print(f"JWT Payload: {payload}")

    token = jwt.encode(payload, private_key, algorithm='RS256')
    print("JWT token generated successfully.")
    return token

def update_env_file(token):
    """Updates the .env file with the new token."""
    print("\n--- Step 4: Updating .env file ---")
    env_path = find_dotenv()
    if not env_path:
        print("Warning: .env file not found. Please create one and add the key manually.")
        env_path = '.env'

    set_key(env_path, 'CORTEX_API_KEY', token)
    print(f"Successfully updated CORTEX_API_KEY in {env_path}")


def main():
    """Main function to run the complete authentication setup."""
    load_dotenv()
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')

    if not all([account, user, password]):
        print("Error: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD must be set in .env file.")
        return

    try:
        generate_key_pair()
        fingerprint = register_and_verify_key(user, account, password)
        token = generate_jwt_token(fingerprint, user, account)

        print("\n--- COPY THIS INTO YOUR .env FILE ---")
        print(f"CORTEX_API_KEY={token}")
        print("-------------------------------------\n")

        update_env_file(token)
        print("\n✅ Authentication setup complete! You can now run the application.")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")

if __name__ == "__main__":
    main()
