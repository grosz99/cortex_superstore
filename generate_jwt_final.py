import os
import datetime
import jwt
import hashlib
import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_pem_public_key
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def generate_public_key_fingerprint(public_key_pem):
    """Generate the public key fingerprint (SHA256 hash)."""
    # Load the public key
    public_key = load_pem_public_key(public_key_pem.encode(), default_backend())

    # Get the public key in DER format without the header/footer
    public_key_der = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    # Calculate SHA256 hash and then Base64 encode it
    sha256_hash = hashlib.sha256(public_key_der).digest()
    b64_encoded_hash = base64.b64encode(sha256_hash).decode('utf-8')
    
    return f"SHA256:{b64_encoded_hash}"


def generate_jwt_token():
    """Generate a JWT token for Snowflake authentication following exact Snowflake requirements."""
    # Get environment variables
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')

    print(f"Generating JWT token for account: {account}")
    print(f"User: {user}")

    try:
        # Read the private key
        with open('rsa_key.p8', 'rb') as f:
            private_key_bytes = f.read()
            private_key = load_pem_private_key(
                private_key_bytes,
                password=None,
                backend=default_backend()
            )
        print("Private key loaded successfully")
        
        # Read the public key
        with open('rsa_key.pub', 'r') as f:
            public_key_pem = f.read()
        print("Public key loaded successfully")

        # Generate public key fingerprint
        public_key_fp = generate_public_key_fingerprint(public_key_pem)
        print(f"Public Key Fingerprint: {public_key_fp}")

        # Format account identifier and user name to uppercase
        # Use the full account identifier
        account_identifier = account.upper()
        user_name = user.upper()

        qualified_username = f"{account_identifier}.{user_name}"
        
        # Current time and expiration (1 hour from now)
        now = datetime.datetime.now(datetime.timezone.utc)
        lifetime = datetime.timedelta(minutes=59)

        # Create JWT payload according to Snowflake docs
        payload = {
            'iss': f"{qualified_username}.{public_key_fp}",
            'sub': qualified_username,
            'iat': now,
            'exp': now + lifetime
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
        
        return token

    except Exception as e:
        print(f"Error generating JWT token: {str(e)}")
        return None

if __name__ == "__main__":
    generate_jwt_token()
