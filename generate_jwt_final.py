# Contents for: C:\Users\PC\CascadeProjects\snowflake-cortex-agent-poc\generate_jwt_final.py

import datetime
import jwt  # PyJWT library
import os # For environment variables if needed for passphrase
import traceback
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
import base64

# No need to load_dotenv here if parameters are passed in
# from dotenv import load_dotenv
# load_dotenv()

def calculate_public_key_fingerprint(public_key_pem: str) -> str:
    """
    Calculates the SHA256 fingerprint of a PEM-encoded public key.
    The fingerprint is returned in the format "SHA256:Base64EncodedDigest".
    """
    try:
        public_key = serialization.load_pem_public_key(
            public_key_pem.encode('utf-8'), 
            backend=default_backend()
        )
        
        der_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        hasher = hashes.Hash(hashes.SHA256(), backend=default_backend())
        hasher.update(der_bytes)
        digest = hasher.finalize()
        
        b64_digest = base64.b64encode(digest).decode('utf-8')
        return f"SHA256:{b64_digest}"
    except Exception as e:
        # It's good to log or print the error, but also re-raise if it's critical
        # print(f"Error calculating public key fingerprint: {e}") 
        raise # Re-raise the exception to signal failure to the caller

def generate_jwt_token(
    snowflake_account: str,  # Snowflake account locator (e.g., "youraccount-id")
    user_name: str,          # Snowflake user name
    private_key_path: str,   # Path to the private key file (e.g., "rsa_key.p8")
    public_key_path: str,    # Path to the public key file (e.g., "rsa_key.pub")
    private_key_passphrase: str = None, # Passphrase for the private key, if encrypted
    lifetime_minutes: int = 59 # JWT token lifetime in minutes
) -> str:
    """
    Generates a JWT token for Snowflake key-pair authentication.
    """
    # print(f"Generating JWT token for account: {snowflake_account}, user: {user_name}")
    # print(f"Private key path: {private_key_path}, Public key path: {public_key_path}")

    try:
        with open(private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=private_key_passphrase.encode('utf-8') if private_key_passphrase else None,
                backend=default_backend()
            )
        
        with open(public_key_path, 'r') as key_file:
            public_key_pem = key_file.read()
        print(f"DEBUG_JWT_INTERNAL: Public key PEM loaded successfully from {public_key_path}")

        public_key_fp = calculate_public_key_fingerprint(public_key_pem)
        print(f"DEBUG_JWT: Public Key Fingerprint: {public_key_fp}")

        qualified_user_name_str = f"{snowflake_account.upper()}.{user_name.upper()}"
        print(f"DEBUG_JWT: Qualified User Name (for sub): {qualified_user_name_str}")
        issuer_str = f"{qualified_user_name_str}.{public_key_fp}"
        print(f"DEBUG_JWT: Issuer (for iss): {issuer_str}")

        now = datetime.datetime.now(datetime.timezone.utc)
        expires_in = datetime.timedelta(minutes=lifetime_minutes) 

        payload = {
            "iss": issuer_str,
            "sub": qualified_user_name_str,
            "iat": now,
            'exp': now + expires_in
        }
        print(f"DEBUG_JWT_INTERNAL: Payload constructed: {payload}")
        print(f"DEBUG_JWT_INTERNAL: Attempting to encode JWT...")
        
        # print(f"JWT Payload: {payload}")

        jwt_token = jwt.encode(
            payload,
            private_key,
            algorithm='RS256'
        )
        print(f"DEBUG_JWT_INTERNAL: JWT encoded successfully.")
        
        # print("JWT token generated successfully!")
        # The part that prints the token for .env is specific to standalone execution
        # and should be removed or handled differently if this is just a library function.
        # print("\n=== JWT TOKEN FOR .env FILE ===")
        # print(f"CORTEX_API_KEY={jwt_token}")
        
        # Ensure payload and public_key_fp are accessible here or passed if necessary
        # Based on the function structure, payload and public_key_fp are defined in the same scope.
        # Make sure jwt_token is a string and does not have 'Bearer' prefix
        if isinstance(jwt_token, bytes):
            jwt_token = jwt_token.decode('utf-8')
        # Ensure we don't add Bearer prefix in the token itself
        if jwt_token.startswith('Bearer '):
            print("WARNING: JWT token already has Bearer prefix, removing it")
            jwt_token = jwt_token[7:]
            
        print(f"DEBUG_JWT_INTERNAL: Final token type: {type(jwt_token)}")
        print(f"DEBUG_JWT_INTERNAL: Final token starts with: '{jwt_token[:10]}...'")
        print(f"DEBUG_JWT_INTERNAL: Returning from generate_jwt_token. Token snippet: '{str(jwt_token)[:50]}...'. Is token None or empty? {not bool(jwt_token)}. Full dict keys: {list({'token': jwt_token, 'payload': payload, 'public_key_fp': public_key_fp}.keys())}")
        
        # Return dictionary with clean JWT token (no Bearer prefix)
        return {"token": jwt_token, "payload": payload, "public_key_fp": public_key_fp}

    except FileNotFoundError as fnf_error:
        print(f"Error generating JWT: Key file not found - {fnf_error}. Searched at {private_key_path} or {public_key_path}")
        raise
    except Exception as e:
        print(f"!!!!!!!!!!!!!! ERROR in generate_jwt_token !!!!!!!!!!!!!!")
        print(f"Exception type: {type(e)}")
        print(f"Exception message: {e}")
        print("Traceback:")
        print(traceback.format_exc())
        print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return None

# The if __name__ == "__main__": block is for standalone testing of this script.
# It's good practice to keep it if you want to test JWT generation independently.
# However, it will need to be adapted to provide the required arguments if you run this file directly.
if __name__ == "__main__":
    print("Testing JWT generation (requires environment variables for direct run):")
    # For standalone testing, you'd need to set these, e.g., from os.getenv or hardcode for a quick test
    # This is just an example and might not run out-of-the-box without setting these up.
    try:
        test_account = os.getenv('SNOWFLAKE_ACCOUNT_TEST', 'your_account_locator') # Replace with actual or env var
        test_user = os.getenv('SNOWFLAKE_USER_TEST', 'your_user') # Replace
        test_priv_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH_TEST', 'rsa_key.p8') # Replace
        test_pub_key_path = os.getenv('SNOWFLAKE_PUBLIC_KEY_PATH_TEST', 'rsa_key.pub') # Replace
        test_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_TEST')

        print(f"Attempting to generate token for {test_account}, {test_user}")
        token = generate_jwt_token(
            snowflake_account=test_account,
            user_name=test_user,
            private_key_path=test_priv_key_path,
            public_key_path=test_pub_key_path,
            private_key_passphrase=test_passphrase
        )
        if token:
            print("\n=== TEST JWT TOKEN (DO NOT USE IN .env directly from here) ===")
            print(token)
    except Exception as e:
        print(f"Error in standalone JWT test: {e}")
