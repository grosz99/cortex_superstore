import os
import json
import base64
import requests
import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def decode_jwt_without_verification(jwt_token):
    """
    Decode a JWT token without verifying the signature
    to inspect its contents
    """
    parts = jwt_token.split('.')
    if len(parts) != 3:
        return "Invalid JWT token format"
    
    # Decode the header and payload
    try:
        header = json.loads(base64.urlsafe_b64decode(parts[0] + '=' * (4 - len(parts[0]) % 4)).decode('utf-8'))
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + '=' * (4 - len(parts[1]) % 4)).decode('utf-8'))
        return {
            "header": header,
            "payload": payload
        }
    except Exception as e:
        return f"Error decoding JWT: {str(e)}"

def test_different_account_formats():
    """
    Test different account identifier formats in the JWT token
    to see which one works with Snowflake
    """
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    api_key = os.getenv('CORTEX_API_KEY')
    
    print("\n=== JWT TOKEN TROUBLESHOOTING ===")
    print(f"Account from env: {account}")
    
    # Decode the current JWT token
    print("\n=== CURRENT JWT TOKEN CONTENTS ===")
    decoded = decode_jwt_without_verification(api_key)
    print(json.dumps(decoded, indent=2))
    
    # Extract account parts
    account_parts = account.split('-')
    account_name = account_parts[0]
    
    print("\n=== ACCOUNT FORMAT OPTIONS ===")
    print(f"1. Full account: {account}")
    print(f"2. Account name only: {account_name}")
    
    # Test different API endpoints
    base_urls = [
        f"https://{account}.snowflakecomputing.com/api/v2/cortex",
        f"https://{account_name}.snowflakecomputing.com/api/v2/cortex"
    ]
    
    for i, base_url in enumerate(base_urls):
        print(f"\n=== TESTING ENDPOINT {i+1}: {base_url}/agent:run ===")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        # Create a simple test payload
        payload = {
            "model": "llama3.1-70b",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Hello"
                        }
                    ]
                }
            ]
        }
        
        try:
            response = requests.post(
                f"{base_url}/agent:run",
                headers=headers,
                json=payload
            )
            
            print(f"Status code: {response.status_code}")
            
            if response.status_code == 200 or response.status_code == 201:
                print("SUCCESS! Connection to Cortex Agent API established.")
                print(f"Response: {json.dumps(response.json(), indent=2)[:500]}...")
            else:
                print(f"ERROR: Failed to connect to Cortex Agent API")
                print(f"Response: {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Request failed: {e}")

def check_snowflake_documentation():
    """
    Print Snowflake documentation references for JWT token format
    """
    print("\n=== SNOWFLAKE JWT TOKEN DOCUMENTATION ===")
    print("According to Snowflake documentation:")
    print("1. The JWT token should use RS256 algorithm")
    print("2. The 'iss' and 'sub' claims should be in the format: 'username.accountname'")
    print("3. The account name should NOT include region or cloud provider")
    print("4. The token should include 'iat' (issued at) and 'exp' (expiration) claims")
    print("5. The public key must be registered with the Snowflake user")
    print("\nReference: https://docs.snowflake.com/en/user-guide/key-pair-auth")

if __name__ == "__main__":
    test_different_account_formats()
    check_snowflake_documentation()
