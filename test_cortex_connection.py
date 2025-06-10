import os
import requests
import json
from dotenv import load_dotenv
from cortex_agent import CortexAgent
from generate_jwt_final import generate_jwt_token # <--- IMPORT ADDED

# Load environment variables, overriding any existing system variables
load_dotenv(override=True)

def test_cortex_connection():
    """
    Test the connection to the Cortex Agent API with detailed error reporting
    """
    # Get environment variables
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    # api_key = os.getenv('CORTEX_API_KEY') # <--- REMOVED
    
    print("\n=== CORTEX AGENT API CONNECTION TEST ===")
    print(f"Account: {account}")

    # Generate JWT token on the fly
    print("Generating JWT token...")
    api_key = generate_jwt_token() # <--- ADDED: Generate token directly

    if not api_key: # <--- ADDED: Check if token generation was successful
        print("ERROR: Failed to generate JWT token. Please check generate_jwt_final.py script and rsa keys.")
        return False

    loaded_api_key_display = "Missing" # This display logic can remain
    if api_key:
        if len(api_key) > 20:
            loaded_api_key_display = f"{api_key[:10]}...{api_key[-10:]}"
        else:
            loaded_api_key_display = api_key
    print(f"API Key (Generated): {loaded_api_key_display}") # <--- MODIFIED: Changed "Loaded" to "Generated"
    
    # Check if environment variables are set
    if not account:
        print("ERROR: SNOWFLAKE_ACCOUNT environment variable not set")
        return False # <--- MODIFIED: Return False for consistency
    
    # No need to check for api_key from env anymore, as we generate it
    # if not api_key:
    #     print("ERROR: CORTEX_API_KEY environment variable not set")
    #     print("You need to generate a JWT token using key-pair authentication")
    #     print("See: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents.html")
    #     return False
    
    # Test the new v2 API endpoint
    base_url = f"https://{account}.snowflakecomputing.com/api/v2/cortex"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"
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
    
    print(f"\nTesting API endpoint: {base_url}/agent:run")
    
    try:
        print("Attempting to send request...")
        response = requests.post(
            f"{base_url}/agent:run",
            headers=headers,
            json=payload,
            stream=True, # Enable streaming
            timeout=30  # Add a 30-second timeout
        )
        print("Request sent. Response received (or error during request).")
        
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            print("SUCCESS! Connection to Cortex Agent API established.")
            print("Streaming response:")
            full_response_text = ""
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith('data:'):
                        # Strip the 'data: ' prefix to get the JSON string
                        json_str = decoded_line[5:].strip()
                        if json_str == "[DONE]":
                            break # Stream is finished
                        try:
                            # Parse the JSON data
                            json_chunk = json.loads(json_str)
                            # Extract and append the text content from the delta
                            if "delta" in json_chunk and "content" in json_chunk["delta"]:
                                for content_item in json_chunk["delta"]["content"]:
                                    if "text" in content_item:
                                        text_chunk = content_item["text"]
                                        print(text_chunk, end="", flush=True)
                                        full_response_text += text_chunk
                        except json.JSONDecodeError:
                            print(f"\nCould not decode JSON from data: {json_str}")
            
            print("\n--- End of Stream ---")
            return True 
        else:
            print("ERROR: Failed to connect to Cortex Agent API")
            try:
                error_response_json = response.json()
                print(f"Response: {json.dumps(error_response_json, indent=2)}")
            except json.JSONDecodeError:
                print(f"Response (raw): {response.text}")
            return False 

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Request failed: {e}")
        return False 
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        return False
    
    print("\n=== TROUBLESHOOTING SUGGESTIONS ===")
    print("1. Verify your Snowflake account has Cortex Agents enabled")
    print("2. Check that your user has the CORTEX_USER role")
    print("3. Verify your JWT token is correctly formatted and not expired")
    print("4. Ensure your public key is properly registered in Snowflake")
    print("5. Check if your account region supports Cortex Agents")
    print("6. Verify the API endpoint format matches the documentation")

def test_cortex_agent_class():
    """
    Test the CortexAgent class implementation
    """
    print("\n=== TESTING CORTEX AGENT CLASS ===")
    agent = CortexAgent()
    
    conversation_id = agent.start_conversation()
    if not conversation_id:
        print("Failed to start conversation")
        return
    
    print(f"Started conversation with ID: {conversation_id}")
    
    print("\nSending test message...")
    response = agent.send_message("What are the top 3 products by sales?")
    
    if response:
        print("\nResponse received from CortexAgent class:")
        response_text = response.get("response")
        if response_text:
            print(response_text)
        else:
            print("[No response text received from CortexAgent class or text was empty]")
    else:
        print("Failed to get response")

if __name__ == "__main__":
    connection_success = test_cortex_connection()
    
    if connection_success:
        print("\nTest_cortex_connection successful. Now testing CortexAgent class...")
        test_cortex_agent_class() 
    else:
        print("\nTest_cortex_connection failed. Skipping CortexAgent class test.")
