import os
from dotenv import load_dotenv
from generate_jwt_final import generate_jwt_token
import requests
import json
import snowflake.connector

def main():
    # Load environment variables
    load_dotenv()
    
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH', 'rsa_key.p8')
    public_key_path = os.getenv('SNOWFLAKE_PUBLIC_KEY_PATH', 'rsa_key.pub')
    database = os.getenv('SNOWFLAKE_DATABASE', 'SUPERSTOREDB')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'DATA')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    role = os.getenv('SNOWFLAKE_ROLE')
    
    # Base URL for Cortex Agent API
    base_url = f"https://{account}.snowflakecomputing.com/api/v2/cortex/agent:run"
    
    # Generate JWT token
    jwt_data = generate_jwt_token(
        snowflake_account=account,
        user_name=user,
        private_key_path=private_key_path,
        public_key_path=public_key_path
    )
    
    # Extract token string
    token = jwt_data['token']
    print(f"DEBUG: Token type: {type(token)}")
    print(f"DEBUG: Token starts with: {token[:10]}...")
    
    # Create headers with proper Bearer token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"
    }
    
    # Create conversation messages
    messages = [
        {
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": "You're a helpful assistant for analyzing Superstore retail data."
                }
            ]
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "What is the top selling category?"
                }
            ]
        }
    ]
    
    # Define tools for SQL execution - using snake_case as required by the API
    # Tool names must match exactly between tools and tool_resources
    tools = [
        {
            "tool_spec": {
                "type": "cortex_analyst_text_to_sql",
                "name": "cortex_analyst_text_to_sql"
            }
        },
        {
            "tool_spec": {
                "type": "sql_exec",
                "name": "sql_exec"
            }
        }
    ]
    
    # Create tool resources with matching names
    tool_resources = {
        "cortex_analyst_text_to_sql": {
            "semantic_model_file": f"@{database}.{schema}.SUPERSTORE_STAGE/superstore_semantic_model.yaml"
        },
        "sql_exec": {
            "warehouse": warehouse,
            "timeout": 60  # Timeout in seconds
        }
    }
    
    # Create payload
    payload = {
        "model": "llama3.1-70b",
        "messages": messages,
        "tools": tools,
        "tool_resources": tool_resources,
        "response_instruction": "You will always maintain a friendly tone and provide concise response."
    }
    
    print(f"Sending request to {base_url}")
    
    # Print the full JSON payload for debugging
    print("\n=== FULL JSON PAYLOAD ===\n")
    print(json.dumps(payload, indent=2, default=str))
    print("\n=== END JSON PAYLOAD ===\n")
    
    # Send request
    try:
        print("Sending POST request...")
        response = requests.post(
            base_url,
            headers=headers,
            json=payload,
            stream=True,
            timeout=300
        )
        
        print(f"Response status: {response.status_code}")
        
        # Print full response for debugging
        if response.status_code != 200:
            print("\n=== FULL ERROR RESPONSE ===\n")
            try:
                print(json.dumps(response.json(), indent=2))
            except Exception as e:
                print(f"Error parsing JSON response: {e}")
                print(response.text)
            print("\n=== END ERROR RESPONSE ===\n")
        
        # Always print response headers for debugging
        print("Response headers:")
        print(json.dumps(dict(response.headers), indent=2))
        
        if response.status_code == 200:
            print("\n=== STREAMING RESPONSE ===\n")
            # Process streaming response
            extracted_sql = None
            final_answer = None
            
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    print(f"RAW: {decoded_line}")
                    
                    if decoded_line.startswith('data:'):
                        json_str = decoded_line[5:].strip()
                        if json_str == "[DONE]":
                            print("Stream completed with [DONE] marker")
                            continue
                            
                        try:
                            chunk_data = json.loads(json_str)
                            print(f"CHUNK: {json.dumps(chunk_data, indent=2)[:200]}...")
                            
                            # Look for SQL query in tool_use
                            if 'delta' in chunk_data and 'content' in chunk_data['delta']:
                                for content_item in chunk_data['delta']['content']:
                                    if content_item.get('type') == 'tool_use':
                                        tool_use = content_item.get('tool_use', {})
                                        
                                        # Extract SQL from Analyst tool
                                        if tool_use.get('name') == 'cortex_analyst_text_to_sql' and 'output' in tool_use:
                                            output = tool_use['output']
                                            if 'query' in output:
                                                extracted_sql = output['query']
                                                print("\n=== EXTRACTED SQL ===\n")
                                                print(extracted_sql)
                                                print("\n=== END EXTRACTED SQL ===\n")
                                        
                                        # Extract final answer from text response
                                        if 'output' in tool_use and 'text' in tool_use['output']:
                                            final_answer = tool_use['output']['text']
                                            print("\n=== FINAL ANSWER ===\n")
                                            print(final_answer)
                                            print("\n=== END FINAL ANSWER ===\n")
                                    
                                    # Look for text content (final answer)
                                    if content_item.get('type') == 'text':
                                        if 'text' in content_item:
                                            final_answer = content_item['text']
                                            print("\n=== FINAL ANSWER (TEXT) ===\n")
                                            print(final_answer)
                                            print("\n=== END FINAL ANSWER ===\n")
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON: {e}")
                            print(f"Raw JSON string: {json_str}")
            
            # After processing all streaming data, print summary
            print("\n=== SUMMARY ===\n")
            if extracted_sql:
                print(f"SQL Query: {extracted_sql[:100]}...")
            else:
                print("No SQL query extracted")
                
            if final_answer:
                print(f"Final Answer: {final_answer}")
            else:
                print("No final answer extracted")
            print("\n=== END SUMMARY ===\n")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
