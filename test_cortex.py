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
    
    # Print headers for debugging
    debug_headers = headers.copy()
    if 'Authorization' in debug_headers:
        auth_parts = debug_headers['Authorization'].split('Bearer ', 1)
        if len(auth_parts) > 1:
            token_str = auth_parts[1]
            debug_headers['Authorization'] = f"Bearer {token_str[:10]}...{token_str[-5:] if len(token_str) > 5 else token_str}"
    print("DEBUG: Headers:")
    print(json.dumps(debug_headers, indent=2))
    
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
    
    # Define tools for SQL execution using snake_case as required by the API
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
    
    # Create semantic model resource
    semantic_model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixed_semantic_model.yaml')
    with open(semantic_model_path, 'r') as file:
        semantic_model_yaml = file.read()
    
    # Get warehouse from environment variables
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    
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
    print("\n=== FULL JSON PAYLOAD (BEFORE SERIALIZATION) ===\n")
    print(json.dumps(payload, indent=2, default=str))
    print("\n=== END JSON PAYLOAD ===\n")
    
    # Print the exact serialized JSON being sent to the API
    serialized_json = json.dumps(payload)
    print("\n=== SERIALIZED JSON ===\n")
    print(serialized_json)
    print("\n=== END SERIALIZED JSON ===\n")
    
    # Send request
    try:
        print("Sending POST request...")
        
        # Print the final payload after serialization
        print("\n=== FINAL PAYLOAD AFTER SERIALIZATION ===\n")
        print(json.dumps(payload, indent=2))
        print("\n=== END FINAL PAYLOAD ===\n")
        
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
            except:
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
            all_chunks = []
            
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
                            all_chunks.append(chunk_data)
                            
                            # Look for SQL query in tool_use
                            if 'delta' in chunk_data and 'content' in chunk_data['delta']:
                                for content_item in chunk_data['delta']['content']:
                                    if content_item.get('type') == 'tool_use':
                                        tool_use = content_item.get('tool_use', {})
                                        
                                        # Extract SQL from Analyst tool
                                        if tool_use.get('name') == 'Analyst' and 'output' in tool_use:
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
            
            # Summary of results
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
            
            if extracted_sql:
                # Execute SQL query
                print("Connecting to Snowflake...")
                conn = snowflake.connector.connect(
                    user=user,
                    account=account,
                    private_key_path=private_key_path,
                    database=database,
                    schema=schema,
                    warehouse=warehouse,
                    role=role
                )
                
                try:
                    cursor = conn.cursor()
                    print("Executing SQL...")
                    cursor.execute(extracted_sql)
                    query_id = cursor.sfqid
                    print(f"SQL executed successfully. Query ID: {query_id}")
                    
                    # Send follow-up request with query ID
                    follow_up_messages = messages.copy()
                    follow_up_messages.append({
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_results",
                                "tool_results": {
                                    "tool_use_id": tool_use_id,
                                    "result": {
                                        "query_id": query_id
                                    }
                                }
                            }
                        ]
                    })
                    
                    # Generate new JWT token for follow-up request
                    jwt_data = generate_jwt_token(
                        snowflake_account=account,
                        user_name=user,
                        private_key_path=private_key_path,
                        public_key_path=public_key_path
                    )
                    
                    # Create headers with proper Bearer token
                    follow_up_headers = {
                        "Authorization": f"Bearer {jwt_data['token']}",
                        "Content-Type": "application/json",
                        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"
                    }
                    
                    follow_up_payload = {
                        "model": "llama3.1-70b",
                        "messages": follow_up_messages,
                        "tools": tools,
                        "tool_resources": tool_resources,
                        "response_instruction": "You will always maintain a friendly tone and provide concise response."
                    }
                    
                    print("\nSending follow-up request...")
                    follow_up_response = requests.post(
                        base_url,
                        headers=follow_up_headers,
                        json=follow_up_payload,
                        stream=True,
                        timeout=300
                    )
                    
                    print(f"Follow-up response status: {follow_up_response.status_code}")
                    
                    if follow_up_response.status_code == 200:
                        print("\n--- Final Answer ---")
                        for line in follow_up_response.iter_lines():
                            if line:
                                decoded_line = line.decode('utf-8')
                                if decoded_line.startswith('data:'):
                                    json_str = decoded_line[5:].strip()
                                    try:
                                        chunk_data = json.loads(json_str)
                                        if 'assistant_response' in chunk_data:
                                            print(f"ANSWER: {chunk_data['assistant_response']}")
                                        elif 'delta' in chunk_data and 'content' in chunk_data['delta']:
                                            for content_item in chunk_data['delta'].get('content', []):
                                                if content_item.get('type') == 'text':
                                                    print(content_item.get('text', ''), end='')
                                    except json.JSONDecodeError:
                                        print(f"Could not decode JSON: {json_str}")
                    else:
                        print(f"Follow-up request failed: {follow_up_response.text}")
                        
                finally:
                    cursor.close()
                    conn.close()
            
                                                                    for content_item in chunk_data['delta'].get('content', []):
                                                                        if content_item.get('type') == 'text':
                                                                            print(content_item.get('text', ''), end='')
                                                            except json.JSONDecodeError:
                                                                print(f"Could not decode JSON: {json_str}")
                                            else:
                                                print(f"Follow-up request failed: {follow_up_response.text}")
                                                
                                        finally:
                                            cursor.close()
                                            conn.close()
                            
                            # Print other response data
                            if 'assistant_response' in chunk_data:
                                print(f"\nDirect assistant response: {chunk_data['assistant_response']}")
                                
                        except json.JSONDecodeError:
                            print(f"Could not decode JSON: {json_str}")
        else:
            print(f"Request failed: {response.text}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
