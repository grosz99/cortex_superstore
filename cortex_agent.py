import os
import requests
import json
import os
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from generate_jwt_final import generate_jwt_token

# Load environment variables, overriding any existing system variables
load_dotenv(override=True)

class CortexAgent:
    def __init__(self, account=None, user=None, private_key_path=None, public_key_path=None, database=None, schema=None, timeout=300):
        self.account = account if account else os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = user if user else os.getenv('SNOWFLAKE_USER')
        self.private_key_path = private_key_path if private_key_path else os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH', 'rsa_key.p8')
        self.public_key_path = public_key_path if public_key_path else os.getenv('SNOWFLAKE_PUBLIC_KEY_PATH', 'rsa_key.pub')
        self.database = database if database else os.getenv('SNOWFLAKE_DATABASE', 'SUPERSTOREDB')
        self.schema = schema if schema else os.getenv('SNOWFLAKE_SCHEMA', 'DATA')
        self.timeout = timeout
        self.snowflake_role = os.getenv('SNOWFLAKE_ROLE')
        self.snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        # self.api_key = os.getenv('CORTEX_API_KEY') # Removed: Token will be generated per request
        
        if not self.account:
            print("ERROR: SNOWFLAKE_ACCOUNT environment variable not set")
        
        # Removed api_key check from init as it's generated per request
        
        # Format the base URL according to Snowflake documentation
        # The format is: https://<account>.snowflakecomputing.com/api/v2/cortex/agent:run
        self.base_url = f"https://{self.account}.snowflakecomputing.com/api/v2/cortex" if self.account else ""
        # Store tool configurations for potential reuse in follow-up calls
        self.tools_payload = []
        self.tool_resources_payload = {}
        # self.headers = {} # Removed: Headers will be set per request
        self.conversation_id = None
        self.messages = []
        self.last_raw_response = []  # Store raw response chunks for debugging
    
    def start_conversation(self):
        """
        Initialize a new conversation with the Cortex Agent
        """
        # Removed api_key check as it's generated on-demand in send_message
            
        if not self.account:
            print("ERROR: Cannot start conversation - Snowflake account is missing")
            return None
        
        # Reset messages to start a new conversation
        self.messages = [
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": "You're a helpful assistant for analyzing Superstore retail data."
                    }
                ]
            }
        ]
        
        # Generate a unique conversation ID
        import uuid
        self.conversation_id = str(uuid.uuid4())
        print(f"Started conversation with ID: {self.conversation_id}")
        return self.conversation_id
    
    def _load_private_key(self):
        """Loads the private key from the path specified in environment variables."""
        pk_path = self.private_key_path
        passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') # Ensure this is in your .env if key is encrypted

        with open(pk_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase.encode() if passphrase else None,
                backend=default_backend()
            )
        return p_key

    def _get_snowflake_connection(self):
        """Establishes a connection to Snowflake using key-pair authentication."""
        private_key = self._load_private_key()
        
        conn_params = {
            'user': self.user,
            'account': self.account,
            'private_key': private_key,
            'database': self.database,
            'schema': self.schema,
        }
        if self.snowflake_role:
            conn_params['role'] = self.snowflake_role
        if self.snowflake_warehouse:
            conn_params['warehouse'] = self.snowflake_warehouse

        try:
            conn = snowflake.connector.connect(**conn_params)
            print("Snowflake connection successful.")
            return conn
        except Exception as e:
            print(f"Snowflake connection failed: {e}")
            raise

    def send_message(self, message, conversation_id=None):
        """
        Send a message to the Cortex Agent
        """
        if not conversation_id and not self.conversation_id:
            conversation_id = self.start_conversation()
        elif not conversation_id:
            conversation_id = self.conversation_id
        
        if not conversation_id:
            print("No valid conversation ID. Cannot send message.")
            return None
        
        # Check if the last message was from the user
        # If so, we need to ensure we don't add another user message
        # This prevents the "Role must change after every message" error
        if not self.messages or self.messages[-1]["role"] != "user":
            # Add user message to conversation history
            self.messages.append({
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": message
                    }
                ]
            })
        else:
            print("Warning: Last message was already from user. Updating the last message instead.")
            # Update the last user message instead of adding a new one
            self.messages[-1]["content"] = [
                {
                    "type": "text",
                    "text": message
                }
            ]

        jwt_token_data = generate_jwt_token(
            snowflake_account=self.account,
            user_name=self.user,
            private_key_path=self.private_key_path,
            public_key_path=self.public_key_path,
            private_key_passphrase=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        )
        if not jwt_token_data or 'token' not in jwt_token_data:
            print("ERROR: CortexAgent - Failed to generate JWT token. Check generate_jwt_final.py and RSA keys.")
            return None

        # Extract just the token string from the dictionary
        token_string = jwt_token_data['token']
        print(f"DEBUG: Token type: {type(token_string)}")
        
        # Directly set the headers with Bearer prefix only once
        headers = {
            "Authorization": f"Bearer {token_string}",
            "Content-Type": "application/json",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
        
        try:
            # Define tools and resources (should be consistent across calls if needed by agent)
            # For the sql_exec flow, we need 'cortex_analyst_text_to_sql' and 'sql_exec'
            # and potentially 'data_to_chart' if we want charts.
            self.tools_payload = [
                {
                    "tool_spec": {
                        "type": "cortex_analyst_text_to_sql",
                        "name": "data_model" # This was our chosen name for this tool type
                    }
                },
                {
                    "tool_spec": {
                        "type": "sql_exec",
                        "name": "sql_execution_tool" # A chosen name for this tool type
                    }
                }
                # Add data_to_chart if desired later
            ]
            self.tool_resources_payload = {
                "data_model": { # Matches the name given in tools_payload
                    "semantic_model_file": f"@{self.database}.{self.schema}.SUPERSTORE_STAGE/superstore_semantic_model.yaml"
                }
                # sql_execution_tool typically doesn't need resources specified here
            }

            # Prepare the request payload according to the documentation
            payload = {
                "model": "llama3.1-70b",
                "messages": self.messages,
                "tools": self.tools_payload,
                "tool_resources": self.tool_resources_payload,
                "response_instruction": "You will always maintain a friendly tone and provide concise response."
                # Add other relevant top-level payload keys if needed, like 'experimental', 'tool_choice'
            }
            
            # Send the request to the agent:run endpoint with streaming enabled
            print(f"CortexAgent: Preparing to send POST request to {self.base_url}/agent:run with timeout={self.timeout}")
            
            # EMERGENCY FIX - Directly clean the Authorization header if it has a double Bearer
            if headers['Authorization'].startswith('Bearer Bearer '):
                print("DEBUG: Found double Bearer prefix, fixing it")
                # Remove the extra Bearer
                headers['Authorization'] = headers['Authorization'].replace('Bearer Bearer ', 'Bearer ')
        
            # Debug information - mask sensitive parts of the JWT token
            debug_headers = headers.copy()
            if 'Authorization' in debug_headers:
                # Get the raw token part (after 'Bearer ')
                auth_parts = debug_headers['Authorization'].split('Bearer ', 1)
                if len(auth_parts) > 1:
                    token_str = auth_parts[1]
                    # Show simplified token
                    debug_headers['Authorization'] = f"Bearer {token_str[:10]}...{token_str[-5:] if len(token_str) > 5 else token_str}"
            print("DEBUG_API_REQUEST: Headers:")
            print(json.dumps(debug_headers, indent=2))
            
            # Print payload structure without full content
            debug_payload = {
                "model": payload.get("model"),
                "messages": f"{len(payload.get('messages', []))} messages",
                "tools": f"{len(payload.get('tools', []))} tools defined",
                "tool_resources": "Present" if payload.get("tool_resources") else "Not present",
                "response_instruction": payload.get("response_instruction", "Not specified")
            }
            print("DEBUG_API_REQUEST: Payload structure:")
            print(json.dumps(debug_payload, indent=2))
            
            response = requests.post(
                f"{self.base_url}/agent:run",
                headers=headers, 
                json=payload,
                stream=True,
                timeout=self.timeout  
            )
            print(f"CortexAgent: POST request completed. Status: {response.status_code if response else 'No response object'}")
            response.raise_for_status()
            
            # Print the full URL for debugging
            print(f"Request URL: {self.base_url}/agent:run")
            
            # Process the streaming response from the Cortex Agent
            self.last_raw_response = []  # Clear previous raw responses
            print("--- CortexAgent: Streaming Response --- ")
            
            # Accumulate parts of the assistant's message if it's multi-chunk
            current_assistant_message_content_parts = []
            assistant_response_text = ""
            
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith('data:'):
                        json_str = decoded_line[5:].strip()
                        if json_str == "[DONE]":
                            print("CortexAgent: DONE signal received.")
                            break
                        try:
                            json_chunk = json.loads(json_str)
                            self.last_raw_response.append(json_chunk)

                            if json_chunk.get("object") == "message.delta" and "delta" in json_chunk:
                                delta_obj = json_chunk["delta"]
                                if "content" in delta_obj:
                                    for content_item in delta_obj.get('content', []):
                                        current_assistant_message_content_parts.append(content_item) # Add to current message parts
                                        if content_item.get("type") == "tool_use" and content_item.get("tool_use", {}).get("name") == "sql_execution_tool":
                                            print("CortexAgent: sql_exec tool_use detected.")
                                            sql_query = content_item["tool_use"]["input"]["query"]
                                            tool_use_id = content_item["tool_use"]["tool_use_id"]
                                            
                                            # Add the assistant's message that led to this tool_use to self.messages
                                            if delta_obj.get('role') == 'assistant' and current_assistant_message_content_parts:
                                                self.messages.append({"role": "assistant", "content": list(current_assistant_message_content_parts)})
                                                current_assistant_message_content_parts = [] # Reset for next message
                                            
                                            return {"status": "pending_sql_execution", "sql_query": sql_query, "tool_use_id": tool_use_id, "assistant_response": ""}
                                        elif content_item.get("type") == "text" and "text" in content_item:
                                            assistant_response_text += content_item["text"]
                        except json.JSONDecodeError:
                            print(f"\nCortexAgent: Could not decode JSON from data: {json_str}")
            print("--- CortexAgent: End of Stream ---")
            # If we finished streaming and collected text or other content parts for the assistant
            if current_assistant_message_content_parts:
                 self.messages.append({"role": "assistant", "content": list(current_assistant_message_content_parts)})
            elif assistant_response_text: # If only text was collected without being part of a larger content structure
                 self.messages.append({"role": "assistant", "content": [{"type": "text", "text": assistant_response_text}]})

            return {"status": "complete", "assistant_response": assistant_response_text}

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return {"status": "error", "assistant_response": "", "error_message": str(e)}
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "assistant_response": "", "error_message": str(e)}

    def execute_sql_and_get_answer(self, sql_query_to_execute, tool_use_id_for_sql_exec):
        print(f"Executing SQL: {sql_query_to_execute}")
        query_id = None
        try:
            conn = self._get_snowflake_connection()
            cursor = conn.cursor()
            cursor.execute(sql_query_to_execute)
            query_id = cursor.sfqid
            print(f"SQL executed successfully. Query ID: {query_id}")
            # We don't fetch results here, agent uses query_id to formulate response
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error executing SQL: {e}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "assistant_response": "", "error_message": f"SQL execution failed: {e}"}

        if not query_id:
            return {"status": "error", "assistant_response": "", "error_message": "Failed to get Query ID from SQL execution."}

        # Construct the tool_results message for the user role
        tool_results_message = {
            "role": "user",
            "content": [
                {
                    "type": "tool_results",
                    "tool_results": {
                        "tool_use_id": tool_use_id_for_sql_exec,
                        "result": { # As per Snowflake documentation for providing query_id
                            "query_id": str(query_id) 
                        }
                        # Optionally, can add "status": "success" or actual data if small and needed by agent
                        # For now, sticking to query_id as per docs for agent to formulate answer.
                    }
                }
            ]
        }
        self.messages.append(tool_results_message)

        print("Sending SQL execution results (Query ID) back to Cortex Agent...")
        # Make the second POST request to the agent
        # This is similar to send_message but uses the updated self.messages
        # And the response should be the final textual answer

        # Generate a new JWT token for this request
        # The user, account, private_key_path, public_key_path are already attributes of self
        jwt_token_data = generate_jwt_token(
            snowflake_account=self.account,
            user_name=self.user,
            private_key_path=self.private_key_path,
            public_key_path=self.public_key_path,
            private_key_passphrase=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        )
        if not jwt_token_data or 'token' not in jwt_token_data:
            print("ERROR: Failed to generate JWT token for the second request.")
            return {"status": "error", "assistant_response": "", "error_message": "JWT generation failed for follow-up."}
        
        # Extract just the token string from the dictionary
        token_string = jwt_token_data['token']
        print(f"DEBUG: Second request - Token type: {type(token_string)}")
        
        # Directly set the headers with Bearer prefix only once
        headers = {
            'Authorization': f"Bearer {token_string}",
            'Content-Type': 'application/json',
            'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT'
        }

        # Payload for the second request - includes all messages including the new tool_results
        payload = {
            "model": "llama3.1-70b", # Or self.model if we make it an attribute
            "messages": self.messages,
            "tools": self.tools_payload, # Assuming self.tools_payload was stored from initial call setup
            "tool_resources": self.tool_resources_payload, # Assuming this was stored
            "response_instruction": "You will always maintain a friendly tone and provide concise response."
            # Add other relevant top-level payload keys if needed, like 'experimental'
        }

        try:
            print(f"CortexAgent: Preparing to send FOLLOW-UP POST request to {self.base_url}/agent:run with timeout={self.timeout}")
        
            # EMERGENCY FIX - Directly clean the Authorization header if it has a double Bearer
            if headers['Authorization'].startswith('Bearer Bearer '):
                print("DEBUG: Found double Bearer prefix in follow-up request, fixing it")
                # Remove the extra Bearer
                headers['Authorization'] = headers['Authorization'].replace('Bearer Bearer ', 'Bearer ')
            
            # Debug headers for follow-up request
            debug_headers = headers.copy()
            if 'Authorization' in debug_headers:
                auth_parts = debug_headers['Authorization'].split('Bearer ', 1)
                if len(auth_parts) > 1:
                    token_str = auth_parts[1]
                    debug_headers['Authorization'] = f"Bearer {token_str[:10]}...{token_str[-5:] if len(token_str) > 5 else token_str}"
            print("DEBUG_API_REQUEST: Follow-up Headers:")
            print(json.dumps(debug_headers, indent=2))
            
            response = requests.post(
                f"{self.base_url}/agent:run",
                headers=headers,
                json=payload,
                stream=True,
                timeout=self.timeout
            )
            print(f"CortexAgent: FOLLOW-UP POST request completed. Status: {response.status_code if response else 'No response object'}")
            response.raise_for_status()

            final_assistant_text = ""
            self.last_raw_response = []
            current_assistant_message_content_parts = []
            print("--- CortexAgent: Streaming Final Response --- ")
            # Initialize final_assistant_text before the loop
            final_assistant_text = ""
            chunk_data = {}
            
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith('data:'):
                        json_str = decoded_line[5:].strip()
                        try:
                            chunk_data = json.loads(json_str)
                            self.last_raw_response.append(chunk_data)
                            
                            if chunk_data.get('done', False):
                                print("CortexAgent: DONE signal received for final response.")
                                break
                            
                            # Extract the actual answer content if available
                            if 'assistant_response' in chunk_data:
                                print("\n--- Final Answer from Cortex Agent ---")
                                print(chunk_data['assistant_response'])
                                # Save this as our final answer
                                final_assistant_text = chunk_data['assistant_response']
                                
                        except json.JSONDecodeError:
                            print(f"Could not decode JSON from data (final response): {json_str}")
                
            print("--- CortexAgent: End of Final Stream ---")
            
            # Print the final answer
            print("\n--- Final Assistant Answer ---")
            if final_assistant_text:
                print(final_assistant_text)
            else:
                # Try to extract answer from the last chunk
                if chunk_data and 'assistant_response' in chunk_data:
                    print(chunk_data['assistant_response'])
                elif chunk_data and 'delta' in chunk_data and 'content' in chunk_data['delta']:
                    for content_item in chunk_data['delta'].get('content', []):
                        if content_item.get('type') == 'text':
                            print(content_item.get('text', ''))
                # If we still don't have an answer, try to parse the raw response
                elif self.last_raw_response:
                    print("Attempting to extract answer from raw response...")
                    for resp in reversed(self.last_raw_response):
                        if 'assistant_response' in resp:
                            print(resp['assistant_response'])
                            break
                        elif 'choices' in resp and resp['choices'] and 'message' in resp['choices'][0]:
                            print(resp['choices'][0]['message'].get('content', ''))
                            break
                else:
                    print("No final answer was extracted from the response.")
            
            # Print the conversation history for reference
            print("\nConversation History:")
            print(json.dumps(self.messages, indent=2))
            
            if current_assistant_message_content_parts:
                self.messages.append({"role": "assistant", "content": list(current_assistant_message_content_parts)})
            elif final_assistant_text and not any(msg.get('role') == 'assistant' and msg.get('content') == [{'type':'text', 'text':final_assistant_text}] for msg in self.messages):
                self.messages.append({"role": "assistant", "content": [{"type": "text", "text": final_assistant_text}]})

            return {"status": "complete", "assistant_response": final_assistant_text}

        except requests.exceptions.RequestException as e:
            print(f"Follow-up request failed: {e}")
            return {"status": "error", "assistant_response": "", "error_message": str(e)}
        except Exception as e:
            print(f"An unexpected error occurred during follow-up: {e}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "assistant_response": "", "error_message": str(e)}

    def _load_semantic_model(self):
        """
        Load the semantic model YAML file directly from disk
        """
        try:
            # Use the fixed semantic model file
            semantic_model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixed_semantic_model.yaml')
            
            print(f"Loading semantic model from: {semantic_model_path}")
            with open(semantic_model_path, 'r') as file:
                semantic_model_yaml = file.read()
            
            # Return the raw YAML content as a string
            return semantic_model_yaml
        except Exception as e:
            print(f"ERROR: Failed to load semantic model YAML file: {e}")
            return ""

    def get_conversation_history(self, conversation_id=None):
        """
        Get the history of a conversation
        """
        if not conversation_id and not self.conversation_id:
            print("No valid conversation ID. Cannot get history.")
            return None
        
        # Return the stored messages
        return self.messages

def test_cortex_agent():
    """
    Test the Cortex Agent API with two-step SQL execution flow.
    """
    agent = CortexAgent(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        private_key_path=os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH', 'rsa_key.p8'),
        public_key_path=os.getenv('SNOWFLAKE_PUBLIC_KEY_PATH', 'rsa_key.pub'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'SUPERSTOREDB'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'DATA')
        
        # Example usage
        message = "What is the top selling category?"
        print(f"Sending message: '{message}'")
        response = agent.send_message(message)
        print(f"SQL to execute: {sql_to_execute}")
        print(f"SQL Tool Use ID: {sql_tool_use_id}")
        
        final_response_data = agent.execute_sql_and_get_answer(sql_to_execute, sql_tool_use_id)
        
        if final_response_data.get("status") == "complete":
            print("\n--- Final Assistant Answer ---")
            print(final_response_data["assistant_response"])
        else:
            print("\n--- Error in obtaining final answer ---")
            print(json.dumps(final_response_data, indent=2))
            print("\n--- Full Parsed Stream Chunks (last_raw_response from last call) ---")
            print(json.dumps(agent.last_raw_response, indent=2))

    elif initial_call_response.get("status") == "complete":
        print("\n--- Direct Assistant Answer (no SQL execution step by agent) ---")
        print(initial_call_response["assistant_response"])
    else: # Error in initial call
        print("\n--- Error in initial call to Cortex Agent ---")
        print(json.dumps(initial_call_response, indent=2))
        print("\n--- Full Parsed Stream Chunks (last_raw_response from initial call) ---")
        print(json.dumps(agent.last_raw_response, indent=2))
    
    # Get conversation history
    history = agent.get_conversation_history()
    if history:
        print("\nConversation History:")
        print(json.dumps(history, indent=2))

if __name__ == "__main__":
    test_cortex_agent()
