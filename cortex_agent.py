import os
import requests
import json
from dotenv import load_dotenv
from generate_jwt_final import generate_jwt_token # Added import

# Load environment variables, overriding any existing system variables
load_dotenv(override=True)

class CortexAgent:
    def __init__(self):
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        # self.api_key = os.getenv('CORTEX_API_KEY') # Removed: Token will be generated per request
        
        if not self.account:
            print("ERROR: SNOWFLAKE_ACCOUNT environment variable not set")
        
        # Removed api_key check from init as it's generated per request
        
        # Format the base URL according to Snowflake documentation
        # The format is: https://<account>.snowflakecomputing.com/api/v2/cortex/agent:run
        self.base_url = f"https://{self.account}.snowflakecomputing.com/api/v2/cortex" if self.account else ""
        # self.headers = {} # Removed: Headers will be set per request
        self.conversation_id = None
        self.messages = []
    
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

        api_key = generate_jwt_token()
        if not api_key:
            print("ERROR: CortexAgent - Failed to generate JWT token. Check generate_jwt_final.py and RSA keys.")
            return None

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
        
        try:
            # Prepare the request payload according to the documentation
            payload = {
                "model": "llama3.1-70b",  # Using one of the supported models
                "messages": self.messages,
                "tools": [
                    {
                        "tool_spec": {
                            "type": "cortex_analyst_text_to_sql",
                            "name": "data_model"
                        }
                    },
                    {
                        "tool_spec": {
                            "type": "sql_exec",
                            "name": "sql_exec"
                        }
                    },
                    {
                        "tool_spec": {
                            "type": "data_to_chart",
                            "name": "data_to_chart"
                        }
                    }
                ],
                "tool_resources": {
                    "data_model": {
                        "semantic_model_file": f"@{os.getenv('SNOWFLAKE_DATABASE', 'SuperstoreDB')}.{os.getenv('SNOWFLAKE_SCHEMA', 'data')}.SUPERSTORE_STAGE/superstore_semantic_model.yaml"
                    }
                }
            }
            
            # Send the request to the agent:run endpoint with streaming enabled
            response = requests.post(
                f"{self.base_url}/agent:run",
                headers=headers, 
                json=payload,
                stream=True,
                timeout=60  
            )
            
            # Print the full URL for debugging
            print(f"Request URL: {self.base_url}/agent:run")
            response.raise_for_status()
            
            # Process the streaming response from the Cortex Agent
            assistant_response = ""
            raw_response_chunks = []
            print("--- CortexAgent: Streaming Response --- ")
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith('data:'):
                        json_str = decoded_line[5:].strip()
                        print(f"CortexAgent RAW JSON STRING: {json_str}") # Log raw string
                        if json_str == "[DONE]":
                            print("CortexAgent: DONE signal received.")
                            break
                        try:
                            json_chunk = json.loads(json_str)
                            print(f"CortexAgent PARSED JSON CHUNK: {json.dumps(json_chunk)}") # Log parsed chunk
                            raw_response_chunks.append(json_chunk)
                            
                            # Attempt to extract text, checking different possible structures
                            text_to_add = ""
                            if json_chunk.get("type") == "text" and isinstance(json_chunk.get("text"), str):
                                text_to_add = json_chunk.get("text", "")
                            elif "delta" in json_chunk and "content" in json_chunk["delta"]:
                                for content_item in json_chunk["delta"]["content"]:
                                    if content_item.get("type") == "text" and "text" in content_item:
                                        text_to_add += content_item["text"]
                            
                            if text_to_add:
                                print(f"CortexAgent TEXT TO ADD: '{text_to_add}'")
                                assistant_response += text_to_add
                                
                        except json.JSONDecodeError:
                            print(f"\nCortexAgent: Could not decode JSON from data: {json_str}")
            print("--- CortexAgent: End of Stream ---")

            # Add the complete assistant response to conversation history
            if assistant_response:
                self.messages.append({
                    "role": "assistant",
                    "content": [
                        {
                            "type": "text",
                            "text": assistant_response
                        }
                    ]
                })
            
            return {"response": assistant_response, "raw_response": raw_response_chunks}
            
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error sending message: {e}")
            print(f"Status code: {e.response.status_code}")
            print(f"Response: {e.response.text}")
            return None
        except Exception as e:
            print(f"Error sending message: {e}")
            return None
    
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
    Test the Cortex Agent API
    """
    agent = CortexAgent()
    
    # Start a conversation
    conversation_id = agent.start_conversation()
    if not conversation_id:
        print("Failed to start conversation. Check your API key and account.")
        return
    
    # Send a test message
    test_message = "What are the top 3 clients by total claimed amount?"
    print(f"Sending message: '{test_message}'")
    
    response = agent.send_message(test_message)
    if response:
        print("Response received:")
        print(json.dumps(response, indent=2))
    else:
        print("Failed to get a response.")
    
    # Get conversation history
    history = agent.get_conversation_history()
    if history:
        print("\nConversation History:")
        print(json.dumps(history, indent=2))

if __name__ == "__main__":
    test_cortex_agent()
