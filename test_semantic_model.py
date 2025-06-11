import os
import json
import time
from dotenv import load_dotenv
from cortex_agent import CortexAgent

# Load environment variables
load_dotenv(override=True)

def test_semantic_model():
    print("=== TESTING SEMANTIC MODEL WITH CORTEX AGENT ===")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create Cortex Agent instance
    agent = CortexAgent()
    
    # Start a conversation
    conversation_id = agent.start_conversation()
    if not conversation_id:
        print("❌ Failed to start conversation")
        return False
    
    print(f"✅ Started conversation with ID: {conversation_id}")
    
    # Test questions of increasing complexity - general analytical questions
    test_questions = [
        "What are the total sales by category?",
        "Who are the top 5 customers by total spending?",
        "What products have the highest profit margin?"
    ]
    
    success = False
    for i, question in enumerate(test_questions, 1):
        print(f"\n=== TEST QUESTION {i}: '{question}' ===")
        print("Sending message to Cortex Agent...")
        
        # Start timer
        start_time = time.time()
        
        # Send message and get response
        response = agent.send_message(question)
        
        # End timer
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"Request completed in {elapsed_time:.2f} seconds")
        
        if response:
            print(f"✅ Received response from Cortex Agent:")
            
            # Attempt to pretty-print if it's JSON
            try:
                # Check if response is a string that contains JSON
                if isinstance(response, str) and (response.startswith('{') or response.startswith('[')):
                    json_obj = json.loads(response)
                    print(f"--- JSON RESPONSE START ---\n{json.dumps(json_obj, indent=2)}\n--- RESPONSE END ---")
                else:
                    print(f"--- TEXT RESPONSE START ---\n{response}\n--- RESPONSE END ---")
            except json.JSONDecodeError:
                # If it's not valid JSON, print as is
                print(f"--- TEXT RESPONSE START ---\n{response}\n--- RESPONSE END ---")
            
            # Get raw JSON chunks from agent for debugging
            if hasattr(agent, 'last_raw_response') and agent.last_raw_response:
                print("\n=== DEBUG: RAW RESPONSE CHUNKS ===")
                for chunk in agent.last_raw_response[:3]:  # Show first 3 chunks only to avoid overwhelming output
                    print(f"CHUNK: {json.dumps(chunk, indent=2)}")
                if len(agent.last_raw_response) > 3:
                    print(f"... and {len(agent.last_raw_response) - 3} more chunks")
            
            success = True
            # Don't break, test all questions
        else:
            print(f"❌ Failed to get response for question: '{question}'")
    
    if success:
        print("\n✅ SEMANTIC MODEL TEST SUCCESSFUL: At least one question produced a valid response")
        return True
    else:
        print("\n❌ SEMANTIC MODEL TEST FAILED: No questions produced valid responses")
        return False

if __name__ == "__main__":
    result = test_semantic_model()
    
    if result:
        print("\nYour semantic model is working correctly with the Cortex Agent!")
        print("You can now continue with other integrations or improvements.")
    else:
        print("\nYour semantic model is still not working correctly.")
        print("Please check the error messages and fix any remaining issues.")
