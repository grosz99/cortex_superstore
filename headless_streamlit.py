import os
import sys
import json
import time
from dotenv import load_dotenv
from cortex_agent import CortexAgent

# Load environment variables
load_dotenv(override=True)

def headless_streamlit_test():
    """
    Run a headless Streamlit-like test of the Cortex Agent
    This simulates what would happen in a Streamlit app but without the UI
    """
    print("\n=== HEADLESS STREAMLIT TEST ===")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize the agent
    print("Initializing Cortex Agent...")
    agent = CortexAgent()
    
    # Start a conversation
    conversation_id = agent.start_conversation()
    print(f"✅ Started conversation with ID: {conversation_id}")
    
    # Test questions
    test_questions = [
        "What are the total sales by category?",
        "Who are the top 5 customers by total spending?",
        "What products have the highest profit margin?"
    ]
    
    # Process each question
    for i, question in enumerate(test_questions):
        print(f"\n=== TEST QUESTION {i+1}: '{question}' ===")
        
        # Send the question to the agent
        print("Sending question to Cortex Agent...")
        start_time = time.time()
        try:
            response = agent.send_message(question)
            end_time = time.time()
        except Exception as e:
            end_time = time.time()
            print(f"❌ Error sending message: {str(e)}")
            import traceback
            print("\n=== EXCEPTION TRACEBACK ===")
            print(traceback.format_exc())
            print("=== END TRACEBACK ===\n")
            response = None
        
        # Process the response
        if response and len(response.strip()) > 0:
            print(f"✅ Got response in {end_time - start_time:.2f} seconds")
            print("\n=== RESPONSE ===")
            print(response[:1000] + "..." if len(response) > 1000 else response)
            print("\n")
            
            # Check if we have raw response chunks for debugging
            if hasattr(agent, 'last_raw_response') and agent.last_raw_response:
                print(f"Raw response chunks: {len(agent.last_raw_response)}")
                
                # Extract any SQL that might be in the response
                sql_queries = []
                for chunk in agent.last_raw_response:
                    if 'content' in chunk and isinstance(chunk['content'], dict):
                        content = chunk['content']
                        if 'parts' in content and isinstance(content['parts'], list):
                            for part in content['parts']:
                                if isinstance(part, dict) and 'text' in part:
                                    text = part['text']
                                    if "```sql" in text.lower():
                                        sql_start = text.lower().find("```sql")
                                        sql_end = text.find("```", sql_start + 6)
                                        if sql_end > sql_start:
                                            sql = text[sql_start + 6:sql_end].strip()
                                            sql_queries.append(sql)
                
                if sql_queries:
                    print("\n=== SQL QUERIES GENERATED ===")
                    for i, sql in enumerate(sql_queries):
                        print(f"\nSQL Query {i+1}:")
                        print(sql)
        else:
            print(f"❌ No response received after {end_time - start_time:.2f} seconds")
            
            # Check if we have raw response chunks for debugging
            if hasattr(agent, 'last_raw_response') and agent.last_raw_response:
                print(f"Raw response chunks available: {len(agent.last_raw_response)}")
                print("First chunk sample:")
                print(json.dumps(agent.last_raw_response[0] if agent.last_raw_response else {}, indent=2))
    
    print("\n=== HEADLESS STREAMLIT TEST COMPLETE ===")

if __name__ == "__main__":
    headless_streamlit_test()
