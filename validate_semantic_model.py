import os
import yaml
import json
import traceback
from dotenv import load_dotenv
from cortex_agent import CortexAgent

# Load environment variables 
load_dotenv(override=True)

def load_yaml(yaml_path):
    """Load local yaml file into string format"""
    with open(yaml_path, 'r') as f:
        yaml_str = f.read()
    return yaml_str

def validate_semantic_model(yaml_path):
    """
    Validate a semantic model YAML file with Snowflake Cortex Agent
    
    Args:
        yaml_path (str): Path to the semantic model YAML file
    """
    print(f"\n=== VALIDATING SEMANTIC MODEL: {yaml_path} ===")
    
    try:
        # Load the YAML content
        yaml_str = load_yaml(yaml_path)
        print(f"✅ Loaded YAML file: {len(yaml_str)} characters")
        
        # Parse YAML to verify syntax
        yaml_data = yaml.safe_load(yaml_str)
        print(f"✅ YAML syntax is valid")
        
        # Check required top-level fields
        required_fields = ['name', 'tables']
        for field in required_fields:
            if field not in yaml_data:
                print(f"❌ Missing required top-level field: {field}")
                return False
                
        print(f"✅ Required top-level fields present")
        
        # Initialize Cortex Agent
        print("Initializing Cortex Agent...")
        agent = CortexAgent()
        conversation_id = agent.start_conversation()
        print(f"✅ Started conversation with ID: {conversation_id}")
        
        # Use a simple validation question
        validation_question = "List all tables in this semantic model"
        print(f"\nSending validation question: '{validation_question}'")
        
        # Send the query to Cortex Agent
        # Note: The agent uses the semantic model from the Snowflake stage path
        # defined in the cortex_agent.py file
        response = agent.send_message(validation_question)
        
        # Check if we got a response
        if response and len(response.strip()) > 0:
            print(f"\n=== VALIDATION SUCCESSFUL ===")
            print(f"Cortex Agent responded with {len(response)} characters")
            print("First 200 characters of response:")
            print(response[:200] + "..." if len(response) > 200 else response)
            return True
        else:
            print(f"\n=== VALIDATION FAILED ===")
            print("No response received from Cortex Agent")
            return False
            
    except Exception as e:
        print(f"\n=== VALIDATION ERROR ===")
        print(f"Error: {str(e)}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Path to your semantic model YAML file
    semantic_model_path = "superstore_semantic_model.yaml"
    
    # Validate the semantic model
    is_valid = validate_semantic_model(semantic_model_path)
    
    if is_valid:
        print("\n✅ Semantic model validation PASSED!")
    else:
        print("\n❌ Semantic model validation FAILED!")
