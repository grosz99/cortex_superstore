import streamlit as st
import os
from dotenv import load_dotenv
from cortex_agent import CortexAgent

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Superstore Cortex Analytics", 
    page_icon="📊", 
    layout="wide"
)

# Set up the app
st.title("Superstore Analytics Assistant")
st.subheader("Ask questions about your Superstore data using natural language")

with st.sidebar:
    st.image("https://www.snowflake.com/wp-content/themes/snowflake/assets/img/logo-blue.svg", width=200)
    st.markdown("### About")
    st.markdown("""
    This app uses Snowflake's Cortex Agent to analyze Superstore data.
    Ask questions about sales, customers, products, and more.
    """)
    
    st.markdown("### Example questions")
    st.markdown("""
    - What were the total sales last year?
    - Who are our top 5 customers by revenue?
    - What products have the highest profit margin?
    - Show me monthly sales trends
    """)
    
    st.markdown("---")
    st.markdown("Powered by Snowflake Cortex Agent API")

# Initialize Cortex Agent (cache it to avoid reinitializing on every interaction)
@st.cache_resource
def get_agent():
    return CortexAgent()

agent = get_agent()

# Chat message history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# User input
prompt = st.chat_input("Ask a question about your Superstore data...")
if prompt:
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Display assistant response
    with st.chat_message("assistant"):
        with st.spinner("Analyzing data..."):
            try:
                response = agent.send_message(prompt)
                
                # Debug information
                st.expander("Debug Info").json({
                    "conversation_id": agent.conversation_id,
                    "message_count": len(agent.messages),
                    "raw_response_chunks": len(agent.last_raw_response) if hasattr(agent, 'last_raw_response') else 0
                })
                
                if not response or response.strip() == "":
                    st.error("No response received from Cortex Agent")
                    
                    # Show raw response for debugging
                    if hasattr(agent, 'last_raw_response') and agent.last_raw_response:
                        with st.expander("Raw Response"):
                            st.json(agent.last_raw_response)
                    
                    response = "I'm sorry, I couldn't process that request. Please try again or rephrase your question."
                
                st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})
                
            except Exception as e:
                st.error(f"Error: {str(e)}")
                
                # Show exception details
                with st.expander("Exception Details"):
                    import traceback
                    st.code(traceback.format_exc())
                
                response = "I'm sorry, an error occurred while processing your request."
                st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})

# Add a reset button
if st.button("Reset conversation"):
    st.session_state.messages = []
    agent = CortexAgent()  # Reinitialize agent
    st.experimental_rerun()
