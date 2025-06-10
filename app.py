import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
from snowflake_connection import get_snowflake_connection
from cortex_agent import CortexAgent

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Snowflake Cortex Agent POC",
    page_icon="❄️",
    layout="wide"
)

# Initialize session state variables
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = None
if "messages" not in st.session_state:
    st.session_state.messages = []
if "agent" not in st.session_state:
    st.session_state.agent = CortexAgent()

# Title and description
st.title("❄️ Snowflake Cortex Agent POC")
st.markdown("""
This application demonstrates the capabilities of Snowflake Cortex Agents for retail data analysis.
You can query the Superstore database using natural language and get AI-powered responses.
""")

# Check if Cortex API Key is set
cortex_api_key = os.getenv('CORTEX_API_KEY')
if not cortex_api_key:
    st.warning("""
    ⚠️ **Cortex API Key Not Configured**
    
    The Cortex Agent functionality requires a valid API key. To use the natural language query features:
    
    1. Set up key-pair authentication in Snowflake
    2. Generate a JWT token
    3. Add the token to your `.env` file as `CORTEX_API_KEY`
    
    For more information, see the [Snowflake Cortex Agents documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents.html).
    
    You can still use the Data Explorer tab to browse your Superstore data directly.
    """)

# Sidebar for connection status and database info
with st.sidebar:
    st.header("Connection Status")
    
    # Check Snowflake connection
    if st.button("Test Snowflake Connection"):
        conn = get_snowflake_connection()
        if conn:
            st.success("✅ Connected to Snowflake successfully!")
            
            # Display database info
            cursor = conn.cursor()
            cursor.execute("SELECT current_database(), current_schema(), current_warehouse()")
            db, schema, warehouse = cursor.fetchone()
            
            st.markdown(f"""
            **Current Connection:**
            - Database: {db}
            - Schema: {schema}
            - Warehouse: {warehouse}
            """)
            
            # Check if tables exist
            cursor.execute("SHOW TABLES IN SuperstoreDB.data")
            tables = cursor.fetchall()
            if tables:
                st.markdown("**Available Tables:**")
                for table in tables:
                    st.markdown(f"- {table[1]}")
            
            cursor.close()
            conn.close()
        else:
            st.error("❌ Failed to connect to Snowflake. Check your credentials.")
    
    # Test Cortex Agent API connection
    cortex_api_status = "❌ Not Connected"
    cortex_api_message = "Cortex Agent API connection failed."

    if st.session_state.cortex_agent.api_key:
        try:
            conversation_id = st.session_state.cortex_agent.start_conversation()
            if conversation_id:
                cortex_api_status = "✅ Connected"
                cortex_api_message = f"Successfully connected to Cortex Agent API. Conversation ID: {conversation_id}"
            else:
                cortex_api_message = "Failed to establish a conversation with the Cortex Agent."
        except Exception as e:
            cortex_api_message = f"Error connecting to Cortex Agent API: {str(e)}"

    # Check if Cortex Agents feature is available
    if cortex_api_status == "❌ Not Connected":
        st.sidebar.warning("⚠️ Cortex Agents API Not Available")
        st.sidebar.info("""
        The Cortex Agents API appears to be unavailable in your Snowflake account. This could be because:
        1. Cortex Agents is not enabled in your account
        2. Your account region doesn't support Cortex Agents yet
        3. Your user doesn't have the necessary permissions
        
        Please contact Snowflake Support to enable this feature.
        """)
    else:
        if st.button("Test Cortex Agent API"):
            if not st.session_state.conversation_id:
                conversation_id = st.session_state.agent.start_conversation()
                if conversation_id:
                    st.session_state.conversation_id = conversation_id
                    st.success(f"✅ Connected to Cortex Agent API! Conversation ID: {conversation_id}")
                else:
                    st.error("❌ Failed to connect to Cortex Agent API. Check your API key.")
            else:
                st.info(f"Already connected. Conversation ID: {st.session_state.conversation_id}")

# Main content area
tab1, tab2, tab3 = st.tabs(["Chat with Data", "Query Builder", "Data Explorer"])

# Tab 1: Chat with Data
with tab1:
    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask a question about the Superstore data..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Display user message
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Get response from Cortex Agent
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                if not st.session_state.conversation_id:
                    st.session_state.conversation_id = st.session_state.agent.start_conversation()
                
                if st.session_state.conversation_id:
                    response = st.session_state.agent.send_message(prompt, st.session_state.conversation_id)
                    if response:
                        response_text = response.get("response", "No response received.")
                        st.markdown(response_text)
                        
                        # Add assistant message to chat history
                        st.session_state.messages.append({"role": "assistant", "content": response_text})
                        
                        # Display SQL if available
                        if "sql" in response:
                            with st.expander("View SQL Query"):
                                st.code(response["sql"], language="sql")
                    else:
                        st.error("Failed to get a response from the Cortex Agent.")
                else:
                    st.error("Failed to establish a conversation with the Cortex Agent.")

# Tab 2: Query Builder
with tab2:
    st.header("Natural Language Query Builder")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        query_type = st.selectbox(
            "Select query type:",
            ["Sales Analysis", "Customer Information", "Product Details"]
        )
        
        if query_type == "Sales Analysis":
            example_queries = [
                "Show me the top 5 products by sales",
                "What is the total profit by region?",
                "Which category has the highest discount rate?"
            ]
        elif query_type == "Customer Information":
            example_queries = [
                "List all Gold tier customers",
                "Which customer has spent the most?",
                "Show me customers managed by Jason Gonzalez"
            ]
        else:
            example_queries = [
                "Which products have an 'A' sustainability rating?",
                "List all products made of Glass",
                "What's the average warranty period for Office Supplies?"
            ]
        
        selected_query = st.selectbox("Choose an example query:", example_queries)
        custom_query = st.text_area("Or write your own query:", value=selected_query, height=100)
        
        if st.button("Run Query"):
            with st.spinner("Processing query..."):
                if not st.session_state.conversation_id:
                    st.session_state.conversation_id = st.session_state.agent.start_conversation()
                
                if st.session_state.conversation_id:
                    response = st.session_state.agent.send_message(custom_query, st.session_state.conversation_id)
                    if response:
                        # Add to chat history
                        st.session_state.messages.append({"role": "user", "content": custom_query})
                        st.session_state.messages.append({"role": "assistant", "content": response.get("response", "No response received.")})
                    else:
                        st.error("Failed to get a response from the Cortex Agent.")
                else:
                    st.error("Failed to establish a conversation with the Cortex Agent.")
    
    with col2:
        st.subheader("Query Results")
        if st.session_state.messages and len(st.session_state.messages) >= 2:
            st.markdown(st.session_state.messages[-1]["content"])
            
            # Display SQL if available in the last response
            if len(st.session_state.messages) >= 2 and "sql" in st.session_state.messages[-1]:
                with st.expander("View SQL Query"):
                    st.code(st.session_state.messages[-1]["sql"], language="sql")

# Tab 3: Data Explorer
with tab3:
    st.header("Data Explorer")
    
    table_choice = st.selectbox(
        "Select a table to explore:",
        ["Orders", "Customers", "Products"]
    )
    
    if st.button("Load Data"):
        with st.spinner(f"Loading {table_choice} data..."):
            conn = get_snowflake_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT * FROM SuperstoreDB.data.{table_choice} LIMIT 100")
                    
                    # Get column names
                    column_names = [desc[0] for desc in cursor.description]
                    
                    # Fetch data
                    data = cursor.fetchall()
                    
                    # Create DataFrame
                    df = pd.DataFrame(data, columns=column_names)
                    
                    # Display data
                    st.dataframe(df, use_container_width=True)
                    
                    # Show basic statistics
                    st.subheader("Basic Statistics")
                    
                    # For numerical columns
                    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
                    if not numeric_cols.empty:
                        st.write(df[numeric_cols].describe())
                    
                    # Count by categorical columns
                    if table_choice == "Customers":
                        st.subheader("Customers by State")
                        state_counts = df["STATE"].value_counts().reset_index()
                        state_counts.columns = ["State", "Count"]
                        st.bar_chart(state_counts.set_index("State"))
                    elif table_choice == "Claims":
                        st.subheader("Claims by Type")
                        claim_type_counts = df["CLAIM_TYPE"].value_counts().reset_index()
                        claim_type_counts.columns = ["Claim Type", "Count"]
                        st.bar_chart(claim_type_counts.set_index("Claim Type"))
                    
                    cursor.close()
                    conn.close()
                except Exception as e:
                    st.error(f"Error loading data: {e}")
                    if conn:
                        conn.close()
            else:
                st.error("Failed to connect to Snowflake.")

# Footer
st.markdown("---")
st.markdown("Snowflake Cortex Agent POC | Created with Streamlit")
