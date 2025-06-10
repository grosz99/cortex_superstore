# Snowflake Cortex Agent POC

This project is a Proof of Concept (POC) for testing Snowflake Cortex Agents using a retail Superstore data model.

## Project Structure

- `snowflake_connection.py` - Utility to connect to Snowflake
- `setup_database.py` - Script to set up the required database schema and tables
- `cortex_agent.py` - Client for interacting with the Cortex Agents API
- `app.py` - Streamlit web application for interacting with the Cortex Agent
- `requirements.txt` - Python dependencies
- `.env.example` - Example environment variables (copy to `.env` and fill in your credentials)

## Setup Instructions

1. **Clone the repository**

2. **Create a virtual environment and install dependencies**
   ```
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure your Snowflake credentials**
   - Copy `.env.example` to `.env`
   - Fill in your Snowflake account details:
     ```
     SNOWFLAKE_ACCOUNT=your_account_locator
     SNOWFLAKE_USER=your_username
     SNOWFLAKE_PASSWORD=your_password
     SNOWFLAKE_WAREHOUSE=SuperstoreWarehouse
     SNOWFLAKE_DATABASE=SuperstoreDB
     SNOWFLAKE_SCHEMA=data
     CORTEX_API_KEY=your_api_key
     ```

4. **Test the Snowflake connection**
   ```
   python snowflake_connection.py
   ```

5. **Set up the database schema and tables**
   ```
   python setup_database.py
   ```

6. **Test the Cortex Agent API**
   ```
   python cortex_agent.py
   ```

7. **Run the Streamlit application**
   ```
   streamlit run app.py
   ```

## Features

- **Chat with Data**: Ask questions about your retail Superstore data in natural language
- **Query Builder**: Use pre-built queries or create your own
- **Data Explorer**: Browse and visualize the data in your Snowflake tables

## Requirements

- Snowflake account with access to:
  - Cortex Agents
  - Cortex Search
  - Cortex Analyst
- Python 3.8+

## Data Model

The POC uses a retail Superstore data model with the following tables:

- **Orders**: Main sales data including order details, customer info, and product sales metrics
- **Customers**: Customer relationship data including contact info, tier, and account manager
- **Products**: Product details including brand, warranty, material, and sustainability rating

## Troubleshooting

- **Connection Issues**: Ensure your Snowflake credentials are correct in the `.env` file
- **Missing Tables**: Run `setup_database.py` to create the required tables and load the Superstore data
- **API Key Errors**: Make sure your Cortex API key is correctly set in the `.env` file
