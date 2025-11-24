import os
import pyodbc
import pytest
from dotenv import load_dotenv

# Load env vars
load_dotenv('.env.local')

def test_showplan_xml_retrieval():
    """
    Manual test to verify that SET SHOWPLAN_XML works correctly
    when executed in separate statements on the same connection.
    """
    conn_str = os.getenv('DB_CONN_LOCALDB')
    if not conn_str:
        pytest.skip("DB_CONN_LOCALDB not found")

    print(f"Connecting to: {conn_str}")
    conn = None
    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        
        print("1. Setting SHOWPLAN_XML ON...")
        cursor.execute("SET SHOWPLAN_XML ON")
        
        print("2. Executing query...")
        query = "Select * from Users Where FirstName ='Yossi'"
        print(f"Query: {query}")
        cursor.execute(query)
        
        print("3. Fetching plan...")
        assert cursor.description is not None, "No result set returned"
        
        row = cursor.fetchone()
        assert row is not None, "No rows returned"
        
        plan_xml = str(row[0])
        print(f"Plan XML found (first 100 chars): {plan_xml[:100]}")
        
        assert "ShowPlanXML" in plan_xml, "Result does not look like XML Showplan"
            
        print("4. Setting SHOWPLAN_XML OFF...")
        cursor.execute("SET SHOWPLAN_XML OFF")
                
    except Exception as e:
        pytest.fail(f"Test failed with error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    test_showplan_xml_retrieval()
