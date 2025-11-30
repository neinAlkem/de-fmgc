import pytest
import psycopg2
from dbSetup import initDatabase, params

def test_initDatabase_success():
    try:
        initDatabase(params)
    except Exception as e:
        pytest.fail(f'Init database fail : {e}')
        
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    
    queryCheck = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'dev_raw'
                AND    table_name   = 'pos'
            );
        """
    
    cursor.execute(queryCheck)
    exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    assert exists is True, "Tabel 'dev_raw.pos' not found!"
