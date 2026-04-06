import pyodbc
import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path


import warnings
warnings.filterwarnings('ignore')

class DatabaseConfigReporting:
    def __init__(self, env_file=r'dbcreds_Reporting.env'):
        # Load environment variables from the specified .env file
        if env_file:
            load_dotenv(env_file, override=True)  # override ensures fresh reload
        else:
            load_dotenv(override=True)

        base_dir = Path(__file__).resolve().parent
        env_path = base_dir / (env_file or 'dbcreds_dba_Monitor.env')
        load_dotenv(env_path, override=True)

        # Initialize environment variables
        self.server = os.getenv('eSERVER')
        self.schema = os.getenv('eSCHEMA')
        self.database = os.getenv('eDATABASE')
        self.username = os.getenv('eUSERNAME')
        self.password = os.getenv('ePASSWORD')

    def get_config(self):
        """Return the configuration as a dictionary."""
        return {
            'server': self.server,
            'schema': self.schema,
            'database': self.database,
            'username': self.username,
            'password': self.password,
        }
    
def fConnectionString2_0(env_file=None, server=None, database=None):
    # If no env_file is passed, fall back to DatabaseConfigReporting default
    if env_file:
        db_config = DatabaseConfigReporting(f'{env_file}.env').get_config()
    else:
        db_config = DatabaseConfigReporting().get_config()
    
    # Use provided server/database if specified, otherwise fall back to .env values
    server = server or db_config['server']
    database = database or db_config['database']
    
    connectionString = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={db_config['username']};"
        f"PWD={db_config['password']}"
    )
    return connectionString

def SQL_Call_pandas(sql, env_file=None, server=None, database=None):
    cs = fConnectionString2_0(env_file=env_file, server=server, database=database)
    conn = None
    try:
        conn = pyodbc.connect(cs)
        df = pd.read_sql(sql, conn)
    finally:
        if conn is not None:
            conn.close()
    
    return df

def SQL_Call_pyodbc(sql, results=0, env_file=None, server=None, database=None):
    cs = fConnectionString2_0(env_file=env_file, server=server, database=database)
    
    sqloutput = None
    try:
        with pyodbc.connect(cs, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

                if results == 1:
                    # Look for the first non-empty result set
                    while True:
                        try:
                            records = cursor.fetchall()
                            if records:  # Found rows
                                sqloutput = [
                                    str(i).replace("('","").replace("',)","") for i in records
                                ]
                                break
                        except pyodbc.ProgrammingError:
                            # This result set didn't return rows
                            pass

                        # Move to the next result set if available
                        if not cursor.nextset():
                            break

    except Exception as e:
        print(f"SQL execution failed: {e}")
        raise

    if results == 1:
        return sqloutput

   
if __name__ == '__main__':
    1=1