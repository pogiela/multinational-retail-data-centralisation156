'''
DatabaseQuery is a class and contains a method helping to return data from the database for the specified query.

Methods:
-------
query_database(engine, query)
    Executes a query on the database using the provided engine and prints out the results.
    
    Parameters:
    ----------
    engine: sqlalchemy.engine.Engine
        The SQLAlchemy engine object.
    query: string
        The SQL query to execute.
'''

from beautifultable import BeautifulTable
from sqlalchemy import text
import sys


class DatabaseQuery:
    def __init__(self):
        pass
     
    def query_database(self, engine, query):
        """
        Executes a query on the database using the provided engine and prints out the results.

        Parameters:
        ----------
            engine: sqlalchemy.engine.Engine
                The SQLAlchemy engine object.
            query: string 
                The SQL query to execute.
        """
        print("")
        try:
            result = engine.execute(text(query))
            # initiate the table
            table = BeautifulTable()
            # add the headers
            table.columns.header = result.keys()
            # add the rows
            for row in result:
                table.rows.append(row)
            
            print(table)
        except Exception as e:
            print(f'Error occurred when reading tables from the DB: {e}')
            engine.close()
            sys.exit()
        
        print("\n--> Query run successfully.\n")
