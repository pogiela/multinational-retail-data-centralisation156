# used to connect with and upload data to the database.
import yaml
from sqlalchemy import create_engine
import sys

class DatabaseConnector:
    def __init__(self):
        pass
    
    def __read_db_creds(self, destination):
        # destination is either SOURCE or OUTPUT 
        # read the db credentials or throw an error
        try:
            with open('.db_creds.yaml', 'r') as file:
                credentials = yaml.safe_load(file)
                return credentials[destination]
        except FileNotFoundError:
            print('Error: .db_creds.yaml not found.')
            return None
        except yaml.YAMLError as e:
            print(f'Error loading YAML: {e}')
            return None
        
    def init_db_engine(self, destination):
        credentials = self.__read_db_creds(destination)
        if credentials is None:
            # Handle the case where credentials are not loaded
            print('Error, credentials has not been initialised')
            return None
        
        # The credentials are loaded, so prepare the database connection details
        DATABASE_TYPE = credentials['DATABASE_TYPE']
        DBAPI = credentials['DBAPI']
        ENDPOINT = credentials['RDS_HOST']
        USER = credentials['RDS_USER']
        PASSWORD = credentials['RDS_PASSWORD']
        PORT = credentials['RDS_PORT']
        DATABASE = credentials['RDS_DATABASE']
        # create and return the db engine based on the connection details
        try:
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}").connect()
        except Exception as e:
            print(f'Error occured when creating engine: {e}')
            sys.exit()
        
        # engine created successfully
        print(f'\n--> Success. {destination} database connection established')
        return engine
        
    def upload_to_db(self, db_engine, data, table_name):
        try:
            data.to_sql(con=db_engine, name=table_name, index=False, if_exists='replace')
        except Exception as e:
            print(f'Error occured when uploading data to the DB: {e}')
            db_engine.close()
            sys.exit()
        
        print(f'\n--> Success. There were {data.shape[0]} rows uploaded to table: {table_name}.\n')
        
#     def list_db_tables(self, db_engine):
#         # engine = self.init_db_engine()
#         engine = db_engine.connect()
#         inspector = inspect(engine)
#         return inspector.get_table_names()
    


# db_connector = DatabaseConnector()
# db_engine = db_connector.init_db_engine('SOURCE')
# test = db_connector.list_db_tables(db_engine)
# print(test)





