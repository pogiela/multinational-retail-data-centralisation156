'''
DatabaseSchema class is used to alter the schema of the database tables. It contains methods to alter the data types of the columns, create new columns, add primary and foreign keys, and remove question marks from the card_number column in the dim_card_details table.

Methods:
-------
alter_rds_table_column_types(engine, table_name, columns_and_types)
    Alter the data types of the columns in the table specified in the table_name parameter from the database specified in the engine parameter.

    Parameters:
    ----------
    engine: db_engine
        DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
    table_name: string
        Table name from which the data should be returned.
    columns_and_types: dict
        Dictionary with column names as keys and new data types as values.
        
create_category_column(engine, table_name, column_name)
    Creates a new column in a database table and populates it with category values based on the weight column.

    Parameters:
    ----------
    engine: db_engine
        DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
    table_name: string
        Table name from which the data should be returned.
    column_name: string
        Name of the new column to be created.
        
create_availability_column(engine, table_name, column_name)
    Creates a new column in the specified table to represent the availability status of products.

    Parameters:
    ----------
    engine: db_engine
        DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
    table_name: string
        Table name from which the data should be returned.
    column_name: string
        Name of the new column to be created.
        
add_primary_keys(engine, tables_and_keys)
    Adds primary keys to the specified tables in the database.

    Parameters:
    ----------
    engine: db_engine
        DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
    tables_and_keys: dict
        Dictionary containing table names as keys and corresponding primary key columns as values.
        
add_foreign_keys(engine, table_name, foreign_keys)
    Adds foreign key constraints to a table in the database.

    Parameters:
    ----------
    engine: db_engine
        DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
    table_name: string
        Table name from which the data should be returned.
    foreign_keys: dict
        Dictionary containing the foreign table names as keys and the corresponding foreign keys as values.
        
remove_question_mark_from_dim_card_details(engine)
    Removes question marks from the card_number column in the dim_card_details table.

    Parameters:
    ----------
    engine: db_engine
        DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
'''

from turtle import up
from sqlalchemy import inspect, text
import sys


class DatabaseSchema:
    def __init__(self):
        pass
    
     
    def alter_rds_table_column_types(self, engine, table_name, columns_and_types):
        '''
        alter_rds_table_column_types(engine, table_name, columns_and_types)
            Alter the data types of the columns in the table specified in the table_name parameter from the database specified in the engine parameter.
            
            Parameters:
            ----------
            engine: db_engine
                DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
            table_name: string
                Table name from which the data should be returned.
            columns_and_types: dict
                Dictionary with column names as keys and new data types as values.
        '''
        # Check the original column types using SQLAlchemy's Inspector
        try:
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
        except Exception as e:
            print(f'Error occured when reading tables from the DB: {e}')
            engine.close()
            sys.exit()
        
        print(f"\n############## Original column types in the '{table_name}' table: ##############\n") 
        for column in columns:
            print(f"Column '{column['name']}' has data type: {column['type']}")
        
        # Alter the column types
        try:
            # Iterate over the columns and execute ALTER TABLE statements
            for column_name, data_type in columns_and_types.items():
                if data_type == 'varchar':
                    characters = self.__max_characters_in_column(engine, table_name, column_name)
                    new_data_type = f'VARCHAR({characters})'
                elif data_type == 'uuid':
                    new_data_type = f'UUID USING {column_name}::UUID'
                else:
                    new_data_type = data_type
                    
                alter_query = text(f'ALTER TABLE {table_name} ALTER COLUMN "{column_name}" TYPE {new_data_type}')
                print('Executing query:', alter_query)
                engine.execute(alter_query)

            # Commit the transaction to make the changes persistent in the database
            engine.execute(text("COMMIT"))

            print(f"\n--> Columns in table '{table_name}' types changed successfully.\n") 
        except Exception as e:
            print(f"Error occurred: {e}")
            engine.close()
            sys.exit()
            
        # Check the updated column types using SQLAlchemy's Inspector
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        
        print(f"\n############## Updated column types in the '{table_name}' table: ##############\n") 
        for column in columns:
            print(f"Column '{column['name']}' has data type: {column['type']}")
            
            
    def __max_characters_in_column(self, engine, table_name, column_name):
        query = text(f'SELECT MAX(LENGTH("{column_name}")) FROM {table_name};')
        characters = engine.execute(query)

        return characters.fetchone()[0]

    
    def create_category_column(self, engine, table_name, column_name):
            """
            Creates a new column in a database table and populates it with category values based on the weight column.

            Args:
                engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object.
                table_name (str): The name of the table.
                column_name (str): The name of the column to be created.

            Raises:
                Exception: If an error occurs during the column creation or data population.

            Returns:
                None
            """
            try: 
                # Add column if doesn't exist
                engine.execute(text(
                    f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} VARCHAR(255) DEFAULT ''",
                ))
                engine.execute(text("COMMIT"))
            except Exception as e:
                print(f"Error occurred: {e}")
                engine.close()
                sys.exit()
                
            try:
                update_query = text(f"UPDATE {table_name} SET {column_name} = CASE WHEN weight < 2 THEN 'Light' WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized' WHEN weight >= 40 AND weight < 140 THEN 'Heavy' ELSE 'Truck_Required' END;")
                print('Executing query:', update_query)
                engine.execute(update_query)

                # Commit the transaction to make the changes persistent in the database
                engine.execute(text("COMMIT"))

                print(f"\n--> The {column_name} column in table '{table_name}' has been added successfully.\n") 
            except Exception as e:
                print(f"Error occurred: {e}")
                engine.close()
                sys.exit()
            
            
    def create_availability_column(self, engine, table_name, column_name):
        """
        Creates a new column in the specified table to represent the availability status of products.
        
        Args:
            engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object used to execute SQL statements.
            table_name (str): The name of the table where the column will be added.
            column_name (str): The name of the new column to be created.
        """
        try:
            # Add the column if it doesn't exist
            update_query = text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} BOOLEAN")
            print('Executing query:', update_query)
            engine.execute(update_query)
            engine.execute(text("COMMIT"))
            
            # Update the column with the availability status
            update_query = text("SELECT column_name FROM information_schema.columns WHERE table_name = 'dim_products' AND column_name = 'removed'")
            print('Executing query:', update_query)
            result = engine.execute(update_query)

            # If the column exists, update the new column with the availability status
            if result.fetchone():
                try:
                    update_query = text(f"UPDATE {table_name} SET {column_name} = CASE WHEN removed = 'Removed' THEN False ELSE True END;")
                    print('Executing query:', update_query)
                    engine.execute(update_query)
                    # Commit the transaction to make the changes persistent in the database
                    engine.execute(text("COMMIT"))
                except Exception as e:
                    print(f"Error occurred: {e}")
                    engine.close()
                    sys.exit()
            
        except Exception as e:
            print(f"Error occurred: {e}")
            engine.close()
            sys.exit()
            
        # Remove 'removed' column
        try:
            update_query = text(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS removed")
            print('Executing query:', update_query)
            engine.execute(update_query)
            engine.execute(text("COMMIT"))
        except Exception as e:
            print(f"Error occurred: {e}")
            engine.close()
            sys.exit()
            
        print(f"\n--> The {column_name} column in table '{table_name}' has been updated successfully.\n") 
        
        
    def add_primary_keys(self, engine, tables_and_keys):
        """
        Adds primary keys to the specified tables in the database.

        Args:
            engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object for connecting to the database.
            tables_and_keys (dict): A dictionary containing table names as keys and corresponding primary key columns as values.

        Returns:
            None
        """
        print("")
        # Add the keys to the tables
        try:
            # Iterate over the tables and execute ALTER TABLE statements
            for table_name, key_column in tables_and_keys.items():
                # Drop the existing primary key constraint if it exists
                drop_constraint_query = text(f'ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {table_name}_pkey CASCADE')
                engine.execute(drop_constraint_query)

                # Add the new primary key to the table
                add_primary_key_query = text(f'ALTER TABLE {table_name} ADD PRIMARY KEY ({key_column})')
                print('Executing query:', add_primary_key_query)
                engine.execute(add_primary_key_query)
                print(f"--> Column '{key_column}' has been changed to primary key in table '{table_name}'.") 
                
            # Commit the transaction to make the changes persistent in the database
            engine.execute(text("COMMIT"))
        except Exception as e:
            print(f"Error occurred: {e}")
            engine.close()
            sys.exit()
            
            
    def add_foreign_keys(self, engine, table_name, foreign_keys):
            """
            Adds foreign key constraints to a table in the database.

            Args:
                engine (sqlalchemy.engine.Engine): The database engine.
                table_name (str): The name of the table.
                foreign_keys (dict): A dictionary containing the foreign table names as keys and the corresponding foreign keys as values.

            Returns:
                None
            """
            print("")
            # Add the keys to the table
            try:
                # Iterate over the tables and execute ALTER TABLE statements
                for foreign_table, foreign_key in foreign_keys.items():
                    # Drop the existing primary key constraint if it exists
                    drop_constraint_query = text(f'ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {table_name}_{foreign_table}_{foreign_key}_fkey')
                    print('Executing query:', drop_constraint_query)
                    engine.execute(drop_constraint_query)

                    # Add the new primary key to the table
                    add_foreign_key_query = text(f'ALTER TABLE {table_name} ADD CONSTRAINT {table_name}_{foreign_table}_{foreign_key}_fkey FOREIGN KEY ({foreign_key}) REFERENCES {foreign_table}({foreign_key})')
                    print('Executing query:', add_foreign_key_query)
                    engine.execute(add_foreign_key_query)
                    print(f"--> Column '{foreign_key}' has been changed to foreign key in table '{table_name}' and links to table '{foreign_table}'s.") 
                    
                # Commit the transaction to make the changes persistent in the database
                engine.execute(text("COMMIT"))
            except Exception as e:
                print(f"Error occurred: {e}")
                engine.close()
                sys.exit()
            
            
    def remove_question_mark_from_dim_card_details(self, engine):
        """
        Removes question marks from the card_number column in the dim_card_details table.

        Args:
            engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object used to connect to the database.

        Raises:
            Exception: If an error occurs during the update operation.

        Returns:
            None
        """
        try:
            # Execute the UPDATE statement
            update_query = text("UPDATE dim_card_details SET card_number = REPLACE(card_number, '?', '') WHERE card_number LIKE '?%'")
            print('Executing query:', update_query)
            engine.execute(update_query)

            # Commit the transaction to make the changes persistent in the database
            engine.execute(text("COMMIT"))

            print("Question mark removed from card_number column.")
        except Exception as e:
            print(f"Error occurred: {e}")
            engine.close()
            sys.exit()