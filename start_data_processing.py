from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import sys
import subprocess
import os

#################### VARIABLES: ####################
divider_symbol_count = 80 # length of a divider line when priting out output
divider_line = '#' * divider_symbol_count # symbol used as a divider line
# list of steps
steps = [
    'STEP 1: Initialising source database connection',
    'STEP 2: Reading the list of tables',
    'STEP 3: Reading data from the legacy_users table',
    'STEP 4: Data cleaning',
    'STEP 5: Initialising output database connection',
    'STEP 6: Uploading the Pandas DataFrame to a new database',
]
# initial step number
step_number = 0 

#################### FUNCTIONS: ####################
# function to print out the step number value 
def print_step_number(step_no):
    print(f'\n\n{divider_line}\n{steps[step_no]}\n{divider_line}')
    global step_number
    step_number += 1 # increase the step number

# function to clear the console
def clear_console():
    subprocess.run('cls' if os.name == 'nt' else 'clear', shell=True)
    
# function to create database engine
def create_db_engine(destination):
    try:
        db_engine = db_connector.init_db_engine(destination)
    except Exception as e:
        print(f'Error occured: {e}')
        sys.exit()

    print(f'\n--> Success. {destination} database connection established')
    return db_engine

# function to list database tables
def get_list_of_tables(db_engine):
    try:
        data_extractor = DataExtractor(db_engine)
        tables = data_extractor.list_db_tables()
        pdf_file = data_extractor.retrieve_pdf_data('CARD_DETAILS_DATA')
    except Exception as e:
        print(f'Error occured: {e}')
        sys.exit()
        
    return tables

# function to get data from a db table
def get_data_from_table(db_engine, table_name):
    try:
        data_extractor = DataExtractor(db_engine)
        data = data_extractor.read_rds_table(table_name)
    except Exception as e:
        print(f'Error occured: {e}')
        sys.exit()
    
    return data

# function to clean the data:
def get_data_cleaning(data):
    try:
        data_cleaning = DataCleaning(data)
        cleaned_data = data_cleaning.clean_user_data()
    except Exception as e:
        print(f'Error occured: {e}')
        sys.exit()

    return cleaned_data



#################### MAIN PROGRAM: ####################

####### STEP 0: INITIALISATION #######
# clear the console
clear_console()

# initialise DatabaseConnector class
db_connector = DatabaseConnector()

####### STEP 1 #######
print_step_number(step_number)
# create the source DB engine or throw an error
source_db_engine = create_db_engine('SOURCE')

####### STEP 2 #######
print_step_number(step_number)
# read list of tables from the source db or throw an error
source_tables = get_list_of_tables(source_db_engine)

####### STEP 3 #######
print_step_number(step_number)
source_data = get_data_from_table(source_db_engine, 'legacy_users')

####### STEP 4 #######
print_step_number(step_number)
output_data = get_data_cleaning(source_data)

####### STEP 5 #######
print_step_number(step_number)
# create the output DB engine or throw an error
output_db_engine = create_db_engine('OUTPUT')

####### STEP 6 #######
print_step_number(step_number)
# upload data to the new database
db_connector.upload_to_db(output_db_engine, output_data, 'dim_users')

# output_tables = get_list_of_tables(output_db_engine)