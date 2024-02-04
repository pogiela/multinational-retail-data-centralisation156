from database_utils import DatabaseConnector
from database_extraction import DataExtractor
from pdf_extraction import PdfDataExtractor
from data_cleaning import DataCleaning
from api_extraction import ApiDataExtractor
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
    'STEP 4: Cleaning data',
    'STEP 5: Initialising output database connection',
    'STEP 6: Uploading data to the database',
    'STEP 7: Retriving data from the PDF file',
    'STEP 8: Cleaning data',
    'STEP 9: Uploading data to the database',
    'STEP 10: Retriving data from API',
    'STEP 11: Cleaning data',
    'STEP 12: Uploading data to the database',
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
    


#################### MAIN PROGRAM: ####################

####### STEP 0: INITIALISATION #######
# clear the console
clear_console()

# initialise all classes
db_connector = DatabaseConnector()
db_extractor = DataExtractor()
pdf_data_extractor = PdfDataExtractor()
api_extractor = ApiDataExtractor()
data_cleaning = DataCleaning()

####### STEP 1 #######
print_step_number(step_number)
# create the source DB engine or throw an error
source_db_engine = db_connector.init_db_engine('SOURCE')

####### STEP 2 #######
print_step_number(step_number)
# read list of tables from the source db or throw an error
source_tables = db_extractor.list_db_tables(source_db_engine)

####### STEP 3 #######
print_step_number(step_number)
source_data = db_extractor.read_rds_table(source_db_engine, 'legacy_users')
# close connection as it's not longer needed
source_db_engine.close()

####### STEP 4 #######
print_step_number(step_number)
string_columns = ['first_name', 'last_name', 'company', 'email_address', 'address', 'country', 'country_code', 'phone_number', 'user_uuid']
date_columns = ['date_of_birth', 'join_date']
output_data = data_cleaning.clean_user_data(source_data, string_columns=string_columns, date_columns=date_columns, number_columns=[])

####### STEP 5 #######
print_step_number(step_number)
# create the output DB engine or throw an error
output_db_engine = db_connector.init_db_engine('OUTPUT')

####### STEP 6 #######
print_step_number(step_number)
# upload data to the new database
db_connector.upload_to_db(output_db_engine, output_data, 'dim_users')

####### STEP 7 #######
print_step_number(step_number)
# retrive data from PDF file
pdf_data = pdf_data_extractor.retrieve_pdf_data('CARD_DETAILS_DATA')

####### STEP 8 #######
print_step_number(step_number)
# clean data
string_columns=['card_number', 'expiry_date', 'card_provider']
date_columns = ['date_payment_confirmed']
output_pdf_data = data_cleaning.clean_user_data(pdf_data, string_columns=string_columns, date_columns=date_columns, number_columns=[])

####### STEP 9 #######
print_step_number(step_number)
# upload data to the new database
db_connector.upload_to_db(output_db_engine, output_pdf_data, 'dim_card_details')

####### STEP 10 #######
print_step_number(step_number)
# Retriving data from API',
api_data = api_extractor.retrive_stores_data()

####### STEP 11 #######
print_step_number(step_number)
# Cleaning data
string_columns=['address', 'locality', 'store_code', 'store_type', 'country_code',	'continent']
date_columns = ['opening_date']
number_columns = ['longitude', 'lat', 'staff_numbers', 'latitude']
int_columns = ['staff_numbers']
output_api_data = data_cleaning.clean_user_data(api_data, string_columns, date_columns, number_columns, int_columns)


####### STEP 12 #######
print_step_number(step_number)
# Uploading data to the database
db_connector.upload_to_db(output_db_engine, output_api_data, 'dim_store_details')


# close connection when all data uploaded
output_db_engine.close()


# output_data.to_excel('./pdf_data.xlsx')
