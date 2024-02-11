'''
This is the main file to run the extraction, cleaning and uploading to a database process.

The whole process is divided into 21 steps. Current step and all actions within the step will 
be displayed on the screen when the programme is running providing insight into the progress.

'''

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import subprocess
import os

#################### VARIABLES: ####################
divider_symbol_count = 80 # length of a divider line when priting out output
divider_line = '#' * divider_symbol_count # symbol used as a divider line
# list of steps
steps = [
    'STEP 1: Initialisation',
    'STEP 2: Reading the list of tables from the Source DB',
    'STEP 3: Retriving Users data from the legacy_users source db table',
    'STEP 4: Cleaning Users data',
    'STEP 5: Uploading Users data to the Output database',
    'STEP 6: Retriving Orders data from orders_table Source db table',
    'STEP 7: Cleaning Orders data',
    'STEP 8: Uploading Orders data to the Output database',
    'STEP 9: Retriving Card Details data from the PDF file',
    'STEP 10: Cleaning Card Details data',
    'STEP 11: Uploading Card Details data to the Output database',
    'STEP 12: Retriving Stores data from API',
    'STEP 13: Cleaning Stores data',
    'STEP 14: Uploading Stores data to the Output database',
    'STEP 15: Retriving Products data from CSV file',
    'STEP 16: Cleaning Products data',
    'STEP 17: Uploading Products data to the Output database',
    'STEP 18: Retriving Date Events data from JSON file',
    'STEP 19: Cleaning Date Events data',
    'STEP 20: Uploading Date Events data to the Output database',
    'SUCCESS: All data has been successfully extracted, cleaned and uploaded to the DB'
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

####### STEP 1 #######
# clear the console
clear_console()
print_step_number(step_number)

# initialise all classes
db_connector = DatabaseConnector()
print(f'\n--> DatabaseConnector class has been initiated.')
data_extractor = DataExtractor()
print(f'\n--> DataExtractor class has been initiated.')
data_cleaning = DataCleaning()
print(f'\n--> DataCleaning class has been initiated.')

# create the source DB engine or throw an error
source_db_engine = db_connector.init_db_engine('SOURCE')
# create the output DB engine or throw an error
output_db_engine = db_connector.init_db_engine('OUTPUT')

####### STEP 2 #######
print_step_number(step_number)
# read list of tables from the source db or throw an error
source_tables = data_extractor.list_db_tables(source_db_engine)

####### STEP 3 #######
print_step_number(step_number)
# read data from the legacy users table
users_data = data_extractor.read_rds_table(source_db_engine, 'legacy_users')

####### STEP 4 #######
print_step_number(step_number)
# clean data
string_columns = ['first_name', 'last_name', 'company', 'email_address', 'address', 'country', 'country_code', 'phone_number', 'user_uuid']
date_columns = ['date_of_birth', 'join_date']
number_columns=[]
integer_columns=[]
output_users_data = data_cleaning.clean_user_data(users_data, string_columns, date_columns, number_columns, integer_columns)

####### STEP 5 #######
print_step_number(step_number)
# upload data to the new database
db_connector.upload_to_db(output_db_engine, output_users_data, 'dim_users')


####### STEP 6 #######
print_step_number(step_number)
orders_data = data_extractor.read_rds_table(source_db_engine, 'orders_table')

####### STEP 7 #######
print_step_number(step_number)
string_columns = ['date_uuid', 'user_uuid', 'card_number', 'store_code', 'product_code']
date_columns = []
number_columns = []
integer_columns = []
cleaned_orders_data = data_cleaning.clean_user_data(orders_data, string_columns, date_columns, number_columns, integer_columns)
# remove unwanted columns
columns_to_remove = ['first_name', 'last_name']
output_orders_data = data_cleaning.clean_orders_data(cleaned_orders_data, columns_to_remove)

####### STEP 8 #######
print_step_number(step_number)
# Uploading data to the database
db_connector.upload_to_db(output_db_engine, output_orders_data, 'orders_table')

####### CLEAN UP #######
# close source db connection as it's not longer needed
source_db_engine.close()


####### STEP 9 #######
print_step_number(step_number)
# retrive data from PDF file
pdf_data = data_extractor.extract_from_remote_location('CARD_DETAILS_DATA', 'pdf')

####### STEP 10 #######
print_step_number(step_number)
# clean data
string_columns=['card_number', 'expiry_date', 'card_provider']
date_columns = ['date_payment_confirmed']
number_columns = []
integer_columns = []
output_pdf_data = data_cleaning.clean_user_data(pdf_data, string_columns, date_columns, number_columns, integer_columns)

####### STEP 11 #######
print_step_number(step_number)
# upload data to the new database
db_connector.upload_to_db(output_db_engine, output_pdf_data, 'dim_card_details')


####### STEP 12 #######
print_step_number(step_number)
# Retriving data from API',
api_data = data_extractor.extract_from_remote_location(['x_api_key', 'retrive_store_api', 'number_of_stores_api'], 'api')

####### STEP 13 #######
print_step_number(step_number)
# Cleaning data
string_columns=['address', 'locality', 'store_code', 'store_type', 'country_code',	'continent']
date_columns = ['opening_date']
number_columns = ['longitude', 'lat', 'staff_numbers', 'latitude']
integer_columns = ['staff_numbers']
output_api_data = data_cleaning.clean_user_data(api_data, string_columns, date_columns, number_columns, integer_columns)

####### STEP 14 #######
print_step_number(step_number)
# Uploading data to the database
db_connector.upload_to_db(output_db_engine, output_api_data, 'dim_store_details')


####### STEP 15 #######
print_step_number(step_number)
# Retriving data from S3',
csv_data = data_extractor.extract_from_remote_location('PRODUCTS_DATA', 'csv')

####### STEP 16 #######
print_step_number(step_number)
# Converting data types
string_columns=['product_name', 'product_price', 'category', 'EAN', 'uuid', 'removed', 'product_code', 'weight']
date_columns = ['date_added']
number_columns = []
integer_columns = []
output_csv_data = data_cleaning.clean_user_data(csv_data, string_columns, date_columns, number_columns, integer_columns)
# Extracting product_price and weight
output_csv_data = data_cleaning.clean_products_data(output_csv_data)

####### STEP 17 #######
print_step_number(step_number)
# Uploading data to the database
db_connector.upload_to_db(output_db_engine, output_csv_data, 'dim_products')


####### STEP 18 #######
print_step_number(step_number)
# Retriving data from S3 / json file',
date_events_data = data_extractor.extract_from_remote_location('DATE_EVENTS_DATA', 'json')

####### STEP 19 #######
print_step_number(step_number)
string_columns = ['time_period', 'date_uuid']
date_columns = ['timestamp']
number_columns=['month', 'year', 'day']
integer_columns=['month', 'year', 'day']
json_data_cleaning = DataCleaning()
output_date_events_data = json_data_cleaning.clean_user_data(date_events_data, string_columns, date_columns, number_columns, integer_columns)

####### STEP 20 #######
print_step_number(step_number)
# Uploading data to the database
db_connector.upload_to_db(output_db_engine, output_date_events_data, 'dim_date_times')

####### CLEAN UP #######
# close connection when all data uploaded
output_db_engine.close()


####### STEP 21 #######
print_step_number(step_number)

# TODO: remove the temporary files

