'''
This is the main file to run the extraction, cleaning and uploading to a database process.

The whole process is divided into 21 steps. Current step and all actions within the step will 
be displayed on the screen when the programme is running providing insight into the progress.

'''
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from database_schema import DatabaseSchema
import os
import subprocess


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

def start_data_processing():
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
    cleaned_pdf_data = data_cleaning.clean_user_data(pdf_data, string_columns, date_columns, number_columns, integer_columns)
    # Remove question mark from the card number column
    output_pdf_data = data_cleaning.remove_question_mark_from_column(cleaned_pdf_data, 'card_number')
    
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


def start_database_schema_update():
    ####### STEP 22 #######
    # clear the console
    clear_console()
    print_step_number(step_number)
    
    ####### STEP 23 #######
    print_step_number(step_number)
    
    # initialise all classes
    db_connector = DatabaseConnector()
    print(f'\n--> DatabaseConnector class has been initiated.')
    data_extractor = DataExtractor()
    print(f'\n--> DataExtractor class has been initiated.')
    database_schema = DatabaseSchema()
    print(f'\n--> DatabaseSchema class has been initiated.')

    # create the output DB engine or throw an error
    output_db_engine = db_connector.init_db_engine('OUTPUT')

    ####### STEP 24 #######
    print_step_number(step_number)
    # read list of tables from the output db or throw an error
    output_tables = data_extractor.list_db_tables(output_db_engine)

    ####### STEP 25 #######
    print_step_number(step_number)
    table_name = 'orders_table'
    columns_and_types = {
        'date_uuid': 'uuid',
        'user_uuid': 'uuid',
        'card_number': 'varchar',
        'store_code': 'varchar',
        'product_code': 'varchar',
        'product_quantity': 'smallint', 
    }
    # update column types for the orders_table
    database_schema.alter_rds_table_column_types(output_db_engine, table_name, columns_and_types)


    ####### STEP 26 #######
    print_step_number(step_number)
    table_name = 'dim_users'
    columns_and_types = {
        'first_name': 'varchar(255)',
        'last_name': 'varchar(255)',
        'date_of_birth': 'date',
        'country_code': 'varchar',
        'user_uuid': 'uuid',
        'join_date': 'date',
    }
    # update column types for the orders_table
    database_schema.alter_rds_table_column_types(output_db_engine, table_name, columns_and_types)


    ####### STEP 27 #######
    print_step_number(step_number)
    table_name = 'dim_store_details'
    columns_and_types = {
        'longitude': 'float',
        'locality': 'varchar(255)',
        'store_code': 'varchar',
        'staff_numbers': 'smallint',
        'opening_date': 'date',
        'store_type': 'varchar(255)',
        'latitude': 'float',
        'country_code': 'varchar',
        'continent': 'varchar(255)',
    }
    # update column types for the orders_table
    database_schema.alter_rds_table_column_types(output_db_engine, table_name, columns_and_types)
    
    
    ####### STEP 28 #######
    print_step_number(step_number)
    table_name = 'dim_products'
    # create a new column for the weight class
    database_schema.create_category_column(output_db_engine, table_name, 'weight_class')
    # create a new column for the availability
    database_schema.create_availability_column(output_db_engine, table_name, 'still_available')
    columns_and_types = {
        'product_price': 'float',
        'weight': 'float',
        'EAN': 'varchar',
        'product_code': 'varchar',
        'date_added': 'date',
        'uuid': 'uuid',
        'still_available': 'boolean',
        'weight_class': 'varchar',
    }
    # update column types for the orders_table
    database_schema.alter_rds_table_column_types(output_db_engine, table_name, columns_and_types)
    
    
    ####### STEP 29 #######
    print_step_number(step_number)
    table_name = 'dim_date_times'
    columns_and_types = {
        'month': 'smallint',  # TODO: Milestone 3, task 6, asks to change the column type to 'varchar'. If needed then change the column type when created.
        'year': 'smallint', # TODO: Milestone 3, task 6, asks to change the column type to 'varchar'. If needed then change the column type when created.
        'day': 'smallint', # TODO: Milestone 3, task 6, asks to change the column type to 'varchar'. If needed then change the column type when created.
        'time_period': 'varchar',
        'date_uuid': 'uuid',
    }
    # update column types for the orders_table
    database_schema.alter_rds_table_column_types(output_db_engine, table_name, columns_and_types)
    
    
    ####### STEP 30 #######
    print_step_number(step_number)
    table_name = 'dim_card_details'
    columns_and_types = {
        'card_number': 'varchar',
        'expiry_date': 'varchar',
        'date_payment_confirmed': 'date',
    }
    # update column types for the orders_table
    database_schema.alter_rds_table_column_types(output_db_engine, table_name, columns_and_types)
    
    
    ####### STEP 31 #######
    print_step_number(step_number)
    tables_and_keys = {
        'dim_users': 'user_uuid',
        'dim_store_details': 'store_code',
        'dim_products': 'product_code',
        'dim_date_times': 'date_uuid',
        'dim_card_details': 'card_number',
    }
    # update tables to have primary keys
    database_schema.add_primary_keys(output_db_engine, tables_and_keys)
   
    
    ####### STEP 32 #######
    print_step_number(step_number)
    table_name = 'orders_table'
    foreign_keys = {
        'dim_users': 'user_uuid',
        'dim_store_details': 'store_code',
        'dim_products': 'product_code',
        'dim_date_times': 'date_uuid',
        'dim_card_details': 'card_number',
    }
    # remove question mark from the card details
    # database_schema.remove_question_mark_from_dim_card_details(output_db_engine)
    # update tables to have primary keys
    database_schema.add_foreign_keys(output_db_engine, table_name, foreign_keys)
    
    
    
    
if __name__ == '__main__':
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
        'SUCCESS: All data has been successfully extracted, cleaned and uploaded to the DB',
        '############################     DATABASE SCHEMA    ############################',
        'STEP 23: Initialisation',
        'STEP 24: Reading the list of tables from the Output DB',
        'STEP 25: Updating table column types: orders_table',
        'STEP 26: Updating table column types: dim_users',
        'STEP 27: Updating table column types: dim_store_details',
        'STEP 28: Updating table column types: dim_products',
        'STEP 29: Updating table column types: dim_date_times',
        'STEP 30: Updating table column types: dim_card_details',
        'STEP 31: Adding Primary Keys to the dimensio tables',
        'STEP 32: Adding Foreign Keys to the fact table',
        'STEP 33: ',
        'STEP 34: ',
        'STEP 35: ',
        'STEP 36: ',
        'STEP 37: ',
        'STEP 38: ',
    ]

    # initial step number
    # step_number = 0 

    #start_data_processing()
    step_number = 21
    start_database_schema_update()