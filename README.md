# Multinational Retail Data Centralisation (MRDC)
Multinational Retail Data Centralisation (MRDC) Project extracts data from various data sources, including AWS RDS database, S3 buckets, API endpoints and various data files, e.g. PDF, JSON and CSV.

The project contains a number of classes helping to extract the data, clean up and upload to a new database.


## Built With

[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

Version:
- Python 3.12.0

## Installation instructions:

1. Clone the repo
    ```
    git clone https://github.com/pogiela/multinational-retail-data-centralisation156.git
    ```

## Usage instructions:

1. Enter *multinational-retail-data-centralisation156* folder:
    ```
    cd ./multinational-retail-data-centralisation156/
    ```

2. Install all dependencies listed below. If using Conda environment use the *conda* commands (where available) or use *pip* commands if not using a dedicated conda environment.
    a. yaml: conda install pyyaml | pip3 install pyyaml
    b. sqlalchemy: conda install sqlalchemy | install sqlalchemy
    c. pandas: conda install pandas | pip3 install pandas
    d. tabula: conda install -c conda-forge tabula-py | pip3 install tabula-py
    e. fitz: pip3 install PyMuPDF
    f. requests: conda install requests | pip3 install requests
    g. pathlib: conda install pathlib | pip3 install pathlib
    h. psycopg2: conda install psycopg2 | pip3 install psycopg2
    i. dotenv: conda install python-dotenv | pip3 install python-dotenv
    j. boto3: conda install boto3 | pip3 install boto3

3. Add environmental variables and database credential files (see sections Environmental Variables and Database Connection Details).

4. To run the programme enter command:
    ```
    python3 ./start_data_processing.py 
    ```

## File structure of the project:
```
.
├── multinational-retail-data-centralisation156     # Project files
│   ├── .env                                        # FILE NOT INCLUDED IN REPO: Environmental variables - see section Environmental Variables.
│   ├── .db_creds.yaml                              # FILE NOT INCLUDED IN REPO: Database connection details - see section Database Connection Details
│   ├── database_utils.py                           # DatabaseConnector class and methods helping to connect to and upload data to a database
│   ├── data_processing.py                          # DataProcessing class and methods helping to extract data from various data sources. Parent class to DataExtractor.
│   ├── data_extraction.py                          # DataExtractor class and methods helping to extract data from various data sources
│   ├── data_cleaning.py                            # DataCleaning class and methods helping to clean the data before uploading to the database.
│   └── start_data_processing.py                    # Main programme. Run this file to start the process.
└── README.md                                       # This file
```

## Environmental Variables
The programme to run properly requires .env file to be created with the following fields. This file is not included in the repository and need to be created by the user.

*CARD_DETAILS_DATA* - url to a PDF file containing Card Details Data.
*DATE_EVENTS_DATA* - url to a JSON file containing Date Events Data.
*PRODUCTS_DATA* url to a CSV file containing Products Data.
*x_api_key* - API connection key
*retrive_store_api* - API URL to get Stores Details Data
*number_of_stores_api* - API URL to get Number of Stores

## Database Connection Details
The programme to run properly requires .db_creds.yaml file to be created with the following fields. This file is not included in the repository and need to be created by the user. The file needs to have two sections - one for Source database and another one for Output database. The file need to be written in the below format:

*SOURCE:*
    *RDS_HOST:* - RDS database host url
    *RDS_PASSWORD:* Password to the database
    *RDS_USER:* User to the database
    *RDS_DATABASE:* Database name
    *RDS_PORT:* Port number (default is *5432*)
    *DATABASE_TYPE:* Type of the database (defaul is *postgresql*)
    *DBAPI:* Database API type (default is *psycopg2*)

*OUTPUT:*
    *RDS_HOST:* - RDS database host url
    *RDS_PASSWORD:* Password to the database
    *RDS_USER:* User to the database
    *RDS_DATABASE:* Database name
    *RDS_PORT:* Port number (default is *5432*)
    *DATABASE_TYPE:* Type of the database (defaul is *postgresql*)
    *DBAPI:* Database API type (default is *psycopg2*)


## License information:
Distributed under the MIT License. 

## Contact
Piotr Ogiela - piotrogiela@gmail.com

Project Link: https://github.com/pogiela/multinational-retail-data-centralisation156.git

