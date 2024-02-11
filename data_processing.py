'''
DataProcessing class is a parent class for DataExtractor class and contains methods helping to extract data from the specified data sources, 
show progress of extraction and prepares data for later conversion to Pandas DataFrames.

Methods:
-------
download_file(remote_file_path, data_type)
    Downloads a data file to a temporary local copy of the file which then can be used to extract data from.
    
    Parameters:
    ----------
    remote_file_path: string
        URL to the remote file location which need to be downloaded.
    data_type: string
        This parameter will be used as the local file extension e.g. pdf, csv, json.
        
process_with_progress(source_url, total_items, source_type, headers={}):
    Extracting data and showing progress for API or multipage PDF files.
    
    Parameters:
    ----------
    source_url: string
        A path to the data source file where from the data should be extracted or an API URL.
    total_items: number
        A number of total pages in PDF or items to retrieve from API. This is used to show the progress.
    source_type: string
        Allowed options are api or pdf. Based on this parameter a various methods will be used to extract the data.
    headers: object {}
        A headers object which is required for API connection connection containing API KEY and Content-Type. This is not required for processing a PDF file.
        
retrive_data_from_api(api_url, headers)
    Returns json data from the API
    
    Parameters:
    ----------
    api_url: string
        An API URL where from the data should be extracted.
    headers: object {}
        A headers object which is required for API connection containing API KEY and Content-Type.
'''

import requests
from pathlib import Path
import sys
import boto3
import tabula

######### VARIABLES ######### 
# Temporary folder name
temporary_folder_name = 'temp_files'
# Temporary file name (without extension)
temporary_file_name = 'temporary'


######### CLASS #########       
class DataProcessing():
    def __initi__(self):
        pass
    
    
    def download_file(self, remote_file_path, data_type):
        '''
        download_file(remote_file_path, data_type)
            Downloads a data file to a temporary local copy of the file which then can be used to extract data from.
            
            Parameters:
            ----------
            remote_file_path: string
                URL to the remote file location which need to be downloaded.
            data_type: string
                This parameter will be used as the local file extension e.g. pdf, csv, json.
        '''
        # Create temporary file path with extension
        temporary_folder = self.__create_folder()
        temporary_file_path = temporary_folder + temporary_file_name + '.' + data_type
        
        # Downlaod the file
        source = remote_file_path.split(':')
        if source[0] == 'https':
            downloaded_file = self.__download_file_from_https(remote_file_path, temporary_file_path)
        elif source[0] == 's3':
            downloaded_file = self.__download_file_from_s3(remote_file_path, temporary_file_path)
        else:
            print(f'\n--> Error, the remote file path has incorrect format.\n\n')
            sys.exit()
            
        return downloaded_file


    def __create_folder(self):
        # Create a temporary folder if not exists
        try:
            Path('./' + temporary_folder_name).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
        
        return './' + temporary_folder_name + '/'
    
    
    def __download_file_from_https(self, remote_file_path, local_file_path):
        # download the file to the temporary folder
        try:
            with requests.get(remote_file_path, stream=True) as response:
                response.raise_for_status()  # Raise an exception for error status codes
                try:
                    with open(local_file_path, 'wb') as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                except Exception as e:
                    print(f'Error occured: {e}')
                    sys.exit()
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
        
        return local_file_path
    
    
    def __download_file_from_s3(self, remote_file_path, local_file_path):
        ## bucket address and file path
        s = remote_file_path
        bucket = s[s.find('s3://')+len('s3://'):s.rfind('/')]
        file_path = s[s.rfind('/')+1:]
        
        # download the file to the temporary folder
        try:
            s3 = boto3.client('s3')
            s3.download_file(bucket, file_path, local_file_path)
            s3.close()
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
        
        return local_file_path
    
    
    def process_with_progress(self, source_url, total_items, source_type, headers={}):
        '''
        process_with_progress(source_url, total_items, source_type, headers={}):
            Extracting data and showing progress for API or multipage PDF files.
            
            Parameters:
            ----------
            source_url: string
                A path to the data source file where from the data should be extracted or an API URL.
            total_items: number
                A number of total pages in PDF or items to retrieve from API. This is used to show the progress.
            source_type: string
                Allowed options are api or pdf. Based on this parameter a various methods will be used to extract the data.
            headers: object {}
                A headers object which is required for API connection connection containing API KEY and Content-Type. This is not required for processing a PDF file.
        '''
        # source = ['api', 'pdf']
        
        dfs = [] # initiate a blank data frame list
        for item_number in range(1, total_items + 1):
            try:
                self.__progress(item_number, total_items)
                if source_type == 'pdf':
                    # Use tabula-py to extract tables from the current page
                    df = tabula.read_pdf(source_url, pages=item_number)
                    dfs.extend(df)
                elif source_type == 'api':
                    # url = f"{source_url}{item_number}" 
                    df = self.retrive_data_from_api(f"{source_url}{item_number - 1}", headers)
                    dfs.extend([df])
                else:
                    print(f'\n--> Error, the source type has incorrect format.\n\n')
                    sys.exit()
                
            except Exception as e:
                print(f'Error occured when processing page no. {item_number}: {e}')
                sys.exit()
        print('\n')
        return dfs
    
    
    def __progress(self, count, total):
        '''
        Displays a progress bar with % completed and the number 
        of processed items and total number of items to process
        
        Parameters:
        -----------
        count: number
            Number of current item to process
        total: number
            Number of total items to process
        '''
        bar_len = 60
        filled_len = int(round(bar_len * count / float(total)))

        percents = round(100.0 * count / float(total), 1)
        bar = '#' * filled_len + '-' * (bar_len - filled_len)

        sys.stdout.write(f'[{bar}] {percents}%  [{count} / {total}]\r')
        sys.stdout.flush()
        
        
    def retrive_data_from_api(self, api_url, headers):
        '''
        retrive_data_from_api(api_url, headers)
            Returns json data from the API
            
            Parameters:
            ----------
            api_url: string
                An API URL where from the data should be extracted.
            headers: object {}
                A headers object which is required for API connection containing API KEY and Content-Type.
        '''
        # create a get request to the api endpoint
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            # Access the response data as JSON
            data = response.json()
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
            sys.exit()
            
        return data