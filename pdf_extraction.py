# This class will work as a utility class, in it you will be creating methods that help extract
# data from different data sources.
# The methods contained will be fit to extract data from a particular data source, 
# these sources will include CSV files, an API and an S3 bucket.

import pandas as pd
import yaml
import tabula
import fitz
import requests
from pathlib import Path
import sys


class PdfDataExtractor:
    def __init__(self):
        # initiate local variables
        self.temporary_folder = './temp_files'
        self.temporary_file = 'temporary.pdf'
     
        
    def retrieve_pdf_data(self, link):
        # get the link to the pdf file
        data_link = self.__read_data_link(link)
        print('\n############## Accessing the file: ##############') 
        print(f'\n--> Data link: {data_link}\n\n')
        
        if data_link:
            print('\n############## Checking the file: ##############') 
            # download the pdf file to a temporary folder
            downloaded_pdf_file = self.__download_pdf_file(data_link)

            # check the number of pages in the pdf file
            page_count = self.__get_page_count(downloaded_pdf_file)
            print(f"\n--> There are {page_count} pages in the PDF\n\n")
            print('\n############## Processing the file: ##############\n\n') 
            final_df = self.__process_pdf_with_progress(downloaded_pdf_file, page_count)
            
            # remove the temporary file and folder
            self.__remove_downloaded_file(downloaded_pdf_file)
            
            return final_df
    
    
    def __download_pdf_file(self, pdf_path):
        # Create a temporary folder if not exists
        try:
            Path(self.temporary_folder).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
            
        # download the pdf file to the temporary folder
        try:
            with requests.get(pdf_path, stream=True) as response:
                response.raise_for_status()  # Raise an exception for error status codes
                try:
                    with open(self.temporary_folder + '/' + self.temporary_file, 'wb') as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                except Exception as e:
                    print(f'Error occured: {e}')
                    sys.exit()
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
        
        return self.temporary_folder + '/' + self.temporary_file
        
        
    def __get_page_count(self, pdf_path):
        try:
            with fitz.open(pdf_path) as doc:
                page_count = doc.page_count
                return page_count
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
            
    
    def __process_pdf_with_progress(self, pdf_path, total_pages):
        dfs = [] # initiate a blank data frame list
        for page_number in range(1, total_pages + 1):
            try:
                self.__progress(page_number, total_pages)
                # Use tabula-py to extract tables from the current page
                df = tabula.read_pdf(pdf_path, pages=page_number)
                dfs.extend(df)
            except Exception as e:
                print(f'Error occured when processing page no. {page_number}: {e}')
                sys.exit()

        # concatenate all pages into a single data frame
        try:
            final_df = pd.concat(dfs, ignore_index=True)
        except Exception as e:
            print(f'Error occured when concatenating pages data: {e}')
            sys.exit()
            
        print("Processing complete.")
        # return the final data frame
        return final_df
    
    
    def __remove_downloaded_file(self, pdf_path):
        # remove the temporary file and folder
        try:
            Path.unlink(pdf_path)
            Path.rmdir(self.temporary_folder)
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
        
        
    def __progress(self, count, total):
        bar_len = 60
        filled_len = int(round(bar_len * count / float(total)))

        percents = round(100.0 * count / float(total), 1)
        bar = '#' * filled_len + '-' * (bar_len - filled_len)

        # sys.stdout.write('[%s] %s%s  [%s / %s] ...%s\r' % (bar, percents, '%', count, total, suffix))
        sys.stdout.write(f'[{bar}] {percents}%  [{count} / {total}]\r')
        sys.stdout.flush()
    
    
    def __read_data_link(self, link_name):
        # read the db credentials or throw an error
        try:
            with open('.s3_data.yaml', 'r') as file:
                links = yaml.safe_load(file)
                return links[link_name]
        except FileNotFoundError:
            print('Error: .s3_data.yaml not found.')
            return None
        except yaml.YAMLError as e:
            print(f'Error loading YAML: {e}')
            return None
        
        
        
    