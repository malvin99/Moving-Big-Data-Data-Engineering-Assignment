
import pandas as pd

if __name__ == "__main__":
    def data_processor ():
        # Define the file paths for the files we will be using
        source_path = "/opt/airflow/S3_Mount/Stocks"
        save_path = "/opt/airflow/S3_Mount/OutPut"
        index_file_path = "/opt/airflow/S3_Mount/CompanyNames/top_companies.txt"
    

        # Get the list of company names we will need to create URLs
        def extract_companies_from_index(index_file_path):
            """Generate a list of company files that need to be processed. 

            Args:
                index_file_path (str): path to index file

            Returns:
                list: Names of company names. 
            """
            company_file = open(index_file_path, "r")
            contents = company_file.read()
            contents = contents.replace("'","")
            contents_list = contents.split(",")
            cleaned_contents_list = [item.strip() for item in contents_list]
            company_file.close()
            return cleaned_contents_list
        list_of_companies = extract_companies_from_index(index_file_path)

    
        # Use the list of company names we got above to create the file paths to the company CSV files
        def get_path_to_company_data(list_of_companies, source_data_path):
            """Creates a list of the paths to the company data
                that will be processed

            Args:
                list_of_companies (list): Extracted `.csv` file names of companies whose data needs to be processed.
                source_data_path (str): Path to where the company `.csv` files are stored. 

            Returns:
                [type]: [description]
            """

            path_to_company_data = []
            for file_name in list_of_companies:
                path_to_company_data.append(source_data_path + "/" + file_name)
            return path_to_company_data

        list_of_companies = extract_companies_from_index(index_file_path)
        source_data_path = source_path

        file_paths = get_path_to_company_data(list_of_companies, source_data_path)

        dataframe = pd.DataFrame()

        for file_path in file_paths:
            try:
                df = pd.read_csv(file_path)
                dataframe = pd.concat([dataframe, df], ignore_index = True, axis=0)
            except:
                df.empty
                continue

        dataframe["stock_date"] = dataframe["Date"].astype('datetime64[ns]')
        dataframe["open_value"] = dataframe["Open"].astype('float')
        dataframe["high_value"] = dataframe["High"].astype('float')
        dataframe["low_value"] = dataframe["Low"].astype('float')
        dataframe["close_value"] = dataframe["Close"].astype('float')
        dataframe["volume_traded"] = dataframe["Volume"].astype('float')
        del dataframe["Date"]
        del dataframe["Open"]
        del dataframe["High"]
        del dataframe["Low"]
        del dataframe["Close"]
        del dataframe["Volume"]
        del dataframe["OpenInt"]



        file_name = "historical_stocks_data"
        output_path = save_path
        header = True

        def save_table(dataframe, output_path, file_name, header):
            """Saves an input pandas dataframe as a CSV file according to input parameters.

                Args:
                dataframe (pandas.dataframe): Input dataframe.
                output_path (str): Path to which the resulting `.csv` file should be saved. 
                file_name (str): The name of the output `.csv` file. 
                header (boolean): Whether to include column headings in the output file.

            """
            #print(f"Path = {output_path}, file = {file_name}")
            dataframe.to_csv(output_path + "/" + file_name + ".csv", index=False, header=header)
    
        save_table(dataframe, output_path, file_name, header)

    data_processor()

