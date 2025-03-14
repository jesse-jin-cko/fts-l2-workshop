import csv
import os

import pandas as pd
import requests
import snowflake.connector
import sys
from datetime import datetime, timedelta
from os.path import expanduser
from pathlib import Path


snowflake.connector.paramstyle = 'qmark'
info = input("What ID do you have? Merchant ID(1), Business ID(2): ")
ID = input("Enter ID (For multiple IDs, separate with a comma): ")
startDate = input("Enter start date(YYYY-MM-DD): ")
endDate = input("Enter end date(YYYY-MM-DD): ")
ticket = input("Enter the ticket id: ")
merge_flag = input("Do you want to merge the files? Merge(1), Don't merge(2): ")

home = expanduser("~")
#************** Change the below to where you want the files uploaded *********************************
GDpath = (f'{home}/Documents/Code/Multi_payment_statements_downloads/')
#************** Change the below to where you want the input file stored *********************************
inputPath = (f'{home}/Documents/Code/Multi_payment_statements_downloads/')


cnn = snowflake.connector.connect(
    user='JJIN',
    account='checkout-checkout.privatelink',
    role = 'JESSE_JIN',
    warehouse='MEDIUM_WH',
    database='LANDING',
    schema='ADMINISTRATION',
    authenticator="externalbrowser"
    )

#Run query and save to input.csv
cs = cnn.cursor()
if info=="1":
    query =(f"select b.businessname,c.businessId,concat('sk_',c.LiveAPISecretKey) \"secretkey\", '{startDate}' as \"Start Date\", '{endDate}' as \"End Date\" from \"LANDING\".\"ADMINISTRATION\".\"DBO_BUSINESS\" b INNER JOIN (select c.*, row_number() over (partition by BusinessId order by BusinessId desc) as seqnum from \"LANDING\".\"ADMINISTRATION\".\"DBO_CHANNEL\" c ) c on C.BusinessId = B.BusinessId and seqnum = 1 WHERE   B.merchantaccountid in ({ID});")
elif info=="2":
    query =(f"select b.businessname,c.businessId,concat('sk_',c.LiveAPISecretKey) \"secretkey\", '{startDate}' as \"Start Date\", '{endDate}' as \"End Date\" from \"LANDING\".\"ADMINISTRATION\".\"DBO_BUSINESS\" b INNER JOIN (select c.*, row_number() over (partition by BusinessId order by BusinessId desc) as seqnum from \"LANDING\".\"ADMINISTRATION\".\"DBO_CHANNEL\" c ) c on C.BusinessId = B.BusinessId and seqnum = 1 WHERE   B.businessId in ({ID});")
else:
    print("You didn't enter 1 or 2")
    sys.exit()

#Create directory
try:
    Path(GDpath).mkdir(parents=True, exist_ok=True)
except OSError:
    print ("Creation of the directory %s failed. The folder may already exist" % GDpath)
else:
    print ("Successfully created the directory %s " % GDpath)

#Run query and save to input.csv
cs.execute(query)
df = cs.fetch_pandas_all()
df.to_csv(f'{inputPath}input_{ticket}.csv')

print(df)
cs.close()
cnn.close()



#Querying the api
def query_api(business_id, api_key,business_name,start_date,end_date):
    home = expanduser("~")
    path = f'{GDpath}{business_name}_payments/'
    headers = {
        "Authorization": api_key,
        "Expect":""
    }

    try:
        Path(path).mkdir(parents=True, exist_ok=True)
    except OSError:
        print ("Creation of the directory %s failed. The folder may already exist" % path)
    else:
        print ("Successfully created the directory %s " % path)

    while end_date >= start_date:
        if start_date.weekday() < 5:
            start_date = start_date.strftime("%y%m%d")
            # r = requests.get(f"https://api.checkout.com/reporting/payments/download?from={start_date}&to={end_date}, headers=headers")
            r = requests.get(f"https://api.checkout.com/reporting/statements/{start_date}B{business_id}/payments/download", headers=headers)
            # r = requests.get(f"http://internal-moola-public-prod-green-1651310552.eu-west-1.elb.amazonaws.com/statements/{start_date}B{business_id}/payments/download", headers=headers)
            print(r.text)
            with open(f"{path}{start_date}B{business_id}.csv", 'w+') as f:
                f.write(r.text)
            start_date = datetime.strptime(start_date,"%y%m%d")
        start_date = start_date + timedelta(days=1)


def merge_csv(base_path, output_file_name):
    """
    Merge small CSV files in a specified directory and delete them after merging.
    Ensures data integrity by validating each file before merging.

    :param base_path: Path to the directory containing the small files.
    :param output_file_name: Name of the merged output file.
    """
    try:
        # Check if the base path exists
        if not os.path.exists(base_path):
            raise FileNotFoundError(f"Directory not found: {base_path}")

        # Get a list of all CSV files in the directory
        file_list = [f for f in os.listdir(base_path) if f.endswith('.csv')]
        if not file_list:
            print("No CSV files found in the directory.")
            return

        # Filter files that match the naming pattern (e.g., 230130B209042.csv)
        # Assumes files have a consistent naming pattern (e.g., 6-digit date + identifier)
        filtered_files = [
            f for f in file_list
            if len(f.split('.')[0]) >= 6  # At least 6 characters before the file extension
        ]

        if not filtered_files:
            print("No files matching the naming pattern found.")
            return

        # Construct full file paths
        file_paths = [os.path.join(base_path, f) for f in filtered_files]

        # Read and validate each file before merging
        dfs = []
        for file_path in file_paths:
            try:
                # Read the CSV file
                df = pd.read_csv(file_path, low_memory=False)
                # Validate the dataframe (check if it's not empty)
                if df.empty:
                    print(f"Warning: File is empty and will be skipped: {file_path}")
                    continue
                # Ensure the dataframe has consistent columns (optional, based on your use case)
                if dfs and not df.columns.equals(dfs[0].columns):
                    print(f"Warning: File has inconsistent columns and will be skipped: {file_path}")
                    continue
                dfs.append(df)
                print(f"Successfully read and validated: {file_path}")
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

        if not dfs:
            print("No valid data to merge.")
            return

        # Concatenate all dataframes
        df_concat = pd.concat(dfs, ignore_index=True)

        # Validate the merged dataframe
        if df_concat.empty:
            print("Warning: Merged dataframe is empty. No file will be saved.")
            return

        # Save the merged dataframe to the output file
        output_path = os.path.join(base_path, output_file_name)
        df_concat.to_csv(output_path, index=False)
        print(f"Merged file saved to: {output_path}")



        # Delete the small files after successful merging
        for file_path in file_paths:
            try:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {str(e)}")

        print("File merging and cleanup completed successfully.")

    except Exception as e:
        print(f"Error during execution: {str(e)}")


# Loading input.csv and establishing columns to be used

with open(f'{inputPath}/input_{ticket}.csv') as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    for row in csv_reader:
        business_id = row['BUSINESSID']
        api_key = row['secretkey']
        business_name= row['BUSINESSNAME']
        start_date= row['Start Date']
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date= row['End Date']
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        query_api(business_id, api_key,business_name,start_date,end_date)
        if merge_flag == "1":
            file_name = f"{business_name}_breakdown_{startDate}_{endDate}.csv"
            path = f'{GDpath}{business_name}_payments/'
            merge_csv(path, file_name)
