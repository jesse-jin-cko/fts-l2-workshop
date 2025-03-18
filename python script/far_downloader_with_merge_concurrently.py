import csv
import os

import pandas as pd

import asyncio
import aiohttp
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


async def download_file(session, date, business_id, api_key, business_name, semaphore, max_retries=5):
    """
    Downloads a single file asynchronously with retries until successful.
    """
    async with semaphore:  # Use semaphore to limit concurrent requests
        path = f'{GDpath}{business_name}_payments/'
        formatted_date = date.strftime("%y%m%d")
        url = f"https://api.checkout.com/reporting/statements/{formatted_date}B{business_id}/payments/download"
        headers = {"Authorization": api_key, "Expect": ""}

        retry_count = 0
        backoff_time = 2  # Initial backoff time in seconds

        while True:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        content = await response.text()
                        with open(f"{path}{formatted_date}B{business_id}.csv", 'w+', encoding='utf-8') as file:
                            file.write(content)
                        print(f"Downloaded {formatted_date}B{business_id}.csv")
                        return  # Exit loop on success
                    else:
                        print(f"Error {response.status} for {formatted_date}: Retrying...")
                        if retry_count >= max_retries:
                            raise Exception(f"Max retries reached for {formatted_date}")
            except Exception as e:
                print(f"Error downloading {formatted_date}: {e}")
                if retry_count >= max_retries:
                    print(f"Failed to download {formatted_date} after {max_retries} retries.")
                    return
            # Exponential backoff before retry
            retry_count += 1
            await asyncio.sleep(backoff_time)
            backoff_time = min(backoff_time * 2, 60)  # Cap backoff time to 60 seconds


async def query_api_with_retry(business_id, api_key, business_name, start_date, end_date, max_concurrent):
    """
    Downloads files asynchronously with retries and rate limiting.
    """
    # Ensure start_date and end_date are datetime objects
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Create directory for saving files
    path = f'{GDpath}{business_name}_payments/'
    Path(path).mkdir(parents=True, exist_ok=True)

    # Generate a list of dates
    date_list = [
        start_date + timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
        if (start_date + timedelta(days=i)).weekday() < 5
    ]

    # Semaphore for limiting concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)

    # Use aiohttp for asynchronous requests
    async with aiohttp.ClientSession() as session:
        tasks = [
            download_file(session, date, business_id, api_key, business_name, semaphore)
            for date in date_list
        ]
        await asyncio.gather(*tasks)

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
        columns_set = None
        for file_path in file_paths:
            try:
                # Read the CSV file
                df = pd.read_csv(file_path, low_memory=False)
                # Validate the dataframe (check if it's not empty)
                if df.empty:
                    print(f"Warning: File is empty and will be skipped: {file_path}")
                    continue
                # Ensure the dataframe has consistent columns (use the columns from the first valid dataframe)
                if columns_set is None:
                    columns_set = df.columns
                if not columns_set.equals(df.columns):
                    print(f"Warning: File has inconsistent columns and will be skipped: {file_path}")
                    continue
                dfs.append(df)
                print(f"Successfully read and validated: {file_path}")
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

        if not dfs:
            print("No valid data to merge.")
            return

        # Merge all valid dataframes
        merged_df = pd.concat(dfs, ignore_index=True)

        # Save the merged dataframe to a CSV file
        output_file_path = os.path.join(base_path, output_file_name)
        merged_df.to_csv(output_file_path, index=False)
        print(f"Merged file saved as: {output_file_name}")

        # Optionally, delete the original CSV files after merging
        for file_path in file_paths:
            os.remove(file_path)
            print(f"Deleted file: {file_path}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

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
        asyncio.run(query_api_with_retry(business_id, api_key,business_name,start_date,end_date,max_concurrent=50))
        if merge_flag == "1":
            file_name = f"{business_name}_breakdown_{startDate}_{endDate}.csv"
            path = f'{GDpath}{business_name}_payments/'
            merge_csv(path, file_name)