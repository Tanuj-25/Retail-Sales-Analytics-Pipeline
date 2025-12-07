import datetime
import os
import shutil
from logging import exception

from src.main.delete.local_file_delete import delete_local_file
from src.main.transformations.jobs.sales_mart_sql_transformation_write import sales_mart_calculation_table_write
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.write.parquet_writer import ParquetWriter
from src.main.transformations.jobs.dimension_tables_join import dimensions_table_join
from pyspark.sql.types import *
from pyspark.sql.functions import *
from src.main.read.database_read import DatabaseReader
from src.main.move.move_files import move_s3_to_s3
from src.main.utility.spark_session import spark_session
from src.main.download.aws_file_download import S3FileDownloader
from src.main.read.aws_read import S3Reader
from src.main.utility.my_sql_session import get_mysql_connection
from resources.dev import config
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from test.sales_data_upload_s3 import s3_directory

# from test.scratch_pad import folder_path, s3_absolute_file_path

# Get S3 Client
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

# Now we can use S3 client for our S3 operations
response = s3_client.list_buckets()
# print(response)
logger.info("List of Buckets: %s", response['Buckets'])

# Check if the Local Directory already has a file (not deleted from here due to process failure in previous stage).
# If the file is there, then check if the same file is present in the Staging Area with status as "A".
# If yes, then do not DELETE and try to re-run; Else give an ERROR and do not process the next file.
csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]

# Establishing MySQL-Python connection using - mysql-connector-python (last in requirements.txt)
connection = get_mysql_connection()
# Think of the connection as the database itself and the cursor as the tool you use to talk to the database.
# Because the database connection alone doesn’t run queries — the cursor does.
# Without creating the cursor, execute() and fetchall() won’t be available.
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    # Checking in the Staging Table whether we have an entry for the file with "A" status.
    # This would mean it failed due to some reason in the previous stage.
    sql_statement = f"""
    Select distinct file_name from {decrypt(config.database_name)}.product_staging_table where
    file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'
    """
    # {str(total_csv_files)[1:-1]} - helps to dynamically check multiple files in one go.
    # Helps to optimise by prevent looping over and checking for each file one-by-one.
    # time is saved since we do not have to create connection again-and-again for running query,
    logger.info(f"Dynamically created statement - {sql_statement}")
    cursor.execute(sql_statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run was failed, please check.")
    else:
        logger.info("No record match in Staging Table.")

else:
    logger.info("Last run was successful!!")

# Reading from S3
try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path)
    logger.info(f"Absolute path for csv file on S3 bucket - {s3_absolute_file_path}")
    # ['s3://youtube-project-testing-tanuj/sales_data/sales_data.csv', 's3://youtube-project-testing-tanuj/sales_data/sales_data_2.csv', 's3://youtube-project-testing-tanuj/sales_data/sales_data_3.csv', 's3://youtube-project-testing-tanuj/sales_data/sales_data_4.json']
    if not s3_absolute_file_path:
        logger.info(f'No files available to process at {folder_path}')
        raise exception("No data available to process..")

except Exception as e:
    logger.error(f"Exited with error - {e}")
    raise e

# Downloading into local directory
prefix = f"s3://{config.bucket_name}/"
prefix_len = len(prefix)
file_paths = [url[prefix_len:] for url in s3_absolute_file_path]
logging.info(f"File path available on S3 under {config.bucket_name} bucket and folder name is {file_paths}")
try:
    downloader = S3FileDownloader(s3_client, config.bucket_name, config.local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error(f"File download error: {e}")
    sys.exit()

# Get a list of all the files in Local Directory - ignoring the hidden by removing those starting with "."
# the ".DS_Store" created by macOS whenever you open or browse a folder with Finder.
all_files = [f for f in os.listdir(config.local_directory) if not f.startswith(".")]
logger.info(f"List of files present at my local directory after download {all_files}")

# Filtering files with ".csv" in their names and creating absolute paths.
if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(config.local_directory, files)))
        else:
            # If we got some files other than .csv like .txt or .json, etc - put it in the error folder to show to user what was wrong
            error_files.append(os.path.abspath(os.path.join(config.local_directory, files)))

    # if no .csv files are present - no data to process and raise exception
    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception ("No csv data available to process the request")

else:
    logger.error("There is no data to process.")
    raise Exception("There is no data to process.")

logger.info("***************** Listing the Files ************************")
logger.info(f"List of csv files that need to be processed - {csv_files}")

logger.info("*************** Creating Spark Session *********************")
spark = spark_session()
logger.info("*************** Spark Session created **********************")

# Check the required column in the schema of csv files. If some non-required columns,
# keep it in a list or error_files ELSE union_all the correct data into one dataframe.
logger.info("**********Checking schema for the data loaded in S3**********")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
        .option("header", True)\
        .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory column schema is - {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"The missing columns are - {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing columns for this data - {data}")
        correct_files.append(data)

logger.info(f"The list of valid files is - {correct_files}")
logger.info(f"The list of error fles is - {error_files}")
# Move the files to Error Directory on Local
logger.info("******* Moving the error data (if any) to the Error Directory.")
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        # check whether given file actually exists on the filesystem.
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)
            shutil.move(file_path, destination_path)
            logger.info(f"Moved the file {file_name} to new path - {destination_path}")

            # Now writing it to S3
            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            # S3 has not given direct move, so we copy frmo src to dest and then delete src.
            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            logger.info(f"{file_path} in Error Files does not exist.")
else:
    logger.info("******* There are no error files available in the DataSet. *******")

# Additional columns need to be taken care of - need to store somewhere, simply drop, etc.
# Determine extra columns

# Before running the process,
# Staging table in MySQL needs to be updated with 'A' or 'I' for maintaining status of processing.
logger.info("Updating in the product_staging_table that we have started the process.")
insert_statements = []
db_name = decrypt(config.database_name)
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"""
            insert into {db_name}.product_staging_table (file_name, file_location, created_date,
            status) values ('{filename}', '{file}', '{formatted_date}', 'A')
        """
        # print(statements)
        insert_statements.append(statements)
    logger.info(f"Insert tables created for staging are - {insert_statements}")
    logger.info("*************** Connecting with MySQL Server *************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("************ MySQL connected successfully **************")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("There is no files to process..")
    raise Exception("******** No data available with correct files *************")

logger.info("The Staging Table has been updated successfully.")
logger.info("Fixing the extra columns from source")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

logger.info("Creating empty DataFrame")
final_df_to_process = spark.createDataFrame([], schema)
# final_df_to_process.show()

# Create a new column with concatenated values of extra columns
for data in correct_files:
    data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"The extra columns present at source are - {extra_columns}")
    if extra_columns:
        # if extra_columns = ["col1", "col2", "col3"]
        # concat_ws(", ", col("col1"), col("col2"), col("col3"))
        data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_column")
        logger.info(f"Processed {data} and added additionl columns")
    else:
        data_df = data_df.withColumn("additional_column", lit(None))\
                .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                        "price", "quantity", "total_cost", "additional_column")

    final_df_to_process = final_df_to_process.union(data_df)

logger.info("Final Dataframe from source which will be going for processing - ")
final_df_to_process.show()

# Enrich the data from all the dimension tables
# Connecting with Database Reader
database_client = DatabaseReader(config.url, config.properties)

# Customer Table DF
logger.info("****** Loading the customer table into customer_table_df ******")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
# customer_table_df.show()

# Product Table
logger.info("****** Loading the product table into product_table_df ******")
product_table_df = database_client.create_dataframe(spark, config.product_table)

# Product_staging_table
logger.info("****** Loading the product_staging table into product_staging_table_df ******")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

# Sales_team table
logger.info("****** Loading the sales_team table into sales_team_table_df ******")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# Store Table
logger.info("****** Loading the store table into store_table_df ******")
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_join = dimensions_table_join(final_df_to_process, customer_table_df
                                                        ,store_table_df, sales_team_table_df)

# Final Enriched Data
logger.info("***************** Final Enriched Data *****************")
s3_customer_store_sales_df_join.show()

# Write the customer data to Customer Datamart in Parquet format.
# File will be written to local, then moved to S3 for reporting tool.
logger.info("Write the data into the Customer Datamart")
final_customer_datamart_df = s3_customer_store_sales_df_join\
    .select("ct.customer_id", "ct.first_name", "ct.last_name", "ct.address",
            "ct.pincode", "phone_number", "sales_date", "total_cost")
logger.info("Final data for the customer DataMart")
final_customer_datamart_df.show()

parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_datamart_df, config.customer_data_mart_local_file)
logger.info(f"******* Customer data written to local disk at {config.customer_data_mart_local_file} ********")

# Move this data to S3 for the customer data mart
logger.info("Data movement from local to S3 for customer data mart")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")

# Sales_team datamart
final_sales_team_datamart_df = s3_customer_store_sales_df_join\
    .select("store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name",
            "store_manager_name", "manager_id", "is_manager", "sales_person_address", "sales_person_pincode",
            "sales_date", "total_cost", expr("substring(sales_date, 1, 7) as sales_month"))

logger.info("Final data for the sales team Datamart -")
final_sales_team_datamart_df.show()
parquet_writer.dataframe_writer(final_sales_team_datamart_df, config.sales_team_data_mart_local_file)

# Move the data from local file to S3
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
logger.info(f"{message}")

# Also writing the data into partitions (optimization by preventing scanning of unwanted partitions)
final_sales_team_datamart_df.write.format("parquet")\
    .option("header", True)\
    .mode("overwrite")\
    .partitionBy("sales_month", "store_id")\
    .option("path", config.sales_team_data_mart_partitioned_local_file)\
    .save()

# Move the data to S3 for the partitioned folder
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp())*1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        # print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

# Calculations for the customer mart.
# Find out the customer total purchase every month and writing to MySQL table.
logger.info("****** Calculating the purchase amount by customer every month ******")
customer_mart_calculation_table_write(final_customer_datamart_df)
logger.info("***** Calculations done and written into the table *****")

# Calculation for the Sales team mart. Give 1% incentive of total_sales to the top
# performing employee for the month, others get nothing. Write to MySQL table.
logger.info("****** Calculating sales every month ******")
sales_mart_calculation_table_write(final_sales_team_datamart_df)
logger.info("***** Calculations done and written into the table *****")

# Last Step - Deleting the files from directory and also marking process as inactive 'I' as it is completed now.
# Move the file on S3 to processed_folder and remove the files from local_directory.
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info("****** Deleting sales data from local ******")
delete_local_file(config.local_directory)
logger.info("****** Deleted sales data from local ******")

logger.info("****** Deleting customers data from local ******")
delete_local_file(config.customer_data_mart_local_file)
logger.info("****** Deleted customers data from local ******")

logger.info("****** Deleting sales team data from local ******")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("****** Deleted sales team data from local ******")

logger.info("****** Deleting sales data partitioned from local ******")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("****** Deleted sales data partitioned from local ******")

# Finally, updating the status of the Staging Table from 'A' to 'I' (active to inactive)
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"""
        update {db_name}.product_staging_table set status='I', updated_date = '{formatted_date}'
        where file_name = '{filename}'
        """
        update_statements.append(statements)
    logger.info(f"Updated statements created for the Staging Table --- {update_statements}")
    logger.info("******* Connecting with the MySQL server *******")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("******** MySQL Server connected successfully *******")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("There is some issue in the processing in between")
    sys.exit()

input("Press ENTER to terminate (created to keep the SparkSession alive) ")