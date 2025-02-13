from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import DataFrameWriter
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os

# Set Java Home
os.environ['JAVA_HOME'] = r'C:\java8'

# Initialise Spark Session
spark = SparkSession.builder\
         .appName("Rexhall Bank ETL")\
         .config("spark.jars", r"C:\Users\DELL\OneDrive\DATA ENGINEERING FOLDER\Rexhall Bank")\
         .getOrCreate()

# Extract the data into a spark dataframe
rexhall_bank_df = spark.read.csv(r"data_set\raw_data\rexhall_bank_transactions.csv", header=True, inferSchema=True)

# Data cleaning
# Filling the missing values in the data
clean_rexhall_df = rexhall_bank_df.fillna({
    'Customer_Name': 'Unknown',
    'Customer_Address': 'Unknown',
    'Customer_State': 'Unknown',
    'Customer_City': 'Unknown',
    'Customer_Country': 'Unknown',
    'Company' : 'Unknown',
    'Job_Title': 'Unknown',
    'Email': 'Unknown',
    'Phone_Number': 'Unknown',
    'Credit_Card_Number': 0,
    'IBAN': 'Unknown',
    'Currency_Code': 'Unknown',
    'Random_Number': 0.0,
    'Category': 'Unknown',
    'Group': 'Unknown',
    'Is_Active': 'Unknown',
    'Description': 'Unknown',
    'Gender': 'Unknown',
    'Marital_Status': 'Unknown'
})

# drop missing values in Last_Updated column
clean_rexhall_df = clean_rexhall_df.na.drop(subset=['Last_Updated'])

# Data Transformation
# Creating Transaction table
transaction = clean_rexhall_df.select('Transaction_Date', 'Amount', 'Transaction_Type') \
                .withColumn('Transaction_ID', monotonically_increasing_id())\
                .select('Transaction_ID', 'Transaction_Date', 'Amount', 'Transaction_Type')
                
# Customers table
customer = clean_rexhall_df.select('Customer_Name', 'Customer_Address', 'Customer_State', 'Customer_City', 'Customer_Country')\
            .withColumn('Customer_ID', monotonically_increasing_id())\
            .select('Customer_ID', 'Customer_Name', 'Customer_Address', 'Customer_State', 'Customer_City', 'Customer_Country')

# Employee table
employee = clean_rexhall_df.select('Company', 'Job_Title', 'Email', 'Phone_Number','Gender', 'Marital_Status')\
            .withColumn('Employee_ID', monotonically_increasing_id())\
            .select('Employee_ID','Company', 'Job_Title', 'Email', 'Phone_Number','Gender', 'Marital_Status')

# Rexford Bank  fact table
fact_table = clean_rexhall_df \
    .join(transaction, ['Transaction_Date', 'Amount', 'Transaction_Type'], 'inner') \
    .join(customer, ['Customer_Name', 'Customer_Address', 'Customer_State', 'Customer_City', 'Customer_Country'], 'inner') \
    .join(employee, ['Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status'], 'inner') \
    .select('Transaction_ID', 'Customer_ID', 'Employee_ID', 'Credit_Card_Number', 'IBAN', 
            'Currency_Code', 'Random_Number', 'Category', 'Group', 'Is_Active', 
            'Description', 'Last_Updated')

# Converting from spark dataframe to pandas dataframe
customer_pd = customer.toPandas()
transaction_pd = transaction.toPandas()
employee_pd = employee.toPandas()
fact_table_pd = fact_table.toPandas()

# Saving the data to a csv file
customer_pd.to_csv('data_set/cleaned_data/customer.csv', index=False)
transaction_pd.to_csv('data_set/cleaned_data/transaction.csv', index=False)
employee_pd.to_csv('data_set/cleaned_data/employee.csv', index=False)
fact_table_pd.to_csv('data_set/cleaned_data/fact_table.csv', index=False)
print("Data saved successfully")

# Loading the environment variables
load_dotenv(override=True)
db_user = os.getenv('DB_USER')
db_name = os.getenv('DB_NAME')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_password = os.getenv('DB_PASSWORD')

# Creating the database url
database_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(database_url)

# Loading the data into a postgres database
customer_pd.to_sql('customer', engine, if_exists='replace', index=False)
transaction_pd.to_sql('transaction', engine, if_exists='replace', index=False)
employee_pd.to_sql('employee', engine, if_exists='replace', index=False)
fact_table_pd.to_sql('fact_table', engine, if_exists='replace', index=False)
print("Data loaded successfully")


