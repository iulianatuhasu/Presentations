# Databricks notebook source
# DBTITLE 1,Method to create dataframe
def create_dataframe():
    df = spark.createDataFrame([("1", "John", 25, "john@example.com"),
                            ("2", "Alice", 30, "alice@example.com"),
                            ("3", "Bob", 28, "bob@example.com"),
                            ("4", "Eve", 35, "eve@example.com"),
                            ("5", "Charlie", 22, "charlie@example.com")],
                           ["id", "name", "age", "emailaddress"])
    return df

def create_dataframe_2():
    df = spark.createDataFrame([("1", "John", 25, "john@example.com","IT"),
                            ("2", "Alice", 30, "alice@example.com","IT"),
                            ("3", "Bob", 28, "bob@example.com","Sales"),
                            ("4", "Eve", 35, "eve@example.com","Sales"),
                            ("5", "Charlie", 22, "charlie@example.com","HR")],
                           ["id", "name", "age", "emailaddress","department"])
    return df

# COMMAND ----------

# DBTITLE 1,Create a dataframe - from scratch, from csv, from json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create dataframe
df = create_dataframe()
print("Create dataframe from values:")
df.show()
print(df)

# Create dataframe from CSV - without taking into account the header
df_csv_without_header = spark.read.csv("/FileStore/tables/pyspark-demo/sample1.csv")
print("Create dataframe from CSV without taking into account the header:")
print(df_csv_without_header)
df_csv_without_header.show()

# Create dataframe from CSV - by taking into account the header
df_csv_with_header = spark.read.option("header", "true").csv("/FileStore/tables/pyspark-demo/sample1.csv")
print("Create dataframe from CSV by taking into account the header:")
print(df_csv_with_header)
df_csv_with_header.show()

# Create dataframe from JSON
df_json = spark.read.json("/FileStore/tables/pyspark-demo/sample3.json")
print("Create dataframe from JSON:")
print(df_json)
df_json.show()

# Create dataframe from JSON by specifying a schema
custom_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("emailaddress", StringType(), True)
])
df_json_schema = spark.read.schema(custom_schema).json("/FileStore/tables/pyspark-demo/sample3.json")
print("Create dataframe from JSON with predefined schema:")
print(custom_schema)
print(df_json_schema)
df_json_schema.show()

# Read from a file with more columns than we have in the schema
df_json_schema_more_cols = spark.read.schema(custom_schema).json("/FileStore/tables/pyspark-demo/sample4.json")
print("Create dataframe from JSON that has more cols than defined in the schema:")
df_json_schema_more_cols.show()


# COMMAND ----------

# DBTITLE 1,Quick tip - difference between print and printSchema
# Create dataframe
df = create_dataframe()
print("Show print(df):")
print(df)
print("Show printSchema:")
df.printSchema()


# COMMAND ----------

# DBTITLE 1,Save df as CSV, Parquet, JSON
# Delete the file - recurse=true means to delete all the subdirectories if there are any
dbutils.fs.rm("/FileStore/tables/pyspark-demo/output_1/csv_output_custom_delimiter", recurse=True)
dbutils.fs.rm("/FileStore/tables/pyspark-demo/output_1/json_output_gzip", recurse=True)

# Create dataframe
df = create_dataframe()
print("Dataframe used in this demo:")
df.show()

# Save df as Parquet and overwrite the previous file
df.write.mode("overwrite").parquet("/FileStore/tables/pyspark-demo/output_1/parquet_output_overwrite")

# Save df as CSV with custom delimiter and header
df.write.option("delimiter", ",").option("header", "true").csv("/FileStore/tables/pyspark-demo/output_1/csv_output_custom_delimiter")

# Save df as JSON and compress it
df.write.option("compression", "gzip").json("/FileStore/tables/pyspark-demo/output_1/json_output_gzip")

# Testing

parquet_df = spark.read.parquet("/FileStore/tables/pyspark-demo/output_1/parquet_output_overwrite")
print("Data from the Parquet file:")
parquet_df.show()


# COMMAND ----------

# DBTITLE 1,General Dataframe functions
from pyspark.sql.functions import desc
# Create dataframe
df = create_dataframe()
print("Dataframe used in this demo:")
df.show()

# List columns
print('Columns of df DataFrame are: ' + ', '.join(df.columns)) # as a sequence of strings
print(df.columns) # as a list

# List no of records
print('Number of rows of df Dataframe:', df.count())

# Sort the df by age
df_sorted = df.orderBy(desc("age"))
print("df ordered by age:")
df_sorted.show()

df_collect = df.collect()
print("df with collect:")
print(df_collect)


# COMMAND ----------

# DBTITLE 1,Working with Columns
from pyspark.sql.functions import col

# Create dataframe
df = create_dataframe()
print("Dataframe used in this demo:")
df.show()

# Select 2 columns
selected_columns = df.select(col("id"), col("name"))
print("DataFrame with only 2 columns:")
selected_columns.show()

# Add another column
df_with_added_column = df.withColumn("is_valid", col("id").isNotNull())
print("DataFrame with the new column is_valid):")
df_with_added_column.show()

# Remove a column
df_without_email = df.drop("emailaddress")
print("DataFrame without email address field:")
df_without_email.show()

# Change data type of age column
df_with_changed_data_type = df.withColumn("age", col("age").cast("string"))
print("DataFrame with column age as string data type instead of int:")
df_with_changed_data_type.show()
print(df_with_changed_data_type)


# COMMAND ----------

# DBTITLE 1,Working with Rows
from pyspark.sql import Row
# Create dataframe
df = create_dataframe_2()
print("Dataframe used in this demo:")
df.show()

# Get the first row
first_row = df.first()
print("First Row:")
print(first_row)

# Get the second row - collect the df into a list of rows
rows = df.collect()
# Access the second row
second_row = rows[1]
print("Second Row:")
print(second_row)

# Access row elements by index
print("First element of the second row is:", second_row[0])

# Access rows based on column name
print("Name:", first_row["name"])
print("Age:", first_row["age"])

# Filter rows based on a condition
filtered_rows = df.filter(df["age"] > 28)
print("Filtered Rows:")
filtered_rows.show()

# Create a new row and append it to the DataFrame
new_row = Row(id="6", name="David", age=26, emailaddress="david@example.com", department="HR")
column_names = df.columns
df = df.union(spark.createDataFrame([new_row], column_names))
print("DataFrame after adding a new row:")
df.show()

# Remove rows with a specific condition
df = df.filter(df["name"] != "David")
print("DataFrame after removing 'David':")
df.show()

# COMMAND ----------

# DBTITLE 1,Update a dataframe
from pyspark.sql.functions import when

# Create dataframe
df = create_dataframe()
print("Dataframe used in this demo:")
df.show()

# Update age column with 32 for Alice
df_updated = df.withColumn("age", when(col("name") == "Alice", 32).otherwise(col("age")))
df_updated.show()

# COMMAND ----------

# DBTITLE 1,JOIN dataframes
# Create dataframe 1
df1 = create_dataframe()

# Create dataframe 2
df2 = create_dataframe_2()

# Join the dataframes - inner
joined_df_inner = df1.join(df2, "id", "inner")
print("df1 inner join df2:")
joined_df_inner.show()

# Join the dataframes - left
#joined_df_left = df1.join(df2, "id", "left")
#print("df1 left join df2:")
#joined_df_left.show()


# COMMAND ----------

# DBTITLE 1,Working with grouped data
from pyspark.sql.functions import max, collect_list, avg,count

# Create dataframe
df = create_dataframe_2()
print("Dataframe used in this demo:")
df.show()

# Group by department and count employees
result_1 = df.groupBy("department").count()
print("Group by department and count employees:")
result_1.show()

# Group the data by the department column and find the employee with the highest age in each department
result_2 = df.groupBy("department").agg(max("age").alias("oldest_employee_age"))
print("Group the data by the department column and find the employee with the highest age in each department:")
result_2.show()

# Approach 1 - Group the data by the department column and calculate the average age of employees in each department
result_3 = df.groupBy("department").agg({"age": "avg"})
print("Approach 1 - Group the data by the department column and calculate the average age of employees in each department:")
result_3.show()

# Approach 2 - Group the data by the department column and calculate the average age of employees in each department
result_3_1 = df.groupBy("department").agg(avg("age").alias("avg_age"))
print("Approach 2 - Group the data by the department column and calculate the average age of employees in each department:")
result_3_1.show()

# Group the data by the department column and create a list of employee names in each department
result_4 = df.groupBy("department").agg(collect_list("name").alias("employee_names"))
print("Group the data by the department column and create a list of employee names in each department:")
result_4.show()

# Group by 2 columns
result_5 = df.groupBy("department","age").agg(count("department"))
print("Group by 2 columns:")
result_5.show()

# COMMAND ----------

# DBTITLE 1,Working with Window functions
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Create dataframe
df = create_dataframe_2()
print("Dataframe used in this demo:")
df.show()

# Define the Window
window_spec_1 = Window.partitionBy("department").orderBy(F.desc("age"))

# Calculate the row_number of each row partitioned by dep and ordered by age
print("Calculate the row_number of each row partitioned by dep and ordered by age DESC:")
df.withColumn("row_number", F.row_number().over(window_spec_1)).orderBy("id").show()

# Define the Window
window_spec_2 = Window.partitionBy("department").orderBy(F.desc("department"))

# Calculate the age average within a department
print("Calculate the age average within a department:")
df.withColumn("age_average", F.avg("age").over(window_spec_2)).orderBy("id").show()

# Calculate the difference between the current row's age and the previous row's age
print("Calculate the difference between the current row's age and the previous row's :")
df.withColumn("age_diff", F.col("age") - F.lag("age").over(window_spec_2)).orderBy("id").show()



# COMMAND ----------

# DBTITLE 1,Working with tables
# Create dataframe
df = create_dataframe()
print("Dataframe used in this demo:")
df.show()

# Save df as table
df.write.mode("overwrite").saveAsTable("employee_table")

# Query the table
result = spark.sql("SELECT name, age FROM employee_table WHERE age > 25")
print("Table result is:")
result.show()

# COMMAND ----------

# DBTITLE 1,Working with views - temp & global temp
# Create dataframe
df = create_dataframe_2()
print("Dataframe used in this demo:")
df.show()

# Create or replace a temporary view from the df
df.createOrReplaceTempView("people_view")

# Query the temp view
result_1 = spark.sql("SELECT name, age FROM people_view WHERE age > 25")
print("Temp view result is:")
result_1.show()

# Create or replace a global temporary view from the df
df.createOrReplaceGlobalTempView("people_view2")

# Query the global temp view
result_2 = spark.sql("SELECT name, age FROM global_temp.people_view2 WHERE age > 25")
print("Global temp view result is:")
result_2.show()

# COMMAND ----------

# DBTITLE 1,SparkSQL vs DataFrame
from pyspark.sql.functions import col, when

# Create a DataFrame
data = [
    (1, "John", "IT", 500),
    (2, "Alice", "HR", 600),
    (3, "Bob", "IT", 700),
    (4, "Eve", "Marketing", 450),
    (5, "Charlie", "HR", 550),
    (6, "David", "Marketing", 800)
]

columns = ["id", "name", "department", "salary"]
df = spark.createDataFrame(data, columns)
print("Dataframe used in this demo:")
df.show()

# Define a DataFrame method to calculate bonuses and categorize employees
def calculate_bonuses_and_categorize(df):
    df = df.withColumn("bonus", when(col("salary") >= 600, 0.1 * col("salary")).otherwise(0))
    df = df.withColumn("category", when(col("department") == "IT", "Technical").otherwise("Non-Technical"))
    return df

# Method for filtering employees with high bonuses
def filter_employees_with_high_bonuses(df):
    return df.filter(col("bonus") >= 50)

# Call the methods
df_output = calculate_bonuses_and_categorize(df)
result_df = filter_employees_with_high_bonuses(df_output)

# Show the result
print("DataFrame result after calling the first method:")
df_output.show()
print("DataFrame result after calling the second method:")
result_df.show()


######################################################Spark SQL#############################################################

# Create a temporary view for the df
df.createOrReplaceTempView("employee_data")

# Use SQL with CTE to calculate bonuses, categorize employees and filter based on bonus
result_df_sparksql = spark.sql("""
    WITH calculated_data AS (
        SELECT *,
               CASE
                   WHEN salary >= 600 THEN 0.1 * salary
                   ELSE 0
               END AS bonus,
               CASE
                   WHEN department = 'IT' THEN 'Technical'
                   ELSE 'Non-Technical'
               END AS category
        FROM employee_data
    ) SELECT * FROM calculated_data WHERE bonus >= 50
""")

# Result
print("Result using SparkSQL:")
result_df_sparksql.show()

# COMMAND ----------

# DBTITLE 1,Write sql code using magic command
# MAGIC %sql
# MAGIC SELECT * FROM employee_data
# MAGIC

# COMMAND ----------

# DBTITLE 1,Semi Join and Anti Join
# Semi-Join returns only the rows from the first df that have matching keys in the second df. while the inner join returns rows from both DataFrames # for which there is a matching key in both DataFrames

df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])
df2 = spark.createDataFrame([(2, "Sales"), (4, "Marketing")], ["id", "department"])

semi_join_result = df1.join(df2, on="id", how="left_semi")
semi_join_result.show()

# returns only the rows from the first df that do not have matching keys in the second df
anti_join_result = df1.join(df2, on="id", how="left_anti")
anti_join_result.show()

# COMMAND ----------

# DBTITLE 1,Check directory structure
# MAGIC %fs
# MAGIC ls /FileStore/tables/pyspark-demo
