# Databricks notebook source
import json
import os
from datetime import datetime, timedelta
import random
from pyspark.sql import SparkSession, functions as F


# Define the source directory
source_dir = "/mnt/demo/transactions-raw"

# Global counter for unique TransactionId
global_transaction_id_counter = 1

# Function to generate sample transactions
def generate_sample_transactions(file_path, num_records=2):
    global global_transaction_id_counter
    transactions = []
    now = datetime.now()
    for i in range(num_records):
        transaction = {
            "TransactionId": f"T{global_transaction_id_counter:03}",
            "CustomerId": str(random.randint(1, 5)),
            "TransactionDate": (now - timedelta(minutes=i*10)).isoformat(),
            "TypeId": str(random.randint(1, 5))
        }
        transactions.append(transaction)
        global_transaction_id_counter += 1
    
    with open(file_path, 'w') as f:
        json.dump(transactions, f)

# Function to list and display file contents
def list_and_display_file_contents(file_paths):
    for file_path in file_paths:
        with open(file_path, 'r') as f:
            file_content = json.load(f)
            print(f"\nContents of {file_path.split('/')[-1]}:")
            print(json.dumps(file_content, indent=4))

# Generate initial sample JSON files
os.makedirs("/dbfs" + source_dir, exist_ok=True)
for i in range(5):
    file_path = f"/dbfs{source_dir}/transactions_{i+1}.json"
    generate_sample_transactions(file_path)

# List and display the generated files
initial_files = dbutils.fs.ls(f"dbfs:{source_dir}")
print("Generated files:")
for file in initial_files:
    print(file.name)
list_and_display_file_contents([f"/dbfs{source_dir}/{file.name}" for file in initial_files])

# Function to generate and load new data
def load_new_data(start_index=5, end_index=10):
    new_files = []
    for i in range(start_index, end_index):
        file_path = f"/dbfs{source_dir}/transactions_{i+1}.json"
        generate_sample_transactions(file_path)
        new_files.append(file_path)
    return new_files

# Delete all files after displaying contents
def delete_files(file_paths):
    for file_path in file_paths:
        dbutils.fs.rm(file_path.replace("/dbfs", "dbfs:"))
        print(f"\nDeleted {file_path.split('/')[-1]}")
