
#Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import fastparquet as fp
import pandas as pd

# Iniciar a SparkSession
spark = SparkSession.builder.appName('Teste').getOrCreate()

# Create a sample DataFrame
# df = pd.DataFrame({
#     'Name': ['Alice', 'Bob', 'Charlie'],
#     'Age': [25, 30, 35],
#     'Salary': [50000, 60000, 70000]
# })

df = spark.read.csv('teste.csv', header=False, inferSchema=False)

# Write the DataFrame to a Parquet file
fp.write('teste.parquet', df)