#Importar as bibliotecas necessárias
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from pyspark.sql.window import Window


# Iniciar a SparkSession
spark = SparkSession.builder.appName('Load Data Bronze').getOrCreate()

# Diretório de destino de arquivos a serem processados
src_path = 'C:/Estudos/PySpark/Projetos/Vendas/src/'
prc_path = 'C:/Estudos/PySpark/Projetos/Vendas/prc/'
brz_path = 'C:/Estudos/PySpark/Projetos/Vendas/bronze/'

# Definir o esquema dos dados brutos
sch_vendas = StructType([
    StructField("IDProduto", IntegerType(), True),
    StructField("Data", DateType(), True),
    StructField("IDCliente", IntegerType(), True),
    StructField("IDCampanha", IntegerType(), True),
    StructField("Unidades", IntegerType(), True),
    StructField("Produto", StringType(), True),
    StructField("Categoria", StringType(), True),
    StructField("Segmento", StringType(), True),
    StructField("IDFabricante", IntegerType(), True),
    StructField("Fabricante", StringType(), True),
    StructField("CustoUnitario", DoubleType(), True),
    StructField("PrecoUnitario", DoubleType(), True),
    StructField("CodigoPostal", StringType(), True),
    StructField("EmailNome", StringType(), True),
    StructField("Cidade", StringType(), True),
    StructField("Estado", StringType(), True),
    StructField("Regiao", StringType(), True),
    StructField("Distrito", StringType(), True),
    StructField("Pais", StringType(), True),
    StructField("Ano", IntegerType(), True)
])

# Cria o DataFrame lendo um arquivo CSV utilizando o schema de cabeçalho
df_vendas = spark.read.csv(src_path + 'dados_2013.csv', header=True, schema=sch_vendas) 
df_vendas = df_vendas.withColumn('Mes', month('Data'))
#df_vendas2 = df_vendas.select('Produto','Categoria','Fabricante','CustoUnitario','Ano')
df_vendas2 = df_vendas.select('Produto','Categoria','Fabricante','CustoUnitario','Cidade','Estado', 'Ano','Mes')
# df_vendas2.printSchema()
# Uso do DISTINCT
#df_vendas2.select(col('Cidade')).distinct().show()
#df_vendas2.select(col('Cidade')).distinct().sort(col('Cidade')).show()
# O comando COLLECT cria uma lista do resultado da consulta
dataCollect = df_vendas2.select(col('Estado')).distinct().collect()   #sort(col('Estado')).show()
#print(dataCollect)
print(dataCollect[8])
#print(type(dataCollect[8]))
ver = dataCollect[8][0]
print(ver)
lista = []
for Estado in dataCollect:
     lista.append(Estado[0])
print(lista)
# --------------------------------------------------------------------------------------------------------------
# Usando o WHEN() e o OTHERWISE() - Como se fosse o IF ELSE
#df_vendas2.withColumn('Coluna_Nova', when(col('Mes') == 12, 'Dezembro').otherwise('Não Identificado')).show()
# Com uma cadeia de WHEN()
df_vendas2 = df_vendas2.withColumn('Mês Descritivo', when(col('Mes') == 12, 'Dezembro') \
                                    .when(col('Mes') == 1, 'Janeiro') \
                                    .when(col('Mes') == 2, 'Fevereiro') \
                                    .when(col('Mes') == 3, 'Março') \
                                    .when(col('Mes') == 4, 'Abril') \
                                    .when(col('Mes') == 5, 'Maio') \
                                    .when(col('Mes') == 6, 'Junho') 
                                    .when(col('Mes') == 7, 'Julho') \
                                    .when(col('Mes') == 8, 'Agosto') \
                                    .when(col('Mes') == 9, 'Setembro') \
                                    .when(col('Mes') == 10, 'Outubro') \
                                    .when(col('Mes') == 11, 'Novembro') \
                                    .otherwise('Não Identificado'))
df_vendas2.show()
df_vendas_jan  = df_vendas2.filter(col('Mes') ==  1).limit(5)
df_vendas_dez  = df_vendas2.filter(col('Mes') == 12).limit(5)
df_vendas_meses = df_vendas_dez.union(df_vendas_jan)
#df_vendas_meses.show()
# --------------------------------------------------------------------------------------------------------------
# Usando JOINS entre DataFrames
df_vendas_dez = df_vendas_dez.drop('Categoria','CustoUnitario','Cidade','Ano','Mes') # remove colunas
df_vendas_jan = df_vendas_jan.drop('Categoria','CustoUnitario','Cidade','Ano','Mes')  # remove colunas
# Usando JOIN
df_novo = df_vendas_dez.join(df_vendas_jan, df_vendas_dez.Produto == df_vendas_jan.Produto)
#df_novo.show()
# Usando o INNER JOIN
df_novo = df_vendas_dez.join(df_vendas_jan, df_vendas_dez.Produto == df_vendas_jan.Produto, 'inner')
#df_novo.show()
# Usando o LEFT JOIN
df_novo = df_vendas_dez.join(df_vendas_jan, df_vendas_dez.Produto == df_vendas_jan.Produto, 'left')
df_novo.show()

                      






# --------------------------------------------------------------------------------------------------------------
# Limpa a memória
df_vendas.unpersist()
spark.catalog.clearCache()