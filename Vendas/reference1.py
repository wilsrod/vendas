#Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
    StructField("ano", IntegerType(), True)
])

# Cria o DataFrame lendo um arquivo CSV utilizando o schema de cabeçalho
df_vendas = spark.read.csv(src_path + 'dados_2013.csv', header=True, schema=sch_vendas) 
df_vendas = df_vendas.withColumn('Ano', year('Data')) \
                     .withColumn('Mes', month('Data'))
#df_vendas.printSchema()
# -------------------------------------------------------------------------------------
# Renomear colunas
df_vendas = df_vendas.withColumnRenamed('ano', 'Ano')
# Dropar uma coluna
#df_vendas.drop('Mes')
#df_vendas.printSchema()
# -------------------------------------------------------------------------------------
# Selecionar colunas
df_vendas_res = df_vendas.select('IDProduto', 'Data', 'IDCliente', 'Produto')#.show(3)
df_vendas.select(col('IDProduto'), col('Data'), col('IDCliente'), col('Produto'))#.show(3)
df_vendas.select(col('IDProduto').alias('ID do Produto'))#.show(5) # utilizando alias para coluna
df_vendas.select('Produto', substring('Produto', 1, 7), substring('Produto', -5, 5)).show(3) #-- Usando Substring
# -------------------------------------------------------------------------------------
# Utilizando filtros nos DataFrames
#df_vendas.filter(col('IDCliente') == '124593').show(5)
#df_vendas.filter((col('Cidade') == 'Miami, FL, USA') & (col('Unidades') >= 1)).show(5) 
df_vendas.filter((col('Cidade') == 'Miami, FL, USA') & (col('PrecoUnitario') <= 90)) # condição AND
df_vendas.filter((col('Cidade') == 'Miami, FL, USA') | (col('Cidade') == 'Mesa, AZ, USA'))#.show(5) # condição OR
# -------------------------------------------------------------------------------------
# Criar coluna nova no DataFrame
df_vendas = df_vendas.withColumn('AnoMes', lit('201300'))#.show()
# -------------------------------------------------------------------------------------
# Uso de Concatenação
#df_vendas_res.withColumn('Id + Produto', concat('IDProduto', 'Produto')).show(5) # sem separador
df_vendas_res.withColumn('Id + Produto', concat_ws(' - ', 'IDProduto', 'Produto', 'Data')).show(5) # com separador
# -------------------------------------------------------------------------------------
# Alterar o tipo de coluna (data type)
df_vendas_res = df_vendas_res.withColumn('IDProduto', col('IDProduto').cast(StringType()))
df_vendas_res.printSchema() # só mostra a alteração da estrutura após a execução
# Troca para casa de um formato de data em campo string com separador '.' ex: '21.08.2024'
# dia = udf(lambda data: data.split('.')[0]) -- cria uma função usando a função lambda
# df.withColumn('Dia', dia('Nascimento')).show() -- cria uma coluna com apenas o dia
# Toda alteração de DataFrame só acontece após atribuir a mudança ao mesmo dataframe. ex: df = df---




