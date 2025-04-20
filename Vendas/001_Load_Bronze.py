#Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Iniciar a SparkSession
spark = SparkSession.builder.appName('Load Data Bronze') \
    .config("spark.sql.shuffle.partitions", "200")  \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Diretório de destino de arquivos a serem processados
src_path = 'C:/Estudos/PySpark/Projetos/Vendas/src/'
prc_path = 'C:/Estudos/PySpark/Projetos/Vendas/prc/'
brz_path = 'C:/Estudos/PySpark/Projetos/Vendas/bronze'

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

df_vendas = spark.read.csv(src_path + 'dados_2013.csv', header=True, schema=sch_vendas) 
df_vendas = df_vendas.withColumn('Ano', lit(2013)) \
                     .withColumn('Mes', month('Data'))
df_vendas.printSchema()
df_vendas.select('IDProduto', 'Data', 'Ano', 'Mes').show(5)
#df_vendas.write.partitionBy('Ano', 'Mes').parquet('reporting')
df_vendas.write.parquet('reporting.parquet')






#--------------------------------------------------------------------------------------
#df_vendas.printSchema()
#df_vendas.show()

# Limpa a memória
#df_vendas.unpersist()
#spark.catalog.clearCache()
