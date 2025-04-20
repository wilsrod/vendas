#Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import fastparquet as fp

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


df_vendas = spark.read.csv(src_path + 'dados_2013.csv', header=True, schema=sch_vendas).limit(5)

df_vendas_end = df_vendas.select('IDProduto', 'Data', 'Fabricante')

# Escrever a tabela no formato Parquet, particionando por DataVenda (ano e mês)
#df_vendas_end.withColumn('Ano',lit(2013)).write.mode('overwrite').partitionBy('IDProduto').parquet('C:/tmp')
fp.write('vendas_fp.parquet', df_vendas_end)
df_vendas_end.show()



# Escrever a tabela no formato Parquet, particionando por DataVenda (ano e mês)
#df_vendas.withColumn("Ano", lit(2013)).withColumn('Mes', lit('')) #\
#df_vendas.withColumn("Mes", month('Data')) \
#             .write.mode("overwrite").partitionBy("Ano").parquet(brz_path)
#df_vendas.printSchema()
#df_vendas.write.parquet('C:/Estudos/PySpark/Projetos/Vendas/bronze/file.parquet')
# fp.write('vendas_fp.parquet', df_vendas)
# df_vendas.show()


#df_vendas2.write.mode("overwrite").partitionBy("Data").parquet(brz_path)
df_vendas.unpersist()
spark.catalog.clearCache()
# Apresentando o DataFrame
#df_vendas.show(5)
#df_vendas2.printSchema()

