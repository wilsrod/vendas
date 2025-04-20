#Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

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

df_vendas2.printSchema()

# Uso da Window Function para Ranking
num_linha = Window.partitionBy('Cidade').orderBy(desc('CustoUnitario'))
#df_vendas2.withColumn('NumLinha', row_number().over(num_linha)).show()

rank = Window.partitionBy('Produto').orderBy('CustoUnitario')
#df_vendas2.withColumn('rank', dense_rank().over(rank)).show(50)  #-- coloca cada um em uma posição q pode repetir

porcentagem = Window.partitionBy('Produto').orderBy('Estado')
#df_vendas2.withColumn('%', percent_rank().over(porcentagem)).show(20)

# Funções Analíticas cm agregações
degrau = Window.partitionBy('Estado').orderBy('Cidade')
#df_vendas2.withColumn('Degrau', lag('CustoUnitario').over(degrau)).show(20) # Mostra o valor da coluna da linha anterior

degrau = Window.partitionBy('Estado').orderBy('Cidade')
#df_vendas2.withColumn('Degrau', lead('CustoUnitario').over(degrau)).show(20) # Mostra o valor da coluna da próxima linha

#df_vendas2.groupBy('Categoria').agg({'CustoUnitario':'avg'}).orderBy('avg(CustoUnitario)', \
                       # ascending=False).show() # max, min, orderBy
#df_vendas2.groupBy('Categoria').agg(avg('CustoUnitario')).orderBy('avg(CustoUnitario)', ascending=False).show()

# Clausula Where usando AND e NOT EQUAL
#df_vendas2.where(('Categoria == "Urban"') and ('Mes == "11"') and \
#                 ('Cidade != "Miami, FL, USA"')).show(truncate=False)
# Usando uma lista
li=['CA','FL','OH']
#df_vendas2.filter(df_vendas.Estado.isin(li)).show()

# Describe - 
#df_vendas2.describe().show()
df_vendas2.where('Cidade = "San Diego, CA, USA"').describe().show()

# Limpa a memória
df_vendas.unpersist()
spark.catalog.clearCache()