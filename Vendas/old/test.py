from pyspark.sql.functions import *

test = 'Wilson ' + 'Rodrigues'
prc_path = 'C:/Estudos/PySpark/Projetos/Vendas/prc/'
file = prc_path + 'dados_2013.csv'
df_file = file
df_file.show()
print(test)
print(file)
