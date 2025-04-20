#Job que busca um arquivo CSV em um diretório gitHub
import urllib.request
import shutil
import os

#URL do arquivo do GitHub
url = 'https://github.com/andrerosa77/trn-pyspark/raw/main/dados_2011.csv'

# Caminho temporário para arquivos fontes
tmp_path = 'C:/Estudos/PySpark/Projetos/Vendas/tmp/dados_2011.csv'

# Baixar o arquivo de vendas para o diretório temporário
urllib.request.urlretrieve(url, tmp_path)

# Diretório de destino de arquivos a serem processados
src_path = 'C:/Estudos/PySpark/Projetos/Vendas/src/'

# Move o arquivo temporário para a pasta a ser procesado
shutil.copy2(tmp_path, src_path)

# Evidência do arquivo baixado e salvo em: {src_path}
files = os.listdir(src_path)
print(files)
