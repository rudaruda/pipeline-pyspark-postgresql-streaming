import os
import shutil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql.functions import input_file_name

# Schema dos Dados
transacoes = StructType([
            StructField("imovel_id", IntegerType(), False),             #Identificador do imóvel transacionado
            StructField("morador_id", IntegerType(), False),            #Identificador do morador que realizou a transação
            StructField("data_transacao", DateType(), False),           #Data da transação
            StructField("valor_transacao", DecimalType(15,2), False),   #Valor da transação
        ])

moradores = StructType([
            StructField("nome", StringType(), nullable=False),              #Nome do morador
            StructField("condominio_id", IntegerType(), nullable=False),    #Identificador do condomínio onde mora
            StructField("data_registro", DateType(), nullable=False),       #Data de registro do morador
        ])

imoveis = StructType([
            StructField("tipo", StringType(), False),                   #Tipo de imóvel (ex: apartamento, casa)
            StructField("condominio_id", IntegerType(), False),         #Identificador do condomínio onde o imóvel está localizado
            StructField("valor", DecimalType(15, 2), False),            #Valor do imóvel
        ])

condominios = StructType([
            StructField("nome", StringType(), False),                   #Nome do condomínio
            StructField("endereco", StringType(), False),               #Endereço do condomínio
        ])

# Ditório do projeto
SRC_DIR = os.path.dirname(os.path.abspath(__file__))


if os.path.exists(f"{SRC_DIR}/../jar-files/postgresql-42.7.3.jar"):     
    # SparkSession
    # Será criado apenas com a existencia do arquivo: postgresql-42.7.3.jar
    spark = SparkSession.builder \
        .appName("SPARK Structured Streaming") \
        .config("spark.jars", f"{SRC_DIR}/../jar-files/postgresql-42.7.3.jar") \
        .getOrCreate()
else:
    print(f'ALERT!! FILE NOT EXISTS: postgresql-42.7.3.jar')


# Diretórios: entrada, saída, erro e check
input_path = f"{SRC_DIR}/../data-files-enter"
output_path = f"{SRC_DIR}/../data-files-enter/output"
error_path = f"{SRC_DIR}/../data-files-enter/error"
check_path = f"{SRC_DIR}/../data-files-enter/check"
# Cria diretórios
if not os.path.exists(output_path): os.makedirs(output_path)
if not os.path.exists(error_path): os.makedirs(error_path)
if not os.path.exists(check_path): os.makedirs(check_path)
# Verifica diretórios
if not os.path.exists(input_path): print(f'ALERT!! NOT EXISTS folder: {input_path}')
if not os.path.exists(output_path): print(f'ALERT!! NOT EXISTS folder: {output_path}')
if not os.path.exists(error_path): print(f'ALERT!! NOT EXISTS folder: {error_path}')


#        "driver": "org.postgresql.Driver",
def getConnString():
    conn = {
        "user": "admin",
        "password": "admin",
        "database": "condomanage",
        "host": "localhost",
        "port": "5432",  
        "driver": "org.postgresql.Driver",
    }
    url = f"jdbc:postgresql://{conn['host']}:{conn['port']}/{conn['database']}"
    return conn, url

def getFilesNames(df:DataFrame):
    files_reads = df.select(input_file_name().alias("arquivo")).distinct().collect()
    files_names = []
    for file in files_reads: files_names.append(file["arquivo"].replace("file://", ""))
    print("> files_names:", files_names)
    return files_names

# Move arquivo
def moveFile(arquivo:str, destino:str):
    base_name = os.path.basename(arquivo)
    try:
        shutil.move(arquivo, os.path.join(destino, base_name))
        print(f"> Arquivo '{base_name}' movido para a pasta: '{destino}'")
    except Exception as e:
        print(f"> Erro ao mover o arquivo '{base_name}' para '{destino}': {e}")

def validSchema(cols_req, cols_table):
    #print("> batch_df.columns:",batch_df.columns)
    return all(item in cols_table for item in cols_req)

def appendDf(df:DataFrame,table_name:str):
    conn_properties, conn_url = getConnString()
    try:
        df.write.jdbc(url=conn_url, table=table_name, mode='append', properties=conn_properties)
        qtd = df.count()
        print(f"Foram inseridos {qtd} registros, tabela: '{table_name}'")
        return qtd
    except Exception as e:
        print(f"Falha no APPEND de registros: {e}")
        return 0

# Aplicação do processamento SINGLE SCHEMA
def batch_transacoes(batch_df:DataFrame, batch_id):
    print(f"> batch_transacoes: {batch_id}, rows:", batch_df.count())
    files_names = getFilesNames(batch_df)
    # Valida Schema
    colunas_esperadas = ["imovel_id","morador_id","data_transacao","valor_transacao"]
    if validSchema(colunas_esperadas,batch_df.columns):
        batch_df.show(5)
        # APPEND
        qtd_rows = appendDf(batch_df, 'transacoes')
        if qtd_rows>0:
            # Insert ok: Arquivos do batch são movidos para diretório de output
            for f in files_names: moveFile(f, output_path)
    else:
        # Colunas invalidas: Arquivos do batch são movidos para diretório de erro
        for f in files_names: moveFile(f, error_path)
    
def batch_moradores(batch_df:DataFrame, batch_id):
    print(f"> batch_moradores: {batch_id}, rows:", batch_df.count())
    files_names = getFilesNames(batch_df)
    # Valida Schema
    colunas_esperadas = ["nome","condominio_id","data_registro"]
    if validSchema(colunas_esperadas,batch_df.columns):
        batch_df.show(5)
        qtd_rows = appendDf(batch_df, 'moradores')
        if qtd_rows>0:
            # Insert ok: Arquivos do batch são movidos para diretório de output
            for f in files_names: moveFile(f, output_path)
    else:
        # Colunas invalidas: Arquivos do batch são movidos para diretório de erro
        for f in files_names: moveFile(f, error_path)

def batch_imoveis(batch_df:DataFrame, batch_id):
    print(f"> batch_imoveis: {batch_id}, rows:", batch_df.count())
    files_names = getFilesNames(batch_df)
   # Valida Schema
    colunas_esperadas = ["tipo","condominio_id","valor"]
    if validSchema(colunas_esperadas,batch_df.columns):
        batch_df.show(5)
        qtd_rows = appendDf(batch_df, 'imoveis')
        if qtd_rows>0:
            # Insert ok: Arquivos do batch são movidos para diretório de output
            for f in files_names: moveFile(f, output_path)
    else:
        # Colunas invalidas: Arquivos do batch são movidos para diretório de erro
        for f in files_names: moveFile(f, error_path)

def batch_condominimos(batch_df:DataFrame, batch_id):
    print(f"> batch_condominimos: {batch_id}, rows:", batch_df.count())
    files_names = getFilesNames(batch_df)
   # Valida Schema
    colunas_esperadas = ["nome","endereco"]
    if validSchema(colunas_esperadas,batch_df.columns):
        batch_df.show(5)
        qtd_rows = appendDf(batch_df, 'condominios')
        if qtd_rows>0:
            # Insert ok: Arquivos do batch são movidos para diretório de output
            for f in files_names: moveFile(f, output_path)
    else:
        # Colunas invalidas: Arquivos do batch são movidos para diretório de erro
        for f in files_names: moveFile(f, error_path)

# Leitura contínua de arquivos *.csv na pasta 
print('readStream')
raw_stream = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .option("sep", ",") \
    .option("path", input_path) \
    .option("pathGlobFilter", "*transacoes*.csv") \
    .schema(transacoes) \
    .load()

raw_stream2 = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .option("sep", ",") \
    .option("path", input_path) \
    .option("pathGlobFilter", "*moradores*.csv") \
    .schema(moradores) \
    .load()

raw_stream3 = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .option("sep", ",") \
    .option("path", input_path) \
    .option("pathGlobFilter", "*imoveis*.csv") \
    .schema(imoveis) \
    .load()

raw_stream4 = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .option("sep", ",") \
    .option("path", input_path) \
    .option("pathGlobFilter", "*condominios*.csv") \
    .schema(condominios) \
    .load()


#Permissão da pasta, caso necessário
#os.chmod(output_path, 0o755)


print('writeStream')
stream = raw_stream.writeStream \
    .format("console") \
    .foreachBatch(batch_transacoes) \
    .outputMode("append") \
    .option(check_path, output_path) \
    .start()

stream2 = raw_stream2.writeStream \
    .format("console") \
    .foreachBatch(batch_moradores) \
    .outputMode("append") \
    .option(check_path, output_path) \
    .start()

stream3 = raw_stream3.writeStream \
    .format("console") \
    .foreachBatch(batch_imoveis) \
    .outputMode("append") \
    .option(check_path, output_path) \
    .start()

stream4 = raw_stream4.writeStream \
    .format("console") \
    .foreachBatch(batch_condominimos) \
    .outputMode("append") \
    .option(check_path, output_path) \
    .start()

print('...listen...')
#.outputMode("append") \

# Mantem o Stream em execução
stream.awaitTermination()
stream2.awaitTermination()
stream3.awaitTermination()
stream4.awaitTermination()


""""
## TENTATIVAS DE FAZER MULTI SCHEMA
# Aplicação do processamento MULTI SCHEMA
def processar_batch_multi_schema(df, batch_id):
    # lista arquivos da para ""data-files-enter"
    print(f"> processar_batch: {batch_id}")
    # Capturar o nome dos arquivos processados
    files_reads = df.select(input_file_name().alias("arquivo")).distinct().collect()
    #arquivos_nomes = [row["arquivo"] for row in arquivos]
    print(f"> processar_batch DF.SHOW()")
    df.show()
    for file in files_reads:
        processed_df = None
        #arquivo_atual = os.path.join(input_path, arquivo)
        file_name = file["arquivo"].replace("file:", "")
        # Filtrar os dados que vieram deste arquivo específico
        df_file = df.filter(input_file_name() == file_name)
        df_file_prossed, df_file_table = setupSchema(df_file)
        print('> processar_batch: DF_ARQUIVO.show()')
        df_file_prossed.show(5)
        #df = readCsvToDf(arquivo_atual)
        # Schema dinamico para df_arquivo
        if df_file_prossed is DataFrame: 
            # .. append
            print(f'>> Fazer Append do arquivo na tabela: {df_file_table}')
            # Move arquivo para pasta de processados/output
            try:
                moveFile(f"{input_path}/{file_name}", output_path)
                print(f"> Arquivo {file_name} movido para a pasta de outuput: {output_path}")
            except Exception as e:
                print(f"> Erro ao mover o arquivo {file_name} para erro: {e}")
        else:
            # Move arquivo para pasta de erros
            try:
                moveFile(f"{input_path}/{file_name}", error_path)
                print(f"> Arquivo {file_name} movido para a pasta de erro: {error_path}")
            except Exception as e:
                print(f"> Erro ao mover o arquivo {file_name} para erro: {e}")

# Função para determinar o schema do DataFrame
def setupSchema(df:DataFrame):
    if "endereco" in df.columns:
        return df.select("nome","endereco"), "condominio"
    elif "valor" in df.columns:
        return df.select("tipo","condominio_id","valor"), "imoveis"
    elif "data_registro" in df.columns:
        return df.select("nome","condominio_id","data_registro"), "moradores"
    elif "valor_transacao" in df.columns:
        return df.select("imovel_id","morador_id","data_transacao","valor_transacao"), "transacoes"
    else:
        return None, None
"""