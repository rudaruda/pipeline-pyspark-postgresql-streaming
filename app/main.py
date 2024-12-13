import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql import functions as F

from dataIngest import ingest
from dataProcess import process
from dataStorage import storage
# Directory structure for a Python project
# project/
#   data_ingestion_layer.py
#   data_storage_layer.py
#   data_processing_layer.py
#   data_serving_layer.py
#   main.py

SRC_DIR = os.path.dirname(os.path.abspath(__file__))

def ingestao(spark, pstg_setup):
    # Ingestão
    etl = ingest.DataIngestionLayer(spark, **pstg_setup) 
    etl.__enter__()
    etl.table_recreate(['condominios', 'imoveis', 'moradores', 'transacoes'])
    etl.ingest_csv_to_postgres("/Users/rudaruda/Documents/Repos/superlogica_teste/app/data-files/condominios.csv", "condominios")
    etl.ingest_csv_to_postgres("/Users/rudaruda/Documents/Repos/superlogica_teste/app/data-files/moradores.csv", "moradores")
    etl.ingest_csv_to_postgres("/Users/rudaruda/Documents/Repos/superlogica_teste/app/data-files/imoveis.csv", "imoveis")
    etl.ingest_csv_to_postgres("/Users/rudaruda/Documents/Repos/superlogica_teste/app/data-files/transacoes.csv", "transacoes")
    etl.__exit__(None, None, None)

def transformacao(spark, pstg_setup):
    # Transformation
    elt = process.DataProcessingLayer(spark, **pstg_setup)
    elt.__enter__()
    df1 = elt.transformTransacoesCondominioTotal()
    df2 = elt.transformMoradorTransacoesTotal()
    df3 = elt.transformTipoImovelTransacoesDia()
    elt.__exit__(None, None, None)
    return df1, df2, df3

def armazenamento(spark, pstg_setup, df1:DataFrame, df2:DataFrame, df3:DataFrame):
    # Storage
    stg = storage.DataStorageLayer(spark, **pstg_setup)
    stg.__enter__()
    stg.write_to_parquet(df1, f"{SRC_DIR}/../parquet-files/TransacoesCondominio")
    stg.write_to_parquet(df2, f"{SRC_DIR}/../parquet-files/MoradorTransacoes")
    stg.write_to_parquet(df3, f"{SRC_DIR}/../TipoImovelTransacoes")
    stg.__exit__(None, None, None)


# Module: main.py
def main(arg:str):
    # PARA EXECUTAR A PIPELINE basta executar o comando:
    # python app/main.py pipeline
    # enviando o argumento "pipeline"

    if args == 'pipeline': print('> PIPELINE start...')

    # Spark session
    print('Spark session')
    spark = SparkSession.builder \
        .appName("SPARK_POSTGRE") \
        .config("spark.jars", f"{SRC_DIR}/jar-files/postgresql-42.7.3.jar") \
        .getOrCreate()
    
    # PostgreSQL Setup
    pstg_setup = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver",
        "database": "condomanage",
        "host": "localhost",
        "port": "5432",        
    }

    df1,df2,df3 = [None]*3

    if arg in ['ingestao','etl','pipeline','pipe']:
        ingestao(spark, pstg_setup)

    if arg in ['transformacao','elt','pipeline','pipe'] or arg in ['storage','armazenamento','arm']:
        df1, df2, df3 = transformacao(spark, pstg_setup)

    if arg in ['storage','armazenamento','arm','pipeline','pipe']:
        armazenamento(spark, pstg_setup, df1, df2, df3)

    
    #with ingest.DataIngestionLayer(spark=spark, **pstg_setup) as db:
    #    db.teste()

    if args == 'pipeline': print('> PIPELINE EXECUTADA COM SUCESSO!!')
    
    # Parando a SparkSession
    spark.stop()


if __name__ == "__main__":
    args = "".join(sys.argv[1:])
    print(f"args:'{args}'")
    print("Irá executar o main()")
    main(str(args))

