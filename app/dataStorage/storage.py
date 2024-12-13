import os
import traceback
from pyspark.sql import SparkSession, DataFrame
import psycopg2

# Module: data_storage_layer.py
class DataStorageLayer:
    def __init__(self, spark: SparkSession, **kwargs):
        # Set
        self.__dict__.update(kwargs)
        self.conn = None
        self.cursor = None
        self.spark = spark
        # Atributos requeridos
        required_fields = ['database', 'host', 'user', 'password', 'driver']
        for field in required_fields:
            if not hasattr(self, field):
                raise ValueError(f"Falta o atributo necessário: {field}")
        # Set url conn
        self.url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        # Set properties
        self.properties = {
            "driver": self.driver,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }
        
    def __enter__(self):
        """Abre a conexão com o PostgreSQL"""
        self.conn = psycopg2.connect(
            database=self.database,
            host=self.host,
            user=self.user,
            password=self.password
        )
        self.cursor = self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Mostra as falhas"""
        if exc_type:
            print(f"* Exceção capturada: {exc_type} - {exc_val}")
            print("* Traceback:", exc_tb)
            tb_lines = traceback.format_exception(exc_type, exc_val, exc_tb)
            for line in tb_lines: print(line, end='')
            return True  # Suprime a exceção
        """Fecha a conexão com o PostgreSQL"""
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()

    def write_to_parquet(self, dataframe:DataFrame, parquet_path: str):
        """DataFrame para Parquet"""
        print(f"> write_to_parquet: {parquet_path}")
        #dataframe.write.mode("overwrite").parquet(parquet_path)
        dataframe.write.parquet(parquet_path, mode="overwrite")