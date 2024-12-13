import os
import traceback
from pyspark.sql import SparkSession, functions as F
import psycopg2

# Module: Processamento e transformação

class DataProcessingLayer:
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

    def transformTransacoesCondominioTotal(self):
        """Relatório Total de transações por Condominio"""
        print("> transformTransacoesCondominioTotal")
        # Carregando TABELAS
        condominios_df = self.spark.read.jdbc(url=self.url, table="condominios", properties=self.properties).alias("condominios")
        imoveis_df = self.spark.read.jdbc(url=self.url, table="imoveis", properties=self.properties).alias("imoveis")
        transacoes_df = self.spark.read.jdbc(url=self.url, table="transacoes", properties=self.properties).alias("transacoes")
        transacoes_condominio = transacoes_df \
        .join(imoveis_df, transacoes_df.imovel_id == imoveis_df.imovel_id) \
        .join(condominios_df, imoveis_df.condominio_id == condominios_df.condominio_id) \
        .groupBy("condominios.condominio_id", "condominios.nome") \
        .agg(F.sum("valor_transacao").alias("vl_transacoes")) \
        .orderBy("vl_transacoes", ascending=False)
        transacoes_condominio.show()
        return transacoes_condominio
    
    def transformMoradorTransacoesTotal(self):
        """Relatório Total de transações por Morador"""
        print("> transformMoradorTransacoesTotal")
        # Carregando TABELAS
        moradores_df = self.spark.read.jdbc(url=self.url, table="moradores", properties=self.properties).alias("moradores")
        transacoes_df = self.spark.read.jdbc(url=self.url, table="transacoes", properties=self.properties).alias("transacoes")
        # 3.2. Total de transações por morador
        moradores_df = self.spark.read.jdbc(url=self.url, table="moradores", properties=self.properties)
        transacoes_df = self.spark.read.jdbc(url=self.url, table="transacoes", properties=self.properties)
        morador_transacoes = transacoes_df \
        .join(moradores_df, transacoes_df.morador_id == moradores_df.morador_id) \
        .groupBy("nome") \
        .agg(F.sum("valor_transacao").alias("vl_transacoes")) \
        .orderBy("vl_transacoes", ascending=False)
        morador_transacoes.show()
        return morador_transacoes

    def transformTipoImovelTransacoesDia(self):
        """Relatório Total de transações por Imovel por Dia"""
        print('> transformTipoImovelTransacoesDia')
        # Carregando TABELAS
        imoveis_df = self.spark.read.jdbc(url=self.url, table="imoveis", properties=self.properties).alias("imoveis")
        transacoes_df = self.spark.read.jdbc(url=self.url, table="transacoes", properties=self.properties).alias("transacoes")
        # 3.3. Total de transações por tipo de imóvel
        tipo_imovel_transacoes = transacoes_df \
        .join(imoveis_df, transacoes_df.imovel_id == imoveis_df.imovel_id) \
        .groupBy("tipo") \
        .agg(F.sum("valor_transacao").alias("vl_transacoes")) \
        .orderBy("vl_transacoes", ascending=False)
        tipo_imovel_transacoes.show()
        return tipo_imovel_transacoes




    #def __init__(self, ingestion_layer: DataIngestionLayer, storage_layer: DataStorageLayer):
    #    self.ingestion_layer = ingestion_layer
    #    self.storage_layer = storage_layer
    #
    #def execute_pipeline(self, csv_path: str, table_name: str, query: str, parquet_path: str):
    #    # Step 1: Ingest CSV to PostgreSQL
    #    self.ingestion_layer.ingest_csv_to_postgres(csv_path, table_name)
    #    
    #    # Step 2: Save Query Result as Parquet
    #    self.storage_layer.save_query_as_parquet(query, parquet_path)
    #    
    #    # Step 3: Aggregate Total Records
    #    total_records = self.storage_layer.aggregate_table_count(table_name)
    #    print(f"Total records in {table_name}: {total_records}")



class ObjectMapperAll(object):

    SQL_PARAM_SYMBOL = "?"

    def __init__(self, entity):
        self.entity = entity

    def _get_pairs(self):
        return [(k,str(v)) for k,v in self.entity.__dict__.iteritems() if not k.startswith("_") and not k.startswith("__")]

    def _get_names(self):
        return [pair[0] for pair in self._get_pairs()]

    def _get_values(self):
        return [pair[1] for pair in self._get_pairs()]

    def _get_names_values(self):
        return self._get_names(), self._get_values()

    def insert(self):
        names, values = self._get_names_values()
        names = "%s" % ",".join(names)
        params = ",".join(["'?'" for x in values])
        return ("INSERT INTO %s(%s) VALUES (%s)" % (self.get_table(), names, params), ) + tuple(values)

    def update(self, id):
        pairs = self._get_pairs()
        fields = ", ".join(["%s = '%s'" % (k, self.SQL_PARAM_SYMBOL) for k,v in pairs])
        values = self._get_values()
        return ("UPDATE %s SET %s WHERE %s = %s" % (self.get_table(), fields, self.get_id(), self.SQL_PARAM_SYMBOL), ) + tuple(values) + (id, )

    def delete(self, id):
        return ("DELETE FROM %s WHERE %s = %s" % (self.get_table(), self.get_id(), self.SQL_PARAM_SYMBOL), id)

    def get_all(self):
        names = ", ".join(self._get_names())
        return "SELECT %s FROM %s" % (names, self.get_table())

    def get_by_id(self, id):
        names = ", ".join(self._get_names())
        return ("SELECT %s FROM %s WHERE %s = %s" % (names, self.get_table(), self.get_id(), self.SQL_PARAM_SYMBOL), id)

    #Overridables

    #You can Extend from this class and override the following methods in order to configurate
    #the table name and the id_name

    def get_table(self):
        return self.entity.__class__.__name__.lower()

    def get_id(self):
        return "id_%s" % self.entity.__class__.__name__.lower()