import os
import traceback
from pyspark.sql import SparkSession
import psycopg2

# Modulo: Ingestão de Dados

class DataIngestionLayer:
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

    def table_create(self, table_name: str):
        """Cria tabela"""
        print(f"> table_create: {table_name}")
        query = ""
        
        if table_name == 'condominios':
            query = """
            CREATE TABLE IF NOT EXISTS condominios(
                condominio_id SERIAL PRIMARY KEY ,
                nome VARCHAR(255) NOT NULL,
                endereco TEXT NOT NULL
            )
            """
        elif table_name == 'imoveis':
            query = """
            CREATE TABLE IF NOT EXISTS imoveis (
                imovel_id SERIAL PRIMARY KEY ,
                tipo VARCHAR(50) NOT NULL,
                condominio_id INT NOT NULL,
                valor NUMERIC(15, 2) NOT NULL,
                FOREIGN KEY (condominio_id) REFERENCES condominios(condominio_id)
            )
            """
        elif table_name == 'moradores':
            query = """
            CREATE TABLE IF NOT EXISTS moradores (
            morador_id SERIAL PRIMARY KEY ,
            nome VARCHAR(255) NOT NULL,
            condominio_id INT NOT NULL,
            data_registro DATE NOT NULL,
            FOREIGN KEY (condominio_id) REFERENCES condominios(condominio_id)
            );
            """
        elif table_name == 'transacoes':
            query="""
            CREATE TABLE IF NOT EXISTS transacoes (
            transacao_id SERIAL PRIMARY KEY ,
            imovel_id INT NOT NULL,
            morador_id INT NOT NULL,
            data_transacao DATE NOT NULL,
            valor_transacao NUMERIC(15, 2) NOT NULL,
            FOREIGN KEY (imovel_id) REFERENCES imoveis(imovel_id),
            FOREIGN KEY (morador_id) REFERENCES moradores(morador_id)
            );
            """
        else:
            print("Nome de tabela invalido")
            return f"Nome de tabela invalido: {table_name}"
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print(f"Tabela '{table_name}' criada ou já existe.")
        except Exception as e:
            self.conn.rollback()
            print(f"Erro ao criar a tabela {table_name}: {e}")
    
    def table_recreate(self, table_name:list):
        """DROP em todas as tabelas e CREATE de todoas as tabelas"""
        print(f"> table_recreate:", ','.join(table_name))
        self.table_drop(table_name)
        self.table_create_all(table_name)

    def table_drop(self, table_name:list):
        """Deleta todos os dados tabela"""
        print('> table_truncate:', ','.join(table_name))
        for t in table_name:
            query = f"DROP TABLE {t} CASCADE;"
            try:
                self.cursor.execute(query)
                self.conn.commit()
                print(f"  Tabela '{t}' removida")
            except Exception as e:
                self.conn.rollback()
                print(f"  Erro ao remover a tabela {t}: {e}")

    def table_truncate(self, table_name:list):
        """Deleta todos os dados tabela"""
        print('> table_truncate:', ','.join(table_name))
        for t in table_name:
            query = f"TRUNCATE TABLE {t} CASCADE RESTART IDENTITY;"
            try:
                self.cursor.execute(query)
                self.conn.commit()
                print(f"  Tabela '{t}' todos os registros deletados")
            except Exception as e:
                self.conn.rollback()
                print(f"  Erro ao truncar a tabela {t}: {e}")
        
    def table_create_all(self, name_tables:list):
        """Realiza a criação de várias tabelas"""
        print('> table_create_all:',','.join(name_tables))
        for t in name_tables:
            try:
                self.table_create(t)
            except Exception as e:
                print(f"Erro ao criar a tabela {t}: {e}")
        print('  Criou todas as tabelas no PostgreSQL: ',','.join(name_tables))

    def ingest_csv_to_postgres(self, csv_path: str, table_name: str, mode:str='append'):
        print(f"> ingest_csv_to_postgres: {csv_path}")
        # Atributos requeridos
        required_fields = ['append', 'overwrite']
        if not mode in required_fields:
            raise ValueError(f"  Atributo 'mode' inválido: {mode}. Deve ser:", ' ou '.join(required_fields))
        # Read CSV
        print('  Read csv...')
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        #df.show()
        # Write in Database
        print('  Write in PostGreSQL')
        df.write.jdbc(url=self.url, table=table_name, mode=mode, properties=self.properties)
        print("    "+str(df.count())+" rows")

class ObjectMapperIngest(object):

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
    

"""
#jdbc_url = self.url
#df.write \
#  .format("jdbc") \
#  .option("url", jdbc_url) \
#  .option("dbtable", table_name) \
#  .option("user", self.user) \
#  .option("password", self.password) \
#  .option("driver", "org.postgresql.Driver") \
#  .mode("append") \
#  .save()
"""