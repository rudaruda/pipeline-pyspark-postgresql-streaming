from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


class Moradores:
    @staticmethod
    def get_schema():
        return StructType([
            StructField("morador_id", IntegerType(), nullable=False),       #Identificador único do morador
            StructField("nome", StringType(), nullable=False),              #Nome do morador
            StructField("condominio_id", IntegerType(), nullable=False),    #Identificador do condomínio onde mora
            StructField("data_registro", DateType(), nullable=False),       #Data de registro do morador
        ])