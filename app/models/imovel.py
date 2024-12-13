from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType


class Imoveis:
    @staticmethod
    def get_schema():
        return StructType([
            StructField("imovel_id", IntegerType(), False),             #Identificador único do imóvel
            StructField("tipo", StringType(), False),                   #Tipo de imóvel (ex: apartamento, casa)
            StructField("condominio_id", IntegerType(), False),         #Identificador do condomínio onde o imóvel está localizado
            StructField("valor", DecimalType(15, 2), False),   #Valor do imóvel
        ])