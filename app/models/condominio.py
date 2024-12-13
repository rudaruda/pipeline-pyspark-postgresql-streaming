from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType


class Condominios:
    @staticmethod
    def get_schema():
        return StructType([
            StructField("condominio_id", IntegerType(), False), #Identificador único do condomínio
            StructField("nome", IntegerType(), False),          #Nome do condomínio
            StructField("endereco", StringType(), False),       #Endereço do condomínio
        ])


# Garantir que o nome não ultrapasse 255 caracteres
#data = [(id, nome[:255], endereco) for id, nome, endereco in data]
