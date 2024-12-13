#from mapper import ObjectMapper
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DecimalType, MapType

class Transacoes:
    @staticmethod
    def get_schema():
        return StructType([
            StructField("transacao_id", IntegerType(), False),          #Identificador único da transação
            StructField("movel_id", IntegerType(), False),              #Identificador do imóvel transacionado
            StructField("morador_id", IntegerType(), False),            #Identificador do morador que realizou a transação
            StructField("data_transacao", DateType(), False),           #Data da transação
            StructField("valor_transacao", DecimalType(15,2), False),   #Valor da transação
        ])