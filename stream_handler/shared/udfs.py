from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

uuidDf = udf(lambda : str(uuid.uuid4()),StringType()).asNondeterministic()
