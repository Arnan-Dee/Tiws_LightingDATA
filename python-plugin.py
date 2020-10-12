# pip install cassandra-driver
from cassandra.cluster import Cluster
import pyspark
from pyspark.sql.types import *
schemaFields = [StructField("date", DateType(), True),
                StructField("number_of_strikes",  IntegerType(), True),
                StructField("center_point_geom", StringType(), True)]
Schema = StructType(schemaFields)

def rename_columns(df, columns):
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    else:
        raise ValueError('''columns should be a dict, like
        {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}''')



cluster = Cluster(['172.17.0.4'])
session = cluster.connect()
session.set_keyspace("testinsertdata")
raw_lighting_data = session.execute("SELECT * FROM year1987")

def geo_to_LAT(point):
    raw = point.split()
    LAT = float(raw[0][6:])
    return LAT
def geo_to_LONG(point):
    raw = point.split()
    LONG = float(raw[1][:-1])
    return LONG

from pyspark.sql.functions import udf

point_to_LAT = udf(lambda point: geo_to_LAT(point), FloatType())
point_to_LONG = udf(lambda point: geo_to_LONG(point), FloatType())
data_update_geo = raw_lighting_data.withColumn("LAT",\
                  point_to_LAT(raw_lighting_data.center_point_geom))\
                  .withColumn("LONG", point_to_LONG(raw_lighting_data\
                  .center_point_geom))

from pyspark.sql.functions import max,min

renameDict = {'max(LAT)':'max_Latitude','min(LAT)':'min_Latitude'}
MinMaxLAT = data_update_geo.select(max("LAT"),min("LAT"))
MinMaxLAT = rename_columns(MinMaxLAT, renameDict)
MinMaxLAT.show()
