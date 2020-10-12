
'''
./bin/pyspark \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions
'''
import sys
import pyspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,max,min
from pyspark import SparkConf
def rename_columns(df, columns):
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    else:
        raise ValueError('''columns should be a dict, like
        {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}''')
def geo_to_LAT(point):
    raw = point.split()
    LAT = float(raw[0][6:])
    return LAT
def geo_to_LONG(point):
    raw = point.split()
    LONG = float(raw[1][:-1])
    return LONG
import sys
def main():

    conf = SparkConf(True) \
    .set("spark.cassandra.connection.host", "172.17.0.4") \
    .set("spark.sql.extensions","com.datastax.spark.connector.CassandraSparkExtensions")\
    .setAppName('cassandra-spark addon')

    session = SparkSession.builder \
    .config(conf=conf).getOrCreate()

    raw_lighting_data = session.read \
    .format("org.apache.spark.sql.cassandra")\
    .options(table="year1987", keyspace="testinsertdata")\
    .load()

    point_to_LAT = udf(geo_to_LAT, FloatType())
    point_to_LONG = udf(geo_to_LONG, FloatType())
    data_update_geo = raw_lighting_data.withColumn("LAT",point_to_LAT('longlat'))\
            .withColumn("LONG", point_to_LONG('longlat'))

    # [('date', 'date'), ('longlat', 'string'), ('number_of_strikes', 'int'), ('LAT', 'string'), ('LONG', 'string')]
    renameDict = {'max(LAT)':'max_Latitude','min(LAT)':'min_Latitude'}
    MinMaxLAT = data_update_geo.select(max("LAT"),min("LAT"))
    MinMaxLAT = rename_columns(MinMaxLAT, renameDict)

    MinMaxLAT.show()

if __name__ == "__main__":
    sys.exit(main())