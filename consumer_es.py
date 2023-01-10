
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from timeloop import Timeloop
from datetime import timedelta
from pyspark.sql.functions import window
from pyspark.sql.types import StructField , StructType , StringType ,\
                DateType, ArrayType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, array
#elasticsearch connection
import test

elastic_host = "http://127.0.0.1:9200"
elastic_index = "test"


# Initiate spark session
spark = SparkSession \
.builder \
    .master("local[*]")\
    .appName("AppKafka") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
#Connect elasticsearch to spark
sc = spark.sparkContext
spark.sparkContext.setLogLevel('ERROR')
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("es.nodes", "127.0.0.1")
spark.conf.set("spark.es.port", "9200")
spark.conf.set("es.nodes.wan.only", "true")
#Connect spark to kafka topic : "mbta-msgs":
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'ubuntu:9092') \
    .option("subscribe", "mbta-msgs").load()


valueDf = df.selectExpr('timestamp', 'CAST(value AS STRING)')

#flatten {"event","data":{....}}  :
etape1 = StructType([StructField("event", StringType(), True), StructField("data", StringType(), True)])

#flatten {"id","links":{....},"attributes":{...},"type"}  :
etape2 = StructType([ StructField("id", StringType(), True),StructField("type", StringType(), True),StructField("attributes", StringType(), True),StructField("links", StringType(), True),StructField("relationships", StringType(), True)])

#transform step2 => Array
etape1_reset = ArrayType(etape2)
etape1Df = valueDf.select('timestamp', from_json(col("value"), etape1))
etape1Df= etape1Df.withColumnRenamed('from_json(value)', 'proc')
print('etape1DF schema')
etape1Df.printSchema()
flat_etape1_Df = etape1Df.selectExpr('timestamp', 'proc.event', 'proc.data')
flat_etape1_Df.printSchema()
#create dataframe for each event possible  value : reset , update or remove
resetEtapeDf = flat_etape1_Df.where(col('event') == 'reset')
updateEtapeDf = flat_etape1_Df.where(col('event') == 'update')
removeEtapeDf = flat_etape1_Df.where(col('event') == 'remove')

#flatten "data":{..}
etape2_Df = resetEtapeDf.select(col('timestamp'), col('event'), from_json(col("data"), etape1_reset)) \
    .withColumnRenamed('from_json(data)', 'proc') \
    .withColumn('exploded', explode(col('proc'))) \
    .select('timestamp', 'event', 'exploded').drop('data') \
    .withColumnRenamed('exploded', 'proc')
#dataframe1 : event = update
etape2UpdateDf = updateEtapeDf.select(col('timestamp'), col('event'), from_json(col("data"), etape2)) \
    .withColumnRenamed('from_json(value)', 'proc')
#dataframe2 : event = remove
etape2RemoveDf = updateEtapeDf.select(col('timestamp'), col('event'), from_json(col("data"), etape2)) \
    .withColumnRenamed('from_json(value)', 'proc')
#union dataframe 1 & dataframe 2
etape2Df = etape2_Df.union(etape2UpdateDf).union(etape2RemoveDf)

flat_etape2_Df = etape2Df.selectExpr('timestamp', 'event', 'proc.id as id', 'proc.type', 'proc.attributes','proc.links', 'proc.relationships')
#flatten "attributes":{"bearing,"current_status","label",...}
etape3_attributes = StructType([StructField("bearing", DoubleType(), True),StructField("current_status", StringType(), True),StructField("current_stop_sequence", IntegerType(), True),StructField("label", StringType(), True),StructField("latitude", DoubleType(), True),StructField("longitude", DoubleType(), True),StructField("speed", DoubleType(), True),StructField("updated_at", DateType(), True)])

#flaten {..,'relationships':{"route,"stop","trip"},..}
etape3_relations = StructType([StructField("route", StringType(), True),StructField("stop", StringType(), True),StructField("trip", StringType(), True)])

etape3_Df = flat_etape2_Df.select(col('timestamp'), col('event'), col('id'), col('type'),
                                        from_json(col('attributes'), etape3_attributes),
                                        from_json(col("relationships"), etape3_relations)) \
    .withColumnRenamed('from_json(attributes)', 'attrs') \
    .withColumnRenamed('from_json(relationships)', 'rels')


flat_etape3_Df = etape3_Df.selectExpr('timestamp', 'event', 'id', 'type',
                                           'attrs.bearing', 'attrs.current_status',
                                           'attrs.current_stop_sequence', 'attrs.label', 'attrs.latitude',
                                           'attrs.longitude', 'attrs.speed', 'attrs.updated_at',
                                           'rels.route', 'rels.stop', 'rels.trip')

etape4_data = StructType([StructField("data", StringType(), True)])

etape4_id = StructType([StructField("id", StringType(), True)])

etape4Df = flat_etape3_Df.select('*', from_json(col('route'), etape4_data),from_json(col('stop'), etape4_data),from_json(col('trip'), etape4_data)) \
    .withColumnRenamed('from_json(route)', 'routeproc') \
    .withColumnRenamed('from_json(stop)', 'stopproc') \
    .withColumnRenamed('from_json(trip)', 'tripproc') \
    .drop('route').drop('stop').drop('trip')

flat_etape4 = etape4Df.select('*',from_json(col('routeproc.data'), etape4_id),from_json(col('stopproc.data'), etape4_id),from_json(col('tripproc.data'), etape4_id)) \
    .withColumnRenamed('from_json(routeproc.data)', 'route') \
    .withColumnRenamed('from_json(stopproc.data)', 'stop') \
    .withColumnRenamed('from_json(tripproc.data)', 'trip') \
    .drop('routeproc').drop('stopproc').drop('tripproc')
#select all colums + create array[long,lat]
etape_finale = flat_etape4.selectExpr('timestamp', 'event', 'id', 'type', 'bearing','current_status', 'current_stop_sequence', 'label','latitude', 'longitude', 'speed','updated_at', 'route.id as route', 'stop.id as stop', 'trip.id as trip') \
    .withColumn('coordinates', array(col("longitude"), col("latitude")))

#Get flatened data
etape_finale = etape_finale.select(col('timestamp'), col('event'), col('id'), col('type'),col('bearing'),col('current_status'),
            col('current_stop_sequence'),col('label'),col('latitude'), col('longitude'), col('coordinates'),
            col('speed'),col('updated_at'), col('route'),col('stop'), col('trip'))


etape_finale.printSchema()


def stream_batches(dfb,batch):
          df=  dfb.write.format("org.elasticsearch.spark.sql") \
                .option("es.port", "9200") \
                .option("es.nodes", "127.0.0.1") \
                .mode("append") \
                .save("test")

write_df = etape_finale.writeStream.outputMode("update").foreachBatch(stream_batches).start()
stream_query1=etape_finale.groupBy('trip', 'longitude','latitude') \
    .count() \
    .writeStream \
    .queryName('test') \
    .outputMode("complete") \
    .format("memory") \
    .start()


stream_query = etape_finale.groupBy('current_status', window(col("timestamp"), "30 seconds", "15 seconds")) \
    .count() \
    .withColumnRenamed('current_status', 'id') \
    .writeStream \
    .queryName('mbta_msgs_stats') \
    .outputMode("complete") \
    .format("memory") \
    .start()


tl = Timeloop()


@tl.job(interval=timedelta(seconds=15))
def write_to_elasticsearch():
    df1 = spark.sql("""select * from test""")
    df1.show()
    df1.write.csv(path="C:\\Users\\ASUS\\OneDrive\\Documents\\test.csv", mode="append")



    df = spark.sql("""select * from mbta_msgs_stats 
                      where window = (select last(window) from 
                            (select * from mbta_msgs_stats where window.end < CURRENT_TIMESTAMP order by window))""")
    df.write.format("org.elasticsearch.spark.sql") \
    .option("es.port", "9200")\
    .option("es.nodes","127.0.0.1")\
    .mode("append") \
    .save("test")



    print("streaming from kafka")
tl.start(block=True)

write_df.awaitTermination()