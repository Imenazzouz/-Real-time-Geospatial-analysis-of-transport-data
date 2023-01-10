import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col



#initate spark session
elastic_host = "http://127.0.0.1:9200"
elastic_index = "test"
spark = SparkSession \
.builder \
    .master("local[*]")\
    .appName("test") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("es.nodes", "127.0.0.1")
spark.conf.set("spark.es.port", "9200")
spark.conf.set("es.nodes.wan.only", "true")

df = spark.read.csv("C:\\Users\\ASUS\\OneDrive\\Documents\\test.csv\\batch8.csv")
df = df.withColumnRenamed('_c0','trip')
df = df.withColumnRenamed('_c1','longitude')
df = df.withColumnRenamed('_c2','latitude')
df4=df
df.show(10)

#convert long and lat to floats
df.withColumn("longitude",df.longitude.cast("float"))
df.withColumn("latitude",df.longitude.cast("float"))


# calculate the distance in km between two different points
def haversine_distance(lat1, lon1, lat2, lon2):
   r = 6371
   phi1 = np.radians(lat1)
   phi2 = np.radians(lat2)
   delta_phi = np.radians(lat2 -lat1)
   delta_lambda = np.radians(lon2 - lon1)
   a = np.sin(delta_phi / 2)**2 + np.cos(phi1) * np.cos(phi2) *   np.sin(delta_lambda / 2)**2
   res = r * (2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))
   return np.round(res, 2)




#get all trips
liste_trip = []
df4 = df4.distinct().toPandas()
for row in df4.itertuples(index=False):
        liste_trip.append(row.trip)
liste_trip = [*set(liste_trip)]
new_lT=[]
for element in liste_trip:
        new_lT.append(element)
print(new_lT)

#select the concerned colums with  calculations
test = df.select(col("trip"),col("latitude"),col("longitude"))
test_trip=test.toPandas()
start= {}
f=0
# collect all the possible points travelled by a trip in a dict of trips
for trip in new_lT :
    l = []

    tt = test_trip.loc[test_trip['trip'] == trip]
    for i in tt.index:
        lat1 =tt["latitude"][i]
        long1=tt["longitude"][i]
        l.append(lat1)
        l.append(long1)

    start[(trip,"trip",f)]=l
    f+=1

# calculate the total distance travelled by a trip based on the collected points

print(start)
dict_distance=[]
k=0
dc=[]
for key,value in start.items():
    tot = 0.0
    print(key)
    print(len(value))
    for j in range(len(value)-3):
        k=k+1
        lat1 = float(value[j])
        long1= float(value[j+1])
        lat2= float(value[j+2])
        long2= float(value[j+3])
        dist= haversine_distance(lat1,long1,lat2,long2)
        dc.append(dist)
        tot+=dist
    if float(tot) != 0 :
        dict_distance.append({'trip':key[0],'tot distance':float(tot)})


print(dict_distance)
new_list= pd.DataFrame(dict_distance)



# Save the output in elasticsearch index
df_pyspark = spark.createDataFrame(new_list,['trip','total distance (km)'] )

df_pyspark.show(50)
df_pyspark.write.format("org.elasticsearch.spark.sql") \
    .option("es.port", "9200")\
    .option("es.nodes","127.0.0.1")\
    .mode("append") \
    .save("distance")




