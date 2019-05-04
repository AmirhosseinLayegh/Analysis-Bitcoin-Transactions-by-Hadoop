import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)


#Defining Schema for vin
vin_schema = StructType([StructField("txid",StringType(),True), StructField("tx_hash",StringType(),True), StructField("vout", IntegerType(),True)])

#Loading vin to DataFrame
vin_df= sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/data/bitcoin/vin.csv', schema=vin_schema)
vin_df.registerTempTable('vin_df')

#Defining Schema for vout
vout_schema = StructType([StructField("hash",StringType(),True), StructField("value",FloatType(),True), StructField("n", IntegerType(),True), StructField('publicKey',StringType(),True)])

#Loading vout to a DataFrame
vout_df= sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/data/bitcoin/vout.csv', schema= vout_schema)
vout_df.registerTempTable('vout_df')

#Initial Filtering to get the specific wikileaks
vout_wkl_df=sqlContext.sql("SELECT hash, value, n, publicKey FROM vout_df WHERE publicKey= '{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}' ") #filter vout with specific address
vout_wkl_df.registerTempTable('vout_wkl_df')

#join of vin and vout_wkl_df
joined_df = sqlContext.sql("SELECT distinct vin_df.txid, vin_df.tx_hash, vin_df.vout FROM vin_df INNER JOIN vout_wkl_df ON vout_wkl_df.hash = vin_df.txid ")
joined_df.registerTempTable('joined_df')

#rejoin to vout_df
best_donors_df = sqlContext.sql("SELECT vout_df.publicKey, vout_df.value FROM joined_df INNER JOIN vout_df on vout_df.hash = joined_df.tx_hash and joined_df.vout = vout_df.n ORDER BY vout_df.value DESC limit 10")
best_donors_df.registerTempTable("best_donors_df")

#Save Output
encd='cp932'
best_donors_df.repartition(1).write.save(path='CW_PartB', format='csv', mode= 'overwrite', header= 'true', encoding= encd)
