# Assignment-21.1


export JAVA_HOME=/usr/java/jdk1.8.0_161
pyspark2 --packages com.databricks:spark-avro_2.11:4.0.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import avro.schema
ssc = StreamingContext(sc, 30)
spark.sql("set spark.sql.streaming.schemaInference=true")
leavedf1 = sqlContext.readStream.format("com.databricks.spark.avro").load("/topicleave2")
leavedf1.createOrReplaceTempView("leave1_strm")

//innerjoin between strm and static df
leavedf2 = sqlContext.read.format("com.databricks.spark.avro").load("/topicleave2")
leavedf2.createOrReplaceTempView("leave2_strm")
query = spark.sql("select a.tenantid,count(b.leaveid) from leave1_strm a join leave2_strm b on a.tenantid=b.tenantid group by a.tenantid") \
                 .writeStream \
                 .format("console") \
                 .outputMode("complete") \
                 .start() 
query.awaitTermination()

//left outer join between strm and static df (multi left joins,where,
leavedf2 = sqlContext.read.format("com.databricks.spark.avro").load("/topicleave")
leavedf2.createOrReplaceTempView("leave2_strm")
query = spark.sql("select a.tenantid,count(b.leaveid) from leave1_strm a left join leave2_strm b on a.tenantid=b.tenantid left join leave2_strm c on a.tenantid=c.tenantid where c.tenantid in(92,64) group by a.tenantid") \
                 .writeStream \
                 .format("console") \
                 .outputMode("complete") \
                 .start() 
query.awaitTermination()
