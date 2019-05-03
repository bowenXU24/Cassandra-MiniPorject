import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._ 



val lines = sc.textFile("sparkinput/accesslog")
val colNames = new Array[String](9)

colNames(0)="ip"
colNames(1)="identity"
colNames(2)="username"
colNames(3)="time"
colNames(4)="zone"
colNames(5)="path"
colNames(6)="protocol"
colNames(7)="status"
colNames(8)="size"

val textrdd = lines.mapPartitionsWithIndex(
     (i,iterator)=>
     if(i==0 && iterator.hasNext){
     iterator.next
     iterator}
     else iterator)

val schema = StructType(colNames.map(fieldName => StructField(fieldName, StringType)))

val rowRDD = textrdd.map(_.split("\\s+").map(p=>Row(p: _*))

val data = spark.createDataFrame(rowRDD,schema)

val data = data.withColumn("id",monotonicallyIncreasingId)

data.write.format("csv").save("./csv/data")