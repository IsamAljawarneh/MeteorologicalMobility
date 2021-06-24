```scala
%%configure -f
{
    "conf": {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,harsha2010:magellan:1.0.5-s_2.11,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0,org.apache.spark:spark-sql_2.11:2.2.0",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11",
        "spark.dynamicAllocation.enabled": false
    }
}
```


Current session configs: <tt>{'conf': {'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,harsha2010:magellan:1.0.5-s_2.11,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0,org.apache.spark:spark-sql_2.11:2.2.0', 'spark.jars.excludes': 'org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11', 'spark.dynamicAllocation.enabled': False}, 'kind': 'spark'}</tt><br>



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>0</td><td>None</td><td>spark</td><td>dead</td><td></td><td></td><td></td></tr></table>



```scala
import util.control.Breaks._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.ForeachWriter
import magellan._
import magellan.index.ZOrderCurve
import magellan.{Point, Polygon}

import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{collect_list, collect_set}
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import java.io.{BufferedWriter, FileWriter}
import org.apache.commons.io.FileUtils
import java.io.File
import scala.collection.mutable.ListBuffer
import java.time.Instant
import org.apache.spark.util.CollectionAccumulator

```

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>1</td><td>None</td><td>spark</td><td>idle</td><td></td><td></td><td>✔</td></tr></table>



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    SparkSession available as 'spark'.



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    import util.control.Breaks._
    import org.apache.spark.sql.streaming.StreamingQueryListener
    import org.apache.spark.util.random.XORShiftRandom
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.types._
    import org.apache.spark.rdd.RDD
    import org.apache.spark.SparkContext
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.SQLImplicits
    import org.apache.spark.sql.functions.from_json
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.ForeachWriter
    import magellan._
    import magellan.index.ZOrderCurve
    import magellan.{Point, Polygon}
    import org.apache.spark.sql.magellan.dsl.expressions._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.streaming.OutputMode
    import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
    import org.apache.spark.sql.streaming._
    import org.apache.spark.sql.streaming.Trigger
    import org.apache.spark.sql.execution.streaming.MemoryStream
    import org.apache.spark.sql.functions.{collect_list, collect_set}
    import org.apache.spark.sql.SQLContext
    import org.apache.log4j.{Level, Logger}
    import scala.collection.mutable
    import scala.concurrent.duration.Duration
    import java.io.{BufferedWriter, FileWriter}
    import org.apache.commons.io.FileUtils
    import java.io.File
    import scala.collection.mutable.ListBuffer
    import java.time.Instant
    import org.apache.spark.util.CollectionAccumulator



```scala
var precision = 25
```


```scala
val geohashUDF = udf{(curve: Seq[ZOrderCurve]) => curve.map(_.toBase32())}

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    geohashUDF: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,ArrayType(StringType,true),Some(List(ArrayType(org.apache.spark.sql.types.ZOrderCurveUDT@210ca565,true))))



```scala
spark.version
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    res2: String = 2.2.0



```scala
var precision = 25


```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    precision: Int = 25



```scala
//bologna geojson
val rawNeighborhoodsBO = spark.sqlContext.read.format("magellan").option("type", "geojson").load("/home/isam/Desktop/Denis/5-5-2021/MeteoMobilityIntegration/data/BolognaQuartieri").select($"polygon", $"metadata"("NOMEQUART").as("neighborhood"))

val neighborhoodsBO = rawNeighborhoodsBO.withColumn("index", $"polygon" index precision).select($"polygon", $"index", 
      $"neighborhood")

val zorderIndexedNeighborhoodsBO = neighborhoodsBO.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation","neighborhood")
val geohashedNeighborhoodsBO= neighborhoodsBO.withColumn("geohashArray", geohashUDF($"index.curve"))

val explodedgeohashedNeighborhoodsBO = geohashedNeighborhoodsBO.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }

/////////////////////////


```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    rawNeighborhoodsBO: org.apache.spark.sql.DataFrame = [polygon: polygon, neighborhood: string]
    neighborhoodsBO: org.apache.spark.sql.DataFrame = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 1 more field]
    zorderIndexedNeighborhoodsBO: org.apache.spark.sql.DataFrame = [polygon: polygon, curve: zordercurve ... 2 more fields]
    geohashedNeighborhoodsBO: org.apache.spark.sql.DataFrame = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 2 more fields]
    warning: there was one deprecation warning; re-run with -deprecation for details
    explodedgeohashedNeighborhoodsBO: org.apache.spark.sql.DataFrame = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 3 more fields]



```scala
explodedgeohashedNeighborhoodsBO.count()

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    res12: Long = 40



```scala
explodedgeohashedNeighborhoodsBO.select("*").where(explodedgeohashedNeighborhoodsBO("geohash")==="spzvp").show()

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------------+--------------------+--------------+--------------------+-------+
    |             polygon|               index|  neighborhood|        geohashArray|geohash|
    +--------------------+--------------------+--------------+--------------------+-------+
    |magellan.Polygon@...|[[ZOrderCurve(11....|Borgo Panigale|[spzvp, spzvr, sr...|  spzvp|
    +--------------------+--------------------+--------------+--------------------+-------+
    



```scala
explodedgeohashedNeighborhoodsBO.show(5)

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------------+--------------------+--------------+--------------------+-------+
    |             polygon|               index|  neighborhood|        geohashArray|geohash|
    +--------------------+--------------------+--------------+--------------------+-------+
    |magellan.Polygon@...|[[ZOrderCurve(11....|Borgo Panigale|[spzvp, spzvr, sr...|  spzvp|
    |magellan.Polygon@...|[[ZOrderCurve(11....|Borgo Panigale|[spzvp, spzvr, sr...|  spzvr|
    |magellan.Polygon@...|[[ZOrderCurve(11....|Borgo Panigale|[spzvp, spzvr, sr...|  srbj0|
    |magellan.Polygon@...|[[ZOrderCurve(11....|Borgo Panigale|[spzvp, spzvr, sr...|  srbj2|
    |magellan.Polygon@...|[[ZOrderCurve(11....|Borgo Panigale|[spzvp, spzvr, sr...|  srbj1|
    +--------------------+--------------------+--------------+--------------------+-------+
    only showing top 5 rows
    



```scala
//italy geojson
val rawNeighborhoods = spark.sqlContext.read.format("magellan").option("type", "geojson").load("/home/isam/Desktop/Denis/5-5-2021/MeteoMobilityIntegration/data/italyQuartieri/").select($"polygon", $"metadata"("name").as("neighborhood")).cache()

val neighborhoods = rawNeighborhoods.withColumn("index", $"polygon" index  precision).select($"polygon", $"index", 
      $"neighborhood").cache()

val zorderIndexedNeighborhoods = neighborhoods.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation","neighborhood")
val geohashedNeighborhoods= neighborhoods.withColumn("geohashArray", geohashUDF($"index.curve"))

val explodedgeohashedNeighborhoods = geohashedNeighborhoods.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }


/////////////////////////




```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    rawNeighborhoods: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [polygon: polygon, neighborhood: string]
    neighborhoods: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 1 more field]
    zorderIndexedNeighborhoods: org.apache.spark.sql.DataFrame = [polygon: polygon, curve: zordercurve ... 2 more fields]
    geohashedNeighborhoods: org.apache.spark.sql.DataFrame = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 2 more fields]
    warning: there was one deprecation warning; re-run with -deprecation for details
    explodedgeohashedNeighborhoods: org.apache.spark.sql.DataFrame = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 3 more fields]



```scala
val newexplodedgeohashedNeighborhoods = explodedgeohashedNeighborhoods.filter(col("neighborhood") =!= "Bologna")

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    newexplodedgeohashedNeighborhoods: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 3 more fields]



```scala
explodedgeohashedNeighborhoods.count()

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    res24: Long = 57819



```scala
val allNeigborhoods = newexplodedgeohashedNeighborhoods.union(explodedgeohashedNeighborhoodsBO)

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    allNeigborhoods: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 3 more fields]



```scala
allNeigborhoods.count()

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    res25: Long = 57841



```scala
allNeigborhoods.show(2)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------------+--------------------+------------+--------------------+-------+
    |             polygon|               index|neighborhood|        geohashArray|geohash|
    +--------------------+--------------------+------------+--------------------+-------+
    |magellan.Polygon@...|[[ZOrderCurve(7.7...|       Agliè|[u0j9b, u0jd0, u0...|  u0j9b|
    |magellan.Polygon@...|[[ZOrderCurve(7.7...|       Agliè|[u0j9b, u0jd0, u0...|  u0jd0|
    +--------------------+--------------------+------------+--------------------+-------+
    only showing top 2 rows
    



```scala
explodedgeohashedNeighborhoods.show(2)

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------------+--------------------+------------+--------------------+-------+
    |             polygon|               index|neighborhood|        geohashArray|geohash|
    +--------------------+--------------------+------------+--------------------+-------+
    |magellan.Polygon@...|[[ZOrderCurve(7.7...|       Agliè|[u0j9b, u0jd0, u0...|  u0j9b|
    |magellan.Polygon@...|[[ZOrderCurve(7.7...|       Agliè|[u0j9b, u0jd0, u0...|  u0jd0|
    +--------------------+--------------------+------------+--------------------+-------+
    only showing top 2 rows
    



```scala
explodedgeohashedNeighborhoods.select("*").where(explodedgeohashedNeighborhoods("geohash")==="spzvp").show()

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------------+--------------------+-------------------+--------------------+-------+
    |             polygon|               index|       neighborhood|        geohashArray|geohash|
    +--------------------+--------------------+-------------------+--------------------+-------+
    |magellan.Polygon@...|[[ZOrderCurve(11....|            Bologna|[spzvp, spzvr, sr...|  spzvp|
    |magellan.Polygon@...|[[ZOrderCurve(11....|Casalecchio di Reno|[spzuz, spzvp, sr...|  spzvp|
    |magellan.Polygon@...|[[ZOrderCurve(11....|       Zola Predosa|[spzuy, spzvn, sp...|  spzvp|
    +--------------------+--------------------+-------------------+--------------------+-------+
    



```scala
val explodedgeohashedNeighborhoods_short = allNeigborhoods.select($"neighborhood",$"geohash")
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    explodedgeohashedNeighborhoods_short: org.apache.spark.sql.DataFrame = [neighborhood: string, geohash: string]



```scala
explodedgeohashedNeighborhoods_short.count
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    res34: Long = 57841



```scala
explodedgeohashedNeighborhoods_short.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("/home/isam/Desktop/Denis/5-5-2021/MeteoMobilityIntegration/data/explodedGeoItaly/")

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```scala

```
