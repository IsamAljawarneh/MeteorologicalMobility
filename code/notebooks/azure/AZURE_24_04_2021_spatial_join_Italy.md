

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


```scala
/**
 * @Description: Joining Italy mobility data (semi-synthetic) with neighborhoods (polygons)
 * N.B. : part of MeteoMobility project
 * @author: Isam Al Jawarneh
 * @date: 02/02/2019
 * @last update: 25/04/2021
 */
```


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


```scala
var precision = 30

```


```scala
/*
here attention, longitude, latitude!
*/
val schemaBO = StructType(Array(
StructField("id", LongType, false),
StructField("data_id", IntegerType, false),
    StructField("received_timestamp", StringType, false),
    StructField("accuracy", DoubleType, false),
    StructField("latitude", DoubleType, false),
    StructField("longitude", DoubleType, false),
    StructField("provider", StringType, false),
    StructField("user_id", IntegerType, false),
    StructField("sample_timestamp", StringType, false)))
```


```scala
val geohashUDF = udf{(curve: Seq[ZOrderCurve]) => curve.map(_.toBase32())}
```

## we first explode the mobility data into the corresponding geohashes
***
**
preparing it thus for the spatial join job, by utilizing the filter-refine (a.k.a. true-hit filtering) approach
**
***


```scala
val points = spark.read.format("csv").option("header", "true").schema(schemaBO).csv("wasb://sspark@7q6kgdctotuwu.blob.core.windows.net/bo/dat/").withColumn("point", point($"longitude",$"latitude")).select("point","id")//.limit(20000)
val ridesGeohashed1 = points.withColumn("index", $"point" index  precision).withColumn("geohashArray", geohashUDF($"index.curve"))
val explodedRidesGeohashed1 = ridesGeohashed1.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }
```


```scala
explodedRidesGeohashed1.show(2)
```

## we then explode Bologna geojson neighborhood file
***
**
to get the geohash covering of Bologna
**
***


```scala
//bologna geojson
val rawNeighborhoodsBO = spark.sqlContext.read.format("magellan").option("type", "geojson").load("wasb://sspark@7q6kgdctotuwu.blob.core.windows.net/bo/bolognaQuartiere").select($"polygon", $"metadata"("NOMEQUART").as("neighborhood"))

val neighborhoodsBO = rawNeighborhoodsBO.withColumn("index", $"polygon" index precision).select($"polygon", $"index", 
      $"neighborhood")

val zorderIndexedNeighborhoodsBO = neighborhoodsBO.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation","neighborhood")
val geohashedNeighborhoodsBO= neighborhoodsBO.withColumn("geohashArray", geohashUDF($"index.curve"))

val explodedgeohashedNeighborhoodsBO = geohashedNeighborhoodsBO.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }

/////////////////////////

```


```scala
explodedgeohashedNeighborhoodsBO.count()
```


```scala
explodedgeohashedNeighborhoodsBO.select("*").where(explodedgeohashedNeighborhoodsBO("geohash")==="spzvpt").show()
```


```scala
explodedgeohashedNeighborhoodsBO.show(5)
```

## we then explode Italy geojson neighborhood file
***
**
to get the geohash covering of Italy
**
***


```scala
//italy geojson
val rawNeighborhoods = spark.sqlContext.read.format("magellan").option("type", "geojson").load("wasb://sspark@7q6kgdctotuwu.blob.core.windows.net/bo/italy/").select($"polygon", $"metadata"("name").as("neighborhood")).cache()

val neighborhoods = rawNeighborhoods.withColumn("index", $"polygon" index  precision).select($"polygon", $"index", 
      $"neighborhood").cache()

val zorderIndexedNeighborhoods = neighborhoods.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation","neighborhood")
val geohashedNeighborhoods= neighborhoods.withColumn("geohashArray", geohashUDF($"index.curve"))

val explodedgeohashedNeighborhoods = geohashedNeighborhoods.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }


/////////////////////////



```


```scala
explodedgeohashedNeighborhoods.show(1)
```

## we then exclude bologna covering from the geohash covery of Italy neighborhoods
***
**
we do so because the mobility data are not all in Bologna
the geojson file of Bologna contains all the neighborhoods
while the geojson file of Italy contains only cities (covering of cities)
**
***


```scala
val newexplodedgeohashedNeighborhoods = explodedgeohashedNeighborhoods.filter(col("neighborhood") =!= "Bologna")
```


```scala
explodedgeohashedNeighborhoods.count()
```

## we union both neighborhood data
***
**
to get the geohash covering of all Italy, including bologna on a granular level (neigborhoods of Bologna == 40)
**
***


```scala
val allNeigborhoods = newexplodedgeohashedNeighborhoods.union(explodedgeohashedNeighborhoodsBO)
```


```scala
allNeigborhoods.count()
```


```scala
explodedgeohashedNeighborhoods.show(2)
```


```scala
explodedgeohashedNeighborhoods.select("*").where(explodedgeohashedNeighborhoods("geohash")==="spzvpt").show()
```

## now we perform the spatial join
***
**
using filter-refine approach:
first: filter --> performing the MBR-join, using the geohash paired with geohash covering, a cheap operation
second: the refine step, where we ensure wether candidate tuples fall in real geometries within a specific neighborhood or not!
**
***


```scala
val rawTripsJoinedBSO = explodedRidesGeohashed1.join(allNeigborhoods, explodedRidesGeohashed1("geohash") === allNeigborhoods("geohash")).select("point", "neighborhood","id").where($"point" within $"polygon")
```


```scala
rawTripsJoinedBSO.count()
```

## we do aggregations
***
**
Top-K spatial query to check which neighborhoods have more dense mobility data
**
***


```scala
rawTripsJoinedBSO.select("*").groupBy(col("neighborhood")).agg(count("*").as("count")).sort(desc("count")).show(20)
```

## DONE!


```scala

```
