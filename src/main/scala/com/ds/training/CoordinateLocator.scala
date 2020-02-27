package com.ds.training

import java.io.FileInputStream
import java.util.Properties

import com.ds.training.Constant._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object CoordinateLocator extends Serializable {

  def checkpointDir: String = java.nio.file.Files.createTempDirectory(this.getClass.getSimpleName).toUri.toString

  val rootLogger: Logger = Logger.getLogger("CoordinateLocator")

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    Logger.getLogger("com.ridecell.data").setLevel(Level.INFO)

    val argsMap: Map[String, String] = collection.immutable.HashMap(
      Constant.PROPS_PATH -> args(0)
    )
    val propertyFileName = argsMap(Constant.PROPS_PATH)

    try {
      val spark: SparkSession = SparkSession
        .builder()
        .config("spark.sql.streaming.checkpointLocation", checkpointDir)
        .getOrCreate()
      spark.conf.set("spark.sql.crossJoin.enabled", "true")
      val props = new AppProperties(new FileInputStream(propertyFileName))

      val airportDf: DataFrame = spark.read.csv(props.get(AIRPORT_CSV_PATH))
        .withColumnRenamed("_c0", AIRPORT_ID_COLUMN)
        .withColumnRenamed("_c1", AIRPORT_LAT_COLUMN)
        .withColumnRenamed("_c2", AIRPORT_LNG_COLUMN)
        .cache()
      //        .withWatermark("airportTimestamp", "10 seconds ")


      val userDf: DataFrame = spark.readStream.format("kafka")
        .option("subscribe", props.get(Constant.USER_TOPIC_NAME))
        .option("kafka.bootstrap.servers", props.get(Constant.KAFKA_BROKERS))
        .load()
        .select(split(col("value"), ",").as("valueArr"), col("timestamp").as("userTimestamp"))
        .withColumn(USER_ID_COLUMN, col("valueArr").getItem(0))
        .withColumn(USER_LAT_COLUMN, col("valueArr").getItem(1))
        .withColumn(USER_LNG_COLUMN, col("valueArr").getItem(2))
      //        .withWatermark("userTimestamp", "10 seconds ")

      val kafkaProducerProps = new Properties()
      kafkaProducerProps.put("bootstrap.servers", props.get(KAFKA_BROKERS))
      kafkaProducerProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
      kafkaProducerProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
      val outputTopic = props.get(Constant.RESULT_TOPIC_NAME)
      val kafkaPush = userDf
        .writeStream
        .option("checkpointLocation", spark.conf.get("spark.sql.streaming.checkpointLocation"))
        .foreachBatch((userDataSet: Dataset[Row], batchId: Long) => {
          rootLogger.info(s"start batchId=${batchId} count=${userDataSet.count()}")
          val userTemp = userDataSet
            .withColumn(USER_ID_COLUMN, col("valueArr").getItem(0))
            .withColumn(USER_LAT_COLUMN, col("valueArr").getItem(1))
            .withColumn(USER_LNG_COLUMN, col("valueArr").getItem(2))

          val result = stitchDataForMinDistance(userTemp,airportDf)
          result.foreach(r => {
            rootLogger.info(s"airportId=${r.getAs[String](0)} userId=${r.getAs[String](1)} distance=${r.getAs[String](2)}")
            val rec = new ProducerRecord[String, String](outputTopic,r.getAs[String](0).concat(",").concat(r.getAs[String](1)))
            val producer = new KafkaProducer[String, String](kafkaProducerProps)
            val o = producer.send(rec)
          })
          rootLogger.info(s"end  batchId=${batchId}")
        })
        .start()

      kafkaPush.awaitTermination()
    }
    finally {
    }
  }

  def stitchDataForMinDistance(userDf: DataFrame, airportDf: DataFrame, map: Map[String, String] = Map()) = {
    val t0 = System.nanoTime()

    val df2 = if (map.nonEmpty) {
      userDf.filter(col(USER_ID_COLUMN)
        .isin(map.keys.toList: _*))
    } else {
      userDf
    }

    val t1 = System.nanoTime()
    rootLogger.debug(s"Elapsed time: ${t1 - t0} ns")

    val airport = airportDf
      .join(df2)
      .withColumn("a",
        pow(sin(radians(col(AIRPORT_LAT_COLUMN) - col(USER_LAT_COLUMN)) / 2), 2)
          + (cos(radians(col(USER_LAT_COLUMN))) * cos(radians(col(AIRPORT_LAT_COLUMN))) *
          pow(sin(radians(col(AIRPORT_LNG_COLUMN) - col(USER_LNG_COLUMN)) / 2), 2)))
      .withColumn("distance", atan2(sqrt("a"), sqrt(-col("a") + 1)) * 2 * EARTH_RADIUS)
    //      .withWatermark("timestamp","2 seconds")

    val t2 = System.nanoTime()
    rootLogger.debug(s"Elapsed time: ${t2 - t1} ns")

    val dfAgg = airport
      .groupBy(col(USER_ID_COLUMN))
      .agg(min("distance"))
      .withColumnRenamed(USER_ID_COLUMN, "userUUIDAgg")

    val t3 = System.nanoTime()
    rootLogger.debug(s"Elapsed time: ${t3 - t2} ns")

    val resultDF = dfAgg.join(airport, airport("distance") === dfAgg("min(distance)"), "inner")
      .select(USER_ID_COLUMN, AIRPORT_ID_COLUMN, "min(distance)")
      //      .withColumnRenamed(USER_ID_COLUMN, USER_ID_COLUMN)
      //      .withColumnRenamed("uuid", "airportId")
      .withColumnRenamed("min(distance)", "minDistance")
    //      .withWatermark("timestamp","1 seconds")

    val t4 = System.nanoTime()
    rootLogger.debug(s"Elapsed time::: ${t4 - t1} ns")

    resultDF
  }


}
