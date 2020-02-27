package com.ds.training

import com.ds.training.Constant._
import com.ds.training.CoordinateLocator.rootLogger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
class CoordinateLocatorTest
  extends AnyFunSuite
    with BeforeAndAfterAll {

//  val USER_DATA_FILE_PATH = "/Users/dshingav/openSourceCode/CoordinatesComparison/src/main/resources/user-geo-sample.csv"
//  val AIRPORT_DATA_FILE_PATH = "/Users/dshingav/openSourceCode/CoordinatesComparison/src/main/resources/optd-airports-sample.csv"

  var sparkSession: SparkSession = _

  //uuid,geoip_latitude,geoip_longitude
  val airportDataSchema = StructType.apply(Seq(
    StructField("uuid", StringType, nullable = false),
    StructField("geoip_latitude", DoubleType, nullable = false),
    StructField("geoip_longitude", DoubleType, nullable = false))
  )

  val userDataSchema = StructType.apply(Seq(
    StructField("userUUID", StringType, nullable = false),
    StructField("userLat", DoubleType, nullable = false),
    StructField("userLng", DoubleType, nullable = false))
  )


  test("Test the input data schema") {
    val df = sparkSession.read.option("header", "true")
      .schema(airportDataSchema)
      .csv(AIRPORT_DATA_FILE_PATH)
    //    assert(df.schema == airportDataSchema)
    assert(df.count() == 6889)
  }

  //uuid,geoip_latitude,geoip_longitude
  test("Test the input user data schema") {
    val df = sparkSession.read.option("header", "true")
      .schema(airportDataSchema)
      .csv(AIRPORT_DATA_FILE_PATH)
    //    assert(df.schema == airportDataSchema)
    assert(df.count() == 6889)
  }

  test("Join the data") {

    val dfUser = sparkSession.read.option("header", "true")
      .schema(airportDataSchema)
      .csv(USER_DATA_FILE_PATH)

    val dfAirport = sparkSession.read.option("header", "true")
      .schema(airportDataSchema)
      .csv(AIRPORT_DATA_FILE_PATH)

    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")
    val map: Map[String, String] = Map("DDEFEBEA-98ED-49EB-A4E7-9D7BFDB7AA0B" -> "16.497695008157372",
      "DAEF2221-14BE-467B-894A-F101CDCC38E4" -> "23.812172200017695")

    val resultDF: DataFrame = CoordinateLocator.stitchDataForMinDistance(dfUser, dfAirport, map)
    /* .foreach(r => {
       rootLogger.debug("------------------------------------------------")
       rootLogger.debug(s"user=${r.getAs[String]("userUUID")}")
       rootLogger.debug(s"distance=${r.getAs[String]("min(distance)")}")
       rootLogger.debug("------------------------------------------------")
     })*/

    assert(resultDF.count() == 2)

    val rows = resultDF.collectAsList()
    val row1 = rows.get(0)
    val row2 = rows.get(1)

    rootLogger.debug("------------------------------------------------")
    rootLogger.debug(map(row1.getAs[String]("userId")))
    rootLogger.debug(row1.getAs[String]("minDistance"))
    rootLogger.debug("------------------------------------------------")
    rootLogger.debug(map(row2.getAs[String]("userId")))
    rootLogger.debug(row2.getAs[String]("minDistance"))
    rootLogger.debug("------------------------------------------------")

    assert(map(row1.getAs[String]("userId")).toDouble == row1.getAs[Double]("minDistance"))
    assert(map(row2.getAs[String]("userId")).toDouble == row2.getAs[Double]("minDistance"))


    /*.foreach(r => {
      rootLogger.debug("------------------------------------------------")
      rootLogger.debug(s"user=${r.getAs[String]("userUUID")}")
      rootLogger.debug(s"distance=${r.getAs[String]("min(distance)")}")
      rootLogger.debug("------------------------------------------------")
    })*/


    //    })

  }

  /*test("Schematize the incoming CSV data from Kafka")  {
    val df = sparkSession.readStream
        .schema(stdSchema)
      .csv(prefix + USER_DATA_DIR)

    df.writeStream.foreachBatch((ds: Dataset[Row], bId: Long) => {
      rootLogger.info(s"Start of $bId")
      ds.foreach((r : Row) => {
        rootLogger.info(r)
      })
      rootLogger.info(s"End  of $bId")
    }).start().awaitTermination()

//      .option("sep", ",")
  }

  test("Join the real time data") {

  }*/

  case class UserLocation(userUUID: String, userLat: Double, userLng: Double)

  override protected def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    sparkSession.close()
  }
}
