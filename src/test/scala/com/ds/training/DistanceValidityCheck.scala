package com.ds.training

import com.ds.training.CoordinateLocator.rootLogger

import scala.io.Source

import com.ds.training.Constant._

object DistanceValidityCheck {

  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371

  def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Double = {

    val a = (Math.sin(Math.toRadians(userLocation.lat - warehouseLocation.lat) / 2) * Math.sin(Math.toRadians(userLocation.lat - warehouseLocation.lat) / 2)) +
      (Math.cos(Math.toRadians(userLocation.lat)) * Math.cos(Math.toRadians(warehouseLocation.lat)) * Math.sin(Math.toRadians(userLocation.lon - warehouseLocation.lon) / 2) * Math.sin(Math.toRadians(userLocation.lon - warehouseLocation.lon) / 2))

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    (AVERAGE_RADIUS_OF_EARTH_KM * c).toDouble

  }

  def main(args: Array[String]): Unit = {
    val diss = calculateDistanceInKilometer(Location(-37.83330154418945, 145.0500030517578),
      Location(-37.975833,145.102222)) //MBW
//      Location(39.45527,-31.13136)) FLW
    val airportSrc = Source.fromFile(AIRPORT_DATA_FILE_PATH)
    val airportList = airportSrc.getLines().drop(1).map(line => {
      val airportData = line.split(",")
      airportData
    })

    val userSrc = Source.fromFile(USER_DATA_FILE_PATH)
    userSrc.getLines().drop(1).foreach(line => {
      if (
//        line.contains("DDEFEBEA-98ED-49EB-A4E7-9D7BFDB7AA0B")
        line.contains("DAEF2221-14BE-467B-894A-F101CDCC38E4")
//        line.contains("31971B3E-2F80-4F8D-86BA-1F2077DF36A2") ||
//        line.contains("1A29A45C-D560-43D8-ADAB-C2F0AD068FFE") ||
//        line.contains("A6EC281B-B8EC-465A-8933-F127472DB0A3")
        ) {
        val userData = line.split(",")
        UserLocation(userData(0), userData(1).toDouble, userData(2).toDouble)
          rootLogger.debug(s"user=${userData(0)}")
        val (dist,airport) = airportList.map(a => {
          (calculateDistanceInKilometer(Location(userData(1).toDouble, userData(2).toDouble), Location(a(1).toDouble, a(2).toDouble)), a(0))
        }).min

          rootLogger.debug(s"airport=${airport} dist=$dist")
//          rootLogger.debug(s"dist=$dist")
      }
    })

  }

}

case class Location(lat: Double, lon: Double)


