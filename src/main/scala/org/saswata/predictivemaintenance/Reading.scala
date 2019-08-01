package org.saswata.predictivemaintenance

import scala.util.matching.Regex

final case class Reading(
  sourceId: String,
  longitude: Option[Double],
  latitude: Option[Double],
  gpsTimestamp: Option[Long],
  speed: Option[Double],
  coolantAnalog: Option[Double],
  engineOilPressure: Option[Int]
) {

  def isValid: Boolean =
    Reading.validVehicle(sourceId) &&
    gpsTimestamp.exists(_ > 1e12.toLong) &&
    engineOilPressure.isDefined &&
    latitude.exists(it => it >= 7.0 && it <= 38.0) &&
    longitude.exists(it => it >= 68.0 && it <= 98.0)

  def asValidReading: Option[ValidReading] =
    if (isValid) Option(ValidReading(this)) else Option.empty[ValidReading]
}

object Reading {
  val VEHICLE_REGEX: Regex = "^[A-Z0-9]{3,}$".r

  def validVehicle(str: String): Boolean =
    VEHICLE_REGEX.pattern.matcher(str).matches
}
/*
    {
        "id": "",
        "sourceId": "HR55Y3199",
        "longitude": 76.4642816,
        "latitude": 30.562045,
        "locationDescription": "2.44 Kms from Village Sarae Banjanra-Sirhind-Patiala-Punjab",
        "gpsTimestamp": 156412 843 0 000,
        "timestamp": 1564128500000,
        "speed": 0,
        "ignStatus": "IGNOFF",
        "fuelValue": 527.21,
        "fuelTankOne": 232.4,
        "fuelTankTwo": 294.81,
        "coolantAnalog": 120,
        "engineOilPressure": 0,
        "airBrakePressure": 0,
        "rpm": null,
        "batteryLevel": 26.5,
        "messageType": "PVT",
        "xaxis": null,
        "yaxis": null,
        "zaxis": null
    },
 */
