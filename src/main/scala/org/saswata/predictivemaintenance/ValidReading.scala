package org.saswata.predictivemaintenance

final case class ValidReading(
  vehicle: String,
  epoch: Long,
  longitude: Double,
  latitude: Double,
  speed: Double,
  coolant: Double,
  engineOilPressure: Int
)

object ValidReading {

  def apply(raw: Reading): ValidReading = new ValidReading(
    vehicle = raw.sourceId,
    epoch = raw.gpsTimestamp.fold(0L)(_ / 1000L),
    longitude = raw.longitude.getOrElse(0.0),
    latitude = raw.latitude.getOrElse(0.0),
    speed = raw.speed.getOrElse(0.0),
    coolant = raw.coolantAnalog.fold(40.0)(c => if (c.toInt == 0 || c.toInt == 120) 40.0 else c),
    engineOilPressure = raw.engineOilPressure.getOrElse(0)
  )
}
