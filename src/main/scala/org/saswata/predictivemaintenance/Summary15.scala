package org.saswata.predictivemaintenance

import com.typesafe.scalalogging.LazyLogging

final case class Summary15(
  vehicle: String,
  timebucket: Long,
  num_instances: Int,
  mean_coolant: Double,
  max_coolant: Double,
  mean_speed: Double,
  max_speed: Double,
  total_distance: Double,
  num_hi_oil_pressure: Int
) {

  def csvLine: Seq[Any] =
    Seq[Any](
      vehicle,
      timebucket,
      num_instances,
      mean_coolant,
      max_coolant,
      mean_speed,
      max_speed,
      total_distance,
      num_hi_oil_pressure
    )
}

object Summary15 extends LazyLogging {

  def apply(values: Map[String, String]): Summary15 =
    new Summary15(
      vehicle = values("vehicle"),
      timebucket = values("timebucket").toLong,
      num_instances = values("num_instances").toInt,
      mean_coolant = values("mean_coolant").toDouble,
      max_coolant = values("max_coolant").toDouble,
      mean_speed = values("mean_speed").toDouble,
      max_speed = values("max_speed").toDouble,
      total_distance = values("total_distance").toDouble,
      num_hi_oil_pressure = values("num_hi_oil_pressure").toInt
    )

  @SuppressWarnings(Array("org.wartremover.warts.Return", "org.wartremover.warts.TraversableOps"))
  def summariseBucket(
    vehicle: String,
    timebucket: Long,
    readings: Vector[ValidReading]
  ): Option[Summary15] = {

    val num_instances = readings.length
    if (num_instances < 5) {
      logger.info(s"Skipped Bucket: few readings: $vehicle : $timebucket : $num_instances")
      return Option.empty[Summary15]
    }

    val coolants = readings.map(_.coolant)
    val max_coolant = coolants.max

    val speeds = readings.map(_.speed)
    val max_speed = speeds.max

    if (max_coolant.intValue < 60 && max_speed.intValue <= 0) {
      logger.info(
        s"Skipped Bucket: low max_coolant & max_speed: $vehicle : $timebucket : coolant = $max_coolant : speed = $max_speed"
      )
      return Option.empty[Summary15]
    }

    val mean_coolant = coolants.sum / num_instances
    val mean_speed = speeds.sum / num_instances

    val num_hi_oil_pressure = readings.map(_.engineOilPressure).sum

    val total_distance = totalDistance(readings)

    Option(
      new Summary15(
        vehicle = vehicle,
        timebucket = timebucket,
        num_instances = num_instances,
        mean_coolant = mean_coolant,
        max_coolant = max_coolant,
        mean_speed = mean_speed,
        max_speed = max_speed,
        total_distance = total_distance,
        num_hi_oil_pressure = num_hi_oil_pressure
      )
    )
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Return",
      "org.wartremover.warts.TraversableOps",
      "com.sksamuel.scapegoat.inspections.collections.TraversableHead"
    )
  )
  def summarise(readings: Vector[ValidReading]): Vector[Summary15] = {
    if (readings.length < 5) {
      val vehicle = readings.headOption.fold("null")(_.vehicle)
      logger.info(s"Skipped overall Summarise: few readings $vehicle : ${readings.length}")
      return Vector.empty[Summary15]
    }

    val vehicle = readings.head.vehicle
    require(readings.forall(_.vehicle == vehicle), s"Found vehicle other than : $vehicle")

    val buckets = readings.groupBy(it => timeBucket(it.epoch))
    buckets
      .flatMap { case (bkt, values) => summariseBucket(vehicle, bkt, values) }
      .toVector
      .sortBy(_.timebucket)
  }

  def totalDistance(readings: Vector[ValidReading]): Double = {

    val sorted = readings.sortBy(_.epoch)
    sorted match {
      case x if x.isEmpty => 0.0
      case head +: tail =>
        val (_, sumDist) = tail.foldLeft((head, 0.0)) {
          case ((last, tot_dist), current) =>
            val distance =
              haversine(last.latitude, last.longitude, current.latitude, current.longitude)
            (current, tot_dist + distance)
        }
        sumDist
    }
  }

  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    import math._

    if (lat1 < 0 || lon1 < 0 || lat2 < 0 || lon2 < 0) {
      0.000
    } else {
      val dLat = (lat2 - lat1).toRadians
      val dLon = (lon2 - lon1).toRadians

      val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(
          lat2.toRadians
        )
      val c = 2 * asin(sqrt(a))

      val dist = 6372.8 * c
      BigDecimal(dist).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
  }

  val BUCKET_SIZE: Long = 15 * 60L

  def timeBucket(secs: Long): Long = {
    val overflow = secs % BUCKET_SIZE
    secs - overflow
  }

  val csvHeaders: Seq[String] =
    Seq(
      "vehicle",
      "timebucket",
      "num_instances",
      "mean_coolant",
      "max_coolant",
      "mean_speed",
      "max_speed",
      "total_distance",
      "num_hi_oil_pressure"
    )

  def cumulativeDistance(summaries: Vector[Summary15]): Double = summaries.map(_.total_distance).sum
}
