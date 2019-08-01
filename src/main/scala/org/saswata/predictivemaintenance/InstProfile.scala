package org.saswata.predictivemaintenance

import com.typesafe.scalalogging.LazyLogging

final case class InstProfile(
  vehicle: String,
  max_trip_avg_coolant_temp: Double,
  max_trip_inst_coolant_temp: Double,
  relative_avg_temp_100: Double,
  relative_avg_temp_90_100: Double,
  relative_inst_temp_100: Double,
  relative_inst_temp_90_100: Double,
  relative_low_oil_pressure_100: Double,
  relative_low_oil_pressure_90_99: Double
) extends CsvLine {

  override def csvLine: Seq[Any] =
    Seq[Any](
      vehicle,
      max_trip_avg_coolant_temp,
      max_trip_inst_coolant_temp,
      relative_avg_temp_100,
      relative_avg_temp_90_100,
      relative_inst_temp_100,
      relative_inst_temp_90_100,
      relative_low_oil_pressure_100,
      relative_low_oil_pressure_90_99
    )
}

object InstProfile extends CsvHeader with LazyLogging {

  val csvHeaderPrefix: String = "Inferred_Profile"

  val csvHeaders: Seq[String] = Seq(
    "vehicle",
    "max_trip_avg_coolant_temp",
    "max_trip_inst_coolant_temp",
    "relative_avg_temp_100",
    "relative_avg_temp_90_100",
    "relative_inst_temp_100",
    "relative_inst_temp_90_100",
    "relative_low_oil_pressure_100",
    "relative_low_oil_pressure_90_99"
  )

  def apply(vehicle: String): InstProfile =
    new InstProfile(vehicle, Int.MinValue, Int.MinValue, 0, 0, 0, 0, 0, 0)

  def compute(summaries: Vector[Summary15], histProfile: Profile): Option[InstProfile] = {
    val currentProfile =
      summaries.foldLeft(InstProfileCounter()) {
        case (accProfile, summary) =>
          val relativeTemp =
            calcRelative(summary.mean_coolant, histProfile.max_historical_avg_temp)
          val relativeInstTemp =
            calcRelative(summary.max_coolant, histProfile.max_historical_inst_temp)
          val lowPressurePercent = 100 - calcRelative(
              summary.num_hi_oil_pressure.toDouble,
              summary.num_instances.toDouble
            )

          InstProfileCounter(
            Math.max(summary.mean_coolant, accProfile.max_trip_avg_coolant_temp),
            Math.max(summary.max_coolant, accProfile.max_trip_inst_coolant_temp),
            accProfile.relative_avg_temp_100 + updateCounter(relativeTemp, 100, Int.MaxValue),
            accProfile.relative_avg_temp_90_100 + updateCounter(relativeTemp, 90, 100),
            accProfile.relative_inst_temp_100 + updateCounter(relativeInstTemp, 100, Int.MaxValue),
            accProfile.relative_inst_temp_90_100 + updateCounter(relativeInstTemp, 90, 100),
            accProfile.relative_low_oil_pressure_100 + updateCounter(lowPressurePercent, 99, 100),
            accProfile.relative_low_oil_pressure_90_99 + updateCounter(lowPressurePercent, 90, 99)
          )
      }

    if (currentProfile.max_trip_avg_coolant_temp < 60) {
      logger.info(
        s"Skipped Alert check: low max_trip_avg_coolant_temp : ${histProfile.vehicle} : ${currentProfile.max_trip_avg_coolant_temp}"
      )

      Option.empty[InstProfile]
    } else {
      Option(
        InstProfile(
          vehicle = histProfile.vehicle,
          max_trip_avg_coolant_temp = currentProfile.max_trip_avg_coolant_temp,
          max_trip_inst_coolant_temp = currentProfile.max_trip_inst_coolant_temp,
          relative_avg_temp_100 =
            calcRelative(currentProfile.relative_avg_temp_100, summaries.length),
          relative_avg_temp_90_100 =
            calcRelative(currentProfile.relative_avg_temp_90_100, summaries.length),
          relative_inst_temp_100 =
            calcRelative(currentProfile.relative_inst_temp_100, summaries.length),
          relative_inst_temp_90_100 =
            calcRelative(currentProfile.relative_inst_temp_90_100, summaries.length),
          relative_low_oil_pressure_100 =
            calcRelative(currentProfile.relative_low_oil_pressure_100, summaries.length),
          relative_low_oil_pressure_90_99 =
            calcRelative(currentProfile.relative_low_oil_pressure_90_99, summaries.length)
        )
      )
    }
  }

  def updateCounter(relativeValue: Double, lowerBound: Int, upperBound: Int): Int =
    if (relativeValue > lowerBound && relativeValue <= upperBound) 1 else 0

  def calcRelative(value: Number, maxValue: Number): Double =
    value.doubleValue() * 100 / maxValue.doubleValue()
}
