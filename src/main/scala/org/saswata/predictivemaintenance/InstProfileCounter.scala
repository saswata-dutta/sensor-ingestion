package org.saswata.predictivemaintenance

final case class InstProfileCounter(
  max_trip_avg_coolant_temp: Double,
  max_trip_inst_coolant_temp: Double,
  relative_avg_temp_100: Int,
  relative_avg_temp_90_100: Int,
  relative_inst_temp_100: Int,
  relative_inst_temp_90_100: Int,
  relative_low_oil_pressure_100: Int,
  relative_low_oil_pressure_90_99: Int
)

object InstProfileCounter {

  def apply(): InstProfileCounter =
    new InstProfileCounter(Int.MinValue, Int.MinValue, 0, 0, 0, 0, 0, 0)
}
