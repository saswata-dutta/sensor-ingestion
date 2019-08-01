package org.saswata.predictivemaintenance

import Helpers.parseDouble

final case class Profile(
  vehicle: String,
  max_historical_inst_temp: Double,
  max_historical_avg_temp: Double,
  max_per_instances_100_low_pressure: Double,
  avg_per_instances_100_low_pressure: Double,
  dev_per_instances_100_low_pressure: Double,
  max_per_instances_90_100_relative_inst_temp: Double,
  avg_per_instances_90_100_relative_inst_temp: Double,
  max_per_instances_90_100_relative_temp: Double,
  avg_per_instances_90_100_relative_temp: Double,
  max_per_instances_90_99_low_pressure: Double,
  avg_per_instances_90_99_low_pressure: Double
) extends CsvLine {
  override def csvLine: Seq[Any] =
    Seq[Any](
      vehicle,
      max_historical_inst_temp,
      max_historical_avg_temp,
      max_per_instances_100_low_pressure,
      avg_per_instances_100_low_pressure,
      dev_per_instances_100_low_pressure,
      max_per_instances_90_100_relative_inst_temp,
      avg_per_instances_90_100_relative_inst_temp,
      max_per_instances_90_100_relative_temp,
      avg_per_instances_90_100_relative_temp,
      max_per_instances_90_99_low_pressure,
      avg_per_instances_90_99_low_pressure
    )
}

object Profile extends CsvHeader {
  val csvHeaderPrefix: String = "Profile"

  val csvHeaders: Seq[String] = Seq(
    "vehicle",
    "max_historical_inst_temp",
    "max_historical_avg_temp",
    "max_per_instances_100_low_pressure",
    "avg_per_instances_100_low_pressure",
    "dev_per_instances_100_low_pressure",
    "max_per_instances_90_100_relative_inst_temp",
    "avg_per_instances_90_100_relative_inst_temp",
    "max_per_instances_90_100_relative_temp",
    "avg_per_instances_90_100_relative_temp",
    "max_per_instances_90_99_low_pressure",
    "avg_per_instances_90_99_low_pressure"
  )

  // format: off
  def apply(values: Map[String, String]): Profile =
    new Profile(
      vehicle = values.getOrElse("vehicle", "null"),
      max_historical_inst_temp = parseDouble(values.get("max_historical_inst_temp")),
      max_historical_avg_temp = parseDouble(values.get("max_historical_avg_temp")),
      max_per_instances_100_low_pressure =
        parseDouble(values.get("max_per_instances_100_low_pressure")),
      avg_per_instances_100_low_pressure =
        parseDouble(values.get("avg_per_instances_100_low_pressure")),
      dev_per_instances_100_low_pressure =
        parseDouble(values.get("dev_per_instances_100_low_pressure")),
      max_per_instances_90_100_relative_inst_temp =
        parseDouble(values.get("max_per_instances_90_100_relative_inst_temp")),
      avg_per_instances_90_100_relative_inst_temp =
        parseDouble(values.get("avg_per_instances_90_100_relative_inst_temp")),
      max_per_instances_90_100_relative_temp =
        parseDouble(values.get("max_per_instances_90_100_relative_temp")),
      avg_per_instances_90_100_relative_temp =
        parseDouble(values.get("avg_per_instances_90_100_relative_temp")),
      max_per_instances_90_99_low_pressure =
        parseDouble(values.get("max_per_instances_90_99_low_pressure")),
      avg_per_instances_90_99_low_pressure = parseDouble(
        values.get("avg_per_instances_90_99_low_pressure")
      )
    )
  // format: off
}
