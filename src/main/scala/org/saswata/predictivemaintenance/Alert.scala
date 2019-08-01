package org.saswata.predictivemaintenance

final case class Alert(vehicle: String, pressure: Boolean, temperature: Boolean, seizure: Boolean)
    extends CsvLine {
  override def csvLine: Seq[Any] = Seq[Any](vehicle, pressure, temperature, seizure)
}

object Alert extends CsvHeader {

  val csvHeaderPrefix: String = "Alert"
  val csvHeaders: Seq[String] = Seq("vehicle", "pressure", "temperature", "seizure")

  def check(ip: InstProfile, hp: Profile): Alert = {
    val temperature = temperatureAlert(ip, hp)
    val pressure = pressureAlert(ip, hp)
    val seizure = (temperature && pressure) || seizureAlert(ip, pressure)

    Alert(ip.vehicle, pressure, temperature, seizure)
  }

  def pressureAlert(ip: InstProfile, hp: Profile): Boolean =
    (ip.relative_low_oil_pressure_100 > hp.max_per_instances_100_low_pressure) ||
    (hp.max_per_instances_100_low_pressure > 75 &&
    ip.relative_low_oil_pressure_100 >
    (hp.avg_per_instances_100_low_pressure + hp.dev_per_instances_100_low_pressure)) ||
    (ip.relative_low_oil_pressure_100 + ip.relative_low_oil_pressure_90_99 > 90 &&
    ((ip.relative_low_oil_pressure_100 + ip.relative_low_oil_pressure_90_99) >
    (hp.max_per_instances_100_low_pressure + hp.max_per_instances_90_99_low_pressure)))

  def temperatureAlert(ip: InstProfile, hp: Profile): Boolean =
    ip.max_trip_avg_coolant_temp > 85 && ip.max_trip_inst_coolant_temp > 95 &&
    (ip.relative_avg_temp_100 > 0 ||
    (ip.relative_avg_temp_90_100 > hp.max_per_instances_90_100_relative_temp &&
    (ip.relative_inst_temp_90_100 + ip.relative_inst_temp_100) > hp.max_per_instances_90_100_relative_inst_temp))

  def seizureAlert(ip: InstProfile, pressureAlert: Boolean): Boolean =
    (ip.max_trip_avg_coolant_temp >= 90 && ip.max_trip_inst_coolant_temp >= 95 && ip.relative_avg_temp_100 >= 30) ||
    (ip.max_trip_avg_coolant_temp >= 95 && ip.max_trip_inst_coolant_temp >= 100 && pressureAlert) ||
    (ip.max_trip_avg_coolant_temp >= 90 && ip.max_trip_inst_coolant_temp >= 95 && ip.relative_low_oil_pressure_100 >= 75) ||
    (ip.max_trip_avg_coolant_temp >= 105 && ip.max_trip_inst_coolant_temp >= 115)

}
