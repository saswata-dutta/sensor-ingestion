package org.saswata.predictivemaintenance

final case class Report(alert: Alert, instProfile: InstProfile, profile: Profile) extends CsvLine {

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def csvLine: Seq[Any] = Seq[CsvLine](alert, instProfile, profile).flatMap(_.csvLine)
}

object Report extends CsvHeader {

  val csvHeaderPrefix: String = ""

  val csvHeaders: Seq[String] = {
    Seq[CsvHeader](Alert, InstProfile, Profile)
      .flatMap(it => it.csvHeaders.map(it.csvHeaderPrefix + "_" + _))
  }
}
