package org.saswata.predictivemaintenance

trait CsvHeader {
  def csvHeaderPrefix: String
  def csvHeaders: Seq[String]
}
