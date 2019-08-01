package org.saswata.predictivemaintenance

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.Date

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import com.softwaremill.sttp._
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

object Helpers extends LazyLogging {

  val s3client: AmazonS3 = AmazonS3Client.builder().withRegion(Regions.AP_SOUTHEAST_1).build()
  val IST: ZoneId = ZoneId.of("Asia/Kolkata")

  val summaryFname = "Summary_Buckets.csv"
  val infProfFname = "Instantaneous_Profiles.csv"
  val alertsFname = "Alerts_Report.csv"

  def parseDouble(value: Option[String]): Double =
    value.fold(Double.NaN)(it => Try(it.toDouble).getOrElse(Double.NaN))

  val CMP_THRESHOLD = .001

  def fuzzyEq(lhs: Double, rhs: Double): Boolean = math.abs(lhs - rhs) <= CMP_THRESHOLD
  def fuzzyGt(lhs: Double, rhs: Double): Boolean = !fuzzyEq(lhs, rhs) && lhs > rhs
  def fuzzyLt(lhs: Double, rhs: Double): Boolean = !fuzzyEq(lhs, rhs) && lhs < rhs

  def csvRead(file: File): List[Map[String, String]] = {
    val reader = CSVReader.open(file)
    val lines = reader.allWithHeaders()
    reader.close()
    lines
  }

  def parseS3uri(path: String): (String, String) = {
    val uri = new java.net.URI(path)
    require(uri.getScheme == "s3", s"Not s3 uri : $path")

    val bucket = uri.getHost
    val key = uri.getPath.substring(1)
    require(key.nonEmpty, s"Bad s3 uri : $path")

    (bucket, key)
  }

  def downloadS3(path: String, destinationPath: String): File = {

    require(destinationPath.nonEmpty, s"Bad local file name $destinationPath")
    val destination = new File(destinationPath)
    require(destination != null, s"Unable to create $destinationPath")
    if (destination.exists) {
      logger.warn(s"Overwriting $destinationPath")
      val _ = destination.delete()
    }

    val (bucket, key) = parseS3uri(path)
    logger.info(s"Fetching : $path")
    val metadata = s3client.getObject(new GetObjectRequest(bucket, key), destination)
    require(metadata != null, s"null MetaData for s3 uri : $path")

    destination
  }

  def tmpFileName(name: String, ext: String): String = {
    val now = Instant.now().toEpochMilli
    s"/tmp/${name}_$now.$ext"
  }

  val EPOCH_SEC_LEN: Int = 10

  def ymdh(epoch: Long): String = {
    val secs = epoch.toString.take(EPOCH_SEC_LEN).toLong
    val i = Instant.ofEpochSecond(secs)
    val zdt = ZonedDateTime.ofInstant(i, IST)
    val (y, m, d, h) = (zdt.getYear, zdt.getMonthValue, zdt.getDayOfMonth, zdt.getHour)
    f"y=$y%04d/m=$m%02d/d=$d%02d/h=$h%02d"
  }

  def writeCsv(file: File, headers: Seq[String], lines: Seq[Seq[Any]]): Unit = {
    val writer = CSVWriter.open(file)
    writer.writeRow(headers)
    writer.writeAll(lines)
    writer.close()
  }

  def s3PartitionUri(s3Prefix: String, epoch: Long, s3Suffix: String): String = {
    val partition = ymdh(epoch)
    s"$s3Prefix/$partition/$s3Suffix"
  }

  def moveToS3(uri: String, file: File): String = {
    val (bucket, key) = parseS3uri(uri)
    logger.info(s"Writing to $uri")
    val response = s3client.putObject(bucket, key, file)
    require(response != null, s"Empty response from s3 putObject $uri")
    val _ = file.delete()

    getPublicUrl(bucket, key)
  }

  def getPublicUrl(bucket: String, key: String): String = {
    val expiration = new Date
    val expTimeMillis = expiration.getTime + 1000 * 60 * 15 // 15m
    expiration.setTime(expTimeMillis)

    s3client.generatePresignedUrl(bucket, key, expiration).toString
  }

  def persistS3(
    s3Prefix: String,
    epoch: Long,
    s3Suffix: String,
    headers: Seq[String],
    lines: Seq[Seq[Any]]
  ): Option[String] =
    if (lines.nonEmpty) {
      val localFname = tmpFileName(s3Suffix.replaceAllLiterally(".", "_"), "tmp")
      val file = new File(localFname)
      writeCsv(file, headers, lines)
      val s3Uri = s3PartitionUri(s3Prefix, epoch, s3Suffix)

      Option(moveToS3(s3Uri, file))
    } else {
      Option.empty[String]
    }

  def exceptionSummary(e: Exception): String = s"[${e.getClass.getName} : ${e.getMessage}]\n"

  def sendEmail(
    epochMillis: Long,
    count: Int,
    alertUrl: Option[String],
    summaryUrl: Option[String],
    instProfileUrl: Option[String]
  ): Unit = {
    val i = Instant.ofEpochMilli(epochMillis)
    val zdt = ZonedDateTime.ofInstant(i, IST)
    val time = DateTimeFormatter.ISO_DATE_TIME.format(zdt)

    val attachments = attachmentList(Seq(alertUrl, summaryUrl, instProfileUrl))
    val payload = emailPayload(time, count, attachments)

    val request = sttp
      .body(payload)
      .header("content-type", "application/json", replaceExisting = true)
      .post(uri"http://notifications-prod.ap-southeast-1.elasticbeanstalk.com/api/v1/email/send")

    logger.info(s"Sending Email Request: \n $payload")
    implicit val backend = HttpURLConnectionBackend()
    val response = request.send()
    logger.info(s"Response from email : \n ${response.unsafeBody}")
  }

  def attachmentList(urls: Seq[Option[String]]): String = {
    val attachments = urls
      .zip(Seq(alertsFname, summaryFname, infProfFname))
      .collect { case (Some(url), name) => attachment(url, name) }

    if (attachments.nonEmpty) {
      s"""[${attachments.mkString(", ")}]"""
    } else {
      "null"
    }
  }

  def attachment(url: String, name: String): String =
    s"""
      |{
      |  "name": "$name",
      |  "url": "$url",
      |  "type": "text/plain"
      |}
      |""".stripMargin

  def emailPayload(time: String, count: Int, attachments: String): String =
    s"""
       |{
       |    "from": "alerts@foo.com",
       |    "to": [
       |        "vehicle.maintenance@bar.com"
       |    ],
       |    "cc": [
       |        "saswatdutta@gmail.com"
       |    ],
       |    "subject": "Predictive Maintenance $time",
       |    "body": "Alerts Generated : $count",
       |    "attachmentList": $attachments
       |}
       |""".stripMargin
}
