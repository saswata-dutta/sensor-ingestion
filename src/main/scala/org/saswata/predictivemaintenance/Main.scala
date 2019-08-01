package org.saswata.predictivemaintenance

import java.time.Instant

import Helpers._
import com.softwaremill.sttp._
import com.softwaremill.sttp.json4s._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

@SuppressWarnings(Array("com.sksamuel.scapegoat.inspections.exception.CatchException"))
object Main extends LazyLogging {

  val DURATION: Long = 3L * 60L * 60L * 1000L // hours to ms

  val defaultArgs: Array[String] = Array(
    "s3://saswata/dev/predictive-maintenance/model/profiles.csv",
    "s3://saswata/dev/predictive-maintenance/model/sample-vehicles.csv",
    "s3://saswata/dev/predictive-maintenance/inference"
  )

  def main(args: Array[String]): Unit = {
    val appStartTs: Long = Instant.now().toEpochMilli
    try {
      val stopTime: Long = args.headOption.fold(appStartTs)(_.toLong)
      val startTime: Long = stopTime - DURATION

      require(appStartTs >= stopTime, s"Execution refers to future $appStartTs < $stopTime")
      logger.info(s"Execution for [$startTime, $stopTime]")

      val Array(profileUri, sampleSetUri, inferenceOutPrefix) =
        Array(1, 2, 3).map(i => args.lift(i).getOrElse(defaultArgs(i - 1)))
      val alertsPrefix = s"$inferenceOutPrefix/alert-report"
      val summaryUriPrefix = s"$inferenceOutPrefix/vitals-3hr-summary"
      val inferredProfilePrefix = s"$inferenceOutPrefix/inferred-profile"

      val profiles = fetchProfiles(sampleSetUri, profileUri)
      val windowEpochs = Vector.tabulate(3)(it => startTime - DURATION * it)
      val oldSummaries = getPreviousSummaries(summaryUriPrefix, windowEpochs)

      val (report, currentSummaries, inferredProfiles) =
        run(startTime, stopTime, profiles, oldSummaries)

      logger.info(s"Computed ${inferredProfiles.length} inference profiles")
      logger.info(s"Found ${report.length} potential alerts")

      val (alertReportUrl, summaryUrl, instProfileUrl) = persistResults(
        stopTime,
        alertsPrefix,
        report,
        summaryUriPrefix,
        currentSummaries,
        inferredProfilePrefix,
        inferredProfiles
      )

      sendEmail(stopTime, report.length, alertReportUrl, summaryUrl, instProfileUrl)
    } catch {
      case e: Exception =>
        logger.error("Unhandled Exception", e)
        System.exit(1)
    } finally {
      val appStopTs: Long = Instant.now().toEpochMilli
      logger.info(s"App Time : ${appStopTs - appStartTs}")
    }
  }

  def run(
    startTime: Long,
    stopTime: Long,
    profiles: Vector[Profile],
    oldSummaries: Map[String, Vector[Summary15]]
  ): (Vector[Report], Vector[Summary15], Vector[InstProfile]) = {

    val report = mutable.ArrayStack[Report]()
    val inferredProfiles = mutable.ArrayStack[InstProfile]()

    val currentSummaries =
      profiles.foldLeft(Vector.empty[Summary15]) {
        case (summaryAcc, profile) =>
          val summaries = Summary15.summarise(invokeApi(profile.vehicle, startTime, stopTime))
          val sampleSummaries = filterSampleSummaries(profile.vehicle, summaries, oldSummaries)

          InstProfile.compute(sampleSummaries, profile).foreach { instProfile =>
            inferredProfiles.push(instProfile)
            val alert = Alert.check(instProfile, profile)
            if (alert.seizure) report.push(Report(alert, instProfile, profile))
          }
          summaryAcc ++ summaries
      }

    (report.toVector, currentSummaries, inferredProfiles.toVector)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def persistResults(
    epoch: Long,
    alertsPrefix: String,
    alertsReport: Vector[Report],
    summaryPrefix: String,
    summaries: Vector[Summary15],
    infProfPrefix: String,
    instProfiles: Vector[InstProfile]
  ): (Option[String], Option[String], Option[String]) =
    (
      persistS3(alertsPrefix, epoch, alertsFname, Report.csvHeaders, alertsReport.map(_.csvLine)),
      persistS3(summaryPrefix, epoch, summaryFname, Summary15.csvHeaders, summaries.map(_.csvLine)),
      persistS3(
        infProfPrefix,
        epoch,
        infProfFname,
        InstProfile.csvHeaders,
        instProfiles.map(_.csvLine)
      )
    )

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Return"))
  def filterSampleSummaries(
    vehicle: String,
    currentSummaries: Vector[Summary15],
    prevSummaries: Map[String, Vector[Summary15]]
  ): Vector[Summary15] = {
    val distanceInCurrentWindow = Summary15.cumulativeDistance(currentSummaries)
    if (distanceInCurrentWindow <= 0.0) {
      logger.info(
        s"Skipped Profile Inference: low distance in last 3hrs :$vehicle : $distanceInCurrentWindow"
      )
      return Vector.empty[Summary15]
    }

    val summaries = (currentSummaries ++ prevSummaries.getOrElse(vehicle, Vector.empty[Summary15]))
      .sortBy(_.timebucket)(Ordering[Long].reverse)

    val summariesWithin200km = mutable.ArrayStack[Summary15]()
    var distanceSoFar = 0.0
    var i = 0
    while (distanceSoFar < 200 && i < summaries.length) {
      val currentSummary = summaries(i)
      summariesWithin200km.push(currentSummary)
      distanceSoFar += currentSummary.total_distance
      i += 1
    }

    if (summariesWithin200km.length < 5 || distanceSoFar.round < 30L) {
      logger.info(
        s"Skipped Profile Inference: few buckets or distance: $vehicle, count in last 200km=${summariesWithin200km.length}, distanceSoFar=$distanceSoFar"
      )
      Vector.empty[Summary15]
    } else {
      summariesWithin200km.toVector.reverse
    }
  }

  def invokeApi(truck: String, start: Long, end: Long): Vector[ValidReading] =
    try {
      require(start < end, s"Bad Time range $start >= $end")
      require(Reading.validVehicle(truck), s"Bad Truck $truck")

      implicit val backend = HttpURLConnectionBackend()
      implicit val serialization = org.json4s.native.Serialization

      val uri =
        uri"https://chronos-api.com/api/v1/data/vehicle_vital_details?sourceId=$truck&startDate=$start&endDate=$end"
      logger.info(s"Invoke ${uri.toString}")

      val request = sttp
        .get(uri)
        .response(asJson[Seq[Reading]])

      val response = request.send()
      val readings = response.unsafeBody

      readings.filter(it => it.sourceId == truck).flatMap(_.asValidReading).toVector
    } catch {
      case e: Exception =>
        logger.warn(s"Failed API fetch for $truck [$start,$end] ${exceptionSummary(e)}")
        Vector.empty[ValidReading]
    }

  def fetchProfiles(sampleSetUri: String, profileUri: String): Vector[Profile] = {

    val sampleSetFile = downloadS3(sampleSetUri, tmpFileName("sample-vehicles", "csv"))

    val sampleVehicles: Set[String] = csvRead(sampleSetFile)
      .map(it => it("vehicle"))
      .filter(Reading.validVehicle)
      .toSet

    val profileFile =
      downloadS3(profileUri, tmpFileName("vehicle-maintenance-profile", "csv"))

    val profiles = csvRead(profileFile)
      .map(Profile.apply)
      .filter(it => sampleVehicles.contains(it.vehicle))
      .toVector

    val _ = (profileFile.delete(), sampleSetFile.delete())

    profiles
  }

  def getPreviousSummaries(
    summaryUriPrefix: String,
    startEpochs: Seq[Long]
  ): Map[String, Vector[Summary15]] =
    startEpochs
      .flatMap { epoch =>
        downloadSummary(s3PartitionUri(summaryUriPrefix, epoch, summaryFname))
      }
      .groupBy(_.vehicle)
      .mapValues(_.toVector)

  def downloadSummary(summaryUri: String): Vector[Summary15] =
    try {
      val summaryFile = downloadS3(summaryUri, tmpFileName("old-summary", "csv"))
      val summary = csvRead(summaryFile).map(Summary15.apply).toVector
      val _ = summaryFile.delete()
      summary
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to download summary $summaryUri ${exceptionSummary(e)}")
        Vector.empty[Summary15]
    }
}
