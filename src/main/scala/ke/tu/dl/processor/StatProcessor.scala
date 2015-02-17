package ke.tu.dl.processor

import kafka.message._
import kafka.consumer._

import org.slf4j.LoggerFactory

import scala.collection.mutable.Map

import org.json4s.{ DefaultFormats, Formats }
import org.json4s.native.JsonParser.parse

import ke.tu.dl.utils.Cfg._

/**
 * Connects to Kafka's IC topic and consume messages by streaming
 */
class StatProcessor(topic: String, consumerGroup: String, zookeeperHosts: String) {
  import scala.collection.mutable.ArrayBuffer
  import org.apache.commons.io.FileUtils
  import java.io.File
  import ke.tu.dl.utils.Cfg._
  import ke.tu.dl.localmodel.Stat._
  import ke.tu.dl.localmodel.{ Stat, IC, CI, CA, BC, FA, VE, OR }
  import ke.tu.dl.utils.Cfg._
  implicit lazy val formats = DefaultFormats

  val props = loadProperties

  val logger = LoggerFactory.getLogger(getClass)
  val kafkaConsumer = new KafkaConsumer(topic, consumerGroup, zookeeperHosts)
  // (File Name, TotalRecords, Total Missing Mandatory, Total TU Valid, Total KBA Valid, Total Amount)
  var stats = Map[String, Tuple12[String, Long, Long, Long, Long, Double, Long, Long, String, String, String, String]]()
  var ves = ArrayBuffer[VE]()
  var ors = ArrayBuffer[OR]()
  var ics = ArrayBuffer[IC]()
  var cis = ArrayBuffer[CI]()
  var cas = ArrayBuffer[CA]()
  var bcs = ArrayBuffer[BC]()
  var fas = ArrayBuffer[FA]()

  def saveStat = {
    if (stats.size > 0)
      stats.foreach { e =>
        val entry = e._2
        try {
          val existingEntry = getStats(entry._1)
          val stat: Stat = toStat(entry._1, entry._2, entry._3, entry._4, entry._5, entry._6, entry._7, entry._8, entry._9, entry._10, entry._11, entry._12)
          if (existingEntry == None) createStat(stat)
          else updateStat(stat)
        } catch {
          case e: Exception => FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getStackTraceString + "\n", true)
        }
      }

    if (ves.size > 0) {
      try {
        createValidationError(ves.toArray)
        ves.clear
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }
    }

    if (ors.size > 0) {
      try {
        createOriginalRecord(ors.toArray)
        ors.clear
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }
    }

    if (ics.size > 0) {
      try {
        createIC(ics.toArray)
        ics.clear
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }
    }

    if (cis.size > 0) {
      try {
        createCI(cis.toArray)
        cis.clear
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }
    }

    if (cas.size > 0) {
      try {
        createCA(cas.toArray)
        cas.clear
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }
    }

    if (bcs.size > 0) {
      try {
        createBC(bcs.toArray)
        bcs.clear
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }
    }

    if (fas.size > 0) {
      try {
        createFA(fas.toArray)
        fas.clear
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }
    }
  }

  def exec(binaryObject: Array[Byte]) = {
    val msg = new String(binaryObject)
    val json = parse(msg)
    val data = json.extract[scala.collection.Map[String, Object]]
    val accountNumber = data.get("accountNumber").getOrElse("").asInstanceOf[String]
    val fileName = data.get("fileName").getOrElse("").asInstanceOf[String]
    val originalTotalRecords = data.get("totalNumRecords").getOrElse("0").asInstanceOf[String]
    val record = data.get("record").getOrElse("").asInstanceOf[String]
    val tuValid = data.get("valid").getOrElse("0").asInstanceOf[String]
    val kbaValid = data.get("kbaValid").getOrElse("0").asInstanceOf[String]
    val missingMandatory = data.get("missingMandatory").getOrElse("0").asInstanceOf[String]
    val recordArr = record.split("\\|", -1)

    // File Name Components
    val fileNameComps = fileName.split("\\.")
    val batchId = fileNameComps(3).toLong
    val bankCode = fileNameComps(1)
    val submissionDate = fileNameComps(0).substring(6, 14)
    val fileTypeCode = fileNameComps(0).substring(4, 6)
    var subscriberId = ""
    try subscriberId = props.getProperty("ke." + bankCode.trim.toUpperCase)
    catch { case e: Exception => }

    processFileStat(fileName, batchId, bankCode, subscriberId, submissionDate,
      fileTypeCode, accountNumber, kbaValid.toInt, tuValid.toInt, missingMandatory.toInt, recordArr, data)

    processGeneralStat(batchId, bankCode, subscriberId, submissionDate,
      fileTypeCode, accountNumber, kbaValid.toInt, tuValid.toInt, recordArr)
  }

  /**
   * Processes configurable statistics
   */
  def processGeneralStat(
    batchId: Long, instCode: String, subscriberId: String, submissionDate: String,
    fileTypeCode: String, accountNumber: String, kbaValid: Int, tuValid: Int, r: Array[String]) = {
    var aggFields = ArrayBuffer[String]()

    if (topic equals IC_TOPIC) {
      var currentBalance = 0.0
      var accountStatus = ""
      var accountProductType = ""
      var employeeIndustry = ""
      var branchCode = ""
      var accountIndicator = ""
      var accountNumber = ""
      var surname = ""
      var otherNames = ""

      try accountStatus = r(props.getProperty("ic.accountStatus").toInt) catch { case e: Exception => }
      try accountProductType = r(props.getProperty("ic.accountProductType").toInt) catch { case e: Exception => }
      try employeeIndustry = r(props.getProperty("ic.employeeIndustryType").toInt) catch { case e: Exception => }
      try branchCode = r(props.getProperty("ic.branchCode").toInt) catch { case e: Exception => }
      try accountIndicator = r(props.getProperty("ic.accountIndicator").toInt) catch { case e: Exception => }
      try accountNumber = r(props.getProperty("ic.accountNumber").toInt) catch { case e: Exception => }
      try surname = r(props.getProperty("ic.surname").toInt) catch { case e: Exception => }
      try otherNames = r(props.getProperty("ic.foreName1").toInt) + " " +
        r(props.getProperty("ic.foreName2").toInt) + " " +
        r(props.getProperty("ic.foreName3").toInt) catch { case e: Exception => }

      try {
        try currentBalance = r(props.getProperty("ic.currentBalance").toInt).toDouble catch { case e: Exception => }
        ics.append(new IC(None,
          batchId, instCode, subscriberId, submissionDate, fileTypeCode, kbaValid, tuValid,
          accountStatus,
          accountProductType,
          employeeIndustry,
          branchCode,
          accountIndicator,
          accountNumber,
          surname,
          otherNames,
          currentBalance))
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }

      if (ics.size % 50 == 0) saveStat

    } else if (topic equals CI_TOPIC) {
      var currentBalance = 0.0
      var accountHolderType = ""
      var accountStatus = ""
      var accountProductType = ""
      var industryCode = ""
      var branchCode = ""
      var accountIndicator = ""
      var accountNumber = ""
      var surname = ""

      try accountHolderType = r(props.getProperty("ci.accountHolderType").toInt) catch { case e: Exception => }
      try accountStatus = r(props.getProperty("ci.accountStatus").toInt) catch { case e: Exception => }
      try accountProductType = r(props.getProperty("ci.accountProductType").toInt) catch { case e: Exception => }
      try industryCode = r(props.getProperty("ci.industryCode").toInt) catch { case e: Exception => }
      try branchCode = r(props.getProperty("ci.branchCode").toInt) catch { case e: Exception => }
      try accountIndicator = r(props.getProperty("ci.accountIndicator").toInt) catch { case e: Exception => }
      try accountNumber = r(props.getProperty("ci.accountNumber").toInt) catch { case e: Exception => }
      try surname = r(props.getProperty("ci.surname").toInt) catch { case e: Exception => }

      try {
        try currentBalance = r(props.getProperty("ci.currentBalance").toInt).toDouble catch { case e: Exception => }
        cis.append(new CI(None,
          batchId, instCode, subscriberId, submissionDate, fileTypeCode, kbaValid, tuValid,
          accountHolderType,
          accountStatus,
          accountProductType,
          industryCode,
          branchCode,
          accountIndicator,
          accountNumber,
          surname,
          currentBalance))
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }

      if (cis.size % 50 == 0) saveStat

    } else if (topic equals CA_TOPIC) {
      var applicationAmount = 0.0
      var applicationStatus = ""
      var branchCode = ""
      var productType = ""
      var facilityType = ""

      try applicationStatus = r(props.getProperty("ca.applicationStatus").toInt) catch { case e: Exception => }
      try branchCode = r(props.getProperty("ca.branchCode").toInt) catch { case e: Exception => }
      try productType = r(props.getProperty("ca.productType").toInt) catch { case e: Exception => }
      try facilityType = r(props.getProperty("ca.facilityApplicationType").toInt) catch { case e: Exception => }

      try {
        try applicationAmount = r(props.getProperty("ca.applicationAmount").toInt).toDouble catch { case e: Exception => }
        cas.append(new CA(None,
          batchId, instCode, subscriberId, submissionDate, fileTypeCode, kbaValid, tuValid,
          applicationStatus,
          branchCode,
          productType,
          facilityType,
          applicationAmount))
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }

      if (cas.size % 50 == 0) saveStat

    } else if (topic equals BC_TOPIC) {
      var chequeAmount = 0.0
      var branchCode = ""
      var accountType = ""

      try branchCode = r(props.getProperty("bc.branchCodeOnCheque").toInt) catch { case e: Exception => }
      try accountType = r(props.getProperty("bc.chequeAccountType").toInt) catch { case e: Exception => }

      try {
        try chequeAmount = r(props.getProperty("bc.chequeAmount").toInt).toDouble catch { case e: Exception => }
        bcs.append(new BC(None,
          batchId, instCode, subscriberId, submissionDate, fileTypeCode, kbaValid, tuValid,
          branchCode,
          accountType,
          chequeAmount))
      } catch { case e: Exception => }

      if (bcs.size % 50 == 0) saveStat

    } else if (topic equals FA_TOPIC) {
      var amount = 0.0
      var lossAmount = 0.0
      var status = ""
      var branchCode = ""

      try status = r(props.getProperty("fa.fraudStatus").toInt) catch { case e: Exception => }
      try branchCode = r(props.getProperty("fa.lenderBranchCode").toInt) catch { case e: Exception => }

      try {
        try amount = r(props.getProperty("fa.amount").toInt).toDouble catch { case e: Exception => }
        try lossAmount = r(props.getProperty("fa.lossAmount").toInt).toDouble catch { case e: Exception => }
        fas.append(new FA(None,
          batchId, instCode, subscriberId, submissionDate, fileTypeCode, kbaValid, tuValid,
          status,
          branchCode,
          amount: Double,
          lossAmount))
      } catch {
        case e: Exception =>
          FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }

      if (fas.size % 50 == 0) saveStat
    }
  }

  /**
   * Processes file level statistics
   */
  def processFileStat(fileName: String, batchId: Long, instCode: String, subscriberId: String, submissionDate: String,
    fileTypeCode: String, accountNumber: String, kbaValid: Int, tuValid: Int, missingMandatory: Int, r: Array[String], data: scala.collection.Map[String, Object]) = {

    val originalTotalRecords = data.get("totalNumRecords").getOrElse("0").asInstanceOf[String]
    val validationErrors: List[String] = data.get("errors").getOrElse(null).asInstanceOf[List[String]]
    val record = data.get("record").getOrElse("").asInstanceOf[String]

    val currentFileStat = stats.get(fileName).getOrElse(null)
    var totalRecords: Long = 0
    var totalMissingMandatory: Long = 0
    var totalTUValid: Long = 0
    var totalKBAValid: Long = 0
    var totalAmount: Double = 0.0

    if (currentFileStat != null) {
      totalRecords += currentFileStat._2
      totalMissingMandatory += currentFileStat._3
      totalTUValid += currentFileStat._4
      totalKBAValid += currentFileStat._5
      totalAmount += currentFileStat._6
    }

    //logger.info("Appended: " + fileName + " ### " + totalRecords + " ### " + totalTUValid)

    // Append to the map
    if (kbaValid equals 1) {
      totalKBAValid += 1
      var amount = "0"

      try {
        if (topic equals IC_TOPIC) amount = r(49)
        else if (topic equals CI_TOPIC) amount = r(41)
        else if (topic equals CA_TOPIC) amount = r(17)
        else if (topic equals CR_TOPIC) amount = r(16)
        else if (topic equals BC_TOPIC) amount = r(4)
        else if (topic equals FA_TOPIC) amount = r(10)

        totalAmount += amount.trim.toDouble
      } catch {
        case e: Exception => FileUtils.writeStringToFile(
          new File(logBaseDir + "dl-analytics.log"), e.getMessage() + "\n\n" + e.getStackTraceString + "\n\n\n", true)
      }
    }

    if (tuValid equals 1) totalTUValid += 1
    if (missingMandatory equals 1) totalMissingMandatory += 1

    // File Name Components
    val fileNameComps = fileName.split("\\.")
    val batchId = fileNameComps(3).toLong
    val bankCode = fileNameComps(1)
    val submissionDate = fileNameComps(0).substring(6, 14)
    val fileTypeCode = fileNameComps(0).substring(4, 6)
    var subscriberId = ""
    try subscriberId = props.getProperty("ke." + bankCode.trim.toUpperCase)
    catch { case e: Exception => }

    stats put (fileName, (fileName, totalRecords + 1, totalMissingMandatory,
      totalTUValid, totalKBAValid, totalAmount, originalTotalRecords.toLong, batchId: Long,
      submissionDate, bankCode, subscriberId, fileTypeCode))

    // Persist validation errors
    if (validationErrors != null) {
      validationErrors.foreach { error =>
        //logger.info(error)

        try {
          ves.append(toVE(accountNumber, error.split("\\|", -1)))
          if (ves.size % 50 == 0) {
            createValidationError(ves.toArray)
            ves.clear
          }
        } catch {
          case e: Exception => FileUtils.writeStringToFile(
            new File(logBaseDir + "dl-analytics.log"), e.getStackTraceString + "\n", true)
        }
      }
    }

    // Persist the original record
    try {
      val fileDetails = fileName.split("\\.")
      ors.append(toOR(fileName, tuValid, kbaValid,
        fileDetails(1), fileDetails(2), topic, record))

      if (ors.size % 50 == 0) {
        createOriginalRecord(ors.toArray)
        ors.clear
      }

    } catch {
      case e: Exception => FileUtils.writeStringToFile(
        new File(logBaseDir + "dl-analytics.log"), e.getStackTraceString + "\n", true)
    }
  }

  def read = {
    if (topic equals topic) {
      kafkaConsumer.read(exec)
    }
  }
}