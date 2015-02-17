package ke.tu.dl.localmodel

import scala.collection.JavaConversions._

case class Stat(
  id: Long,
  fileName: String,
  totalRecords: Long,
  totalMissingMandatory: Long,
  totalTUValid: Long,
  totalKBAValid: Long,
  totalAmount: Double,
  originalTotalRecords: Long,
  batchId: Long,
  submissionDate: String,
  bankCode: String,
  subscriberId: String,
  fileTypeCode: String)

case class IC(
  id: Option[Long],
  batchId: Long,
  instCode: String,
  subscriberId: String,
  submissionDate: String,
  fileTypeCode: String,
  kbaValid: Int,
  tuValid: Int,
  accountStatus: String,
  accountProductType: String,
  employeeIndustryType: String,
  branchCode: String,
  accountIndicator: String,
  accountNumber: String,
  surname: String,
  otherNames: String,
  currentBalance: Double)

case class CI(
  id: Option[Long],
  batchId: Long,
  instCode: String,
  subscriberId: String,
  submissionDate: String,
  fileTypeCode: String,
  kbaValid: Int,
  tuValid: Int,
  accountHolderType: String,
  accountStatus: String,
  accountProductType: String,
  industryCode: String,
  branchCode: String,
  accountIndicator: String,
  accountNumber: String,
  surname: String,
  currentBalance: Double)

case class CA(
  id: Option[Long],
  batchId: Long,
  instCode: String,
  subscriberId: String,
  submissionDate: String,
  fileTypeCode: String,
  kbaValid: Int,
  tuValid: Int,
  applicationStatus: String,
  branchCode: String,
  productType: String,
  facilityType: String,
  applicationAmount: Double)

case class BC(
  id: Option[Long],
  batchId: Long,
  instCode: String,
  subscriberId: String,
  submissionDate: String,
  fileTypeCode: String,
  kbaValid: Int,
  tuValid: Int,
  branchCodeOnCheque: String,
  chequeAccountType: String,
  chequeAmount: Double)

case class FA(
  id: Option[Long],
  batchId: Long,
  instCode: String,
  subscriberId: String,
  submissionDate: String,
  fileTypeCode: String,
  kbaValid: Int,
  tuValid: Int,
  fraudStatus: String,
  lenderBranchCode: String,
  amount: Double,
  lossAmount: Double)

case class VE(
  id: Long,
  fileName: String,
  affectedField: String,
  affectedFieldLabel: String,
  providedValue: String,
  errorType: String,
  errorMessage: String,
  advice: String,
  accountNumber: String)

case class OR(
  id: Long,
  fileName: String,
  tuValid: Int,
  kbaValid: Int,
  bankCode: String,
  processedDate: String,
  fileType: String,
  recordDetail: String)

object Stat {
  import scalikejdbc._
  //import ke.tu.dl.utils.localDBConn._
  import ke.tu.dl.utils.productionDBConn._

  def toStat(fileName: String, totalRecords: Long, totalMissingMandatory: Long,
    totalTUValid: Long, totalKBAValid: Long, totalAmount: Double, originalTotalRecords: Long,
    batchId: Long, submissionDate: String, bankCode: String, subscriberId: String, fileTypeCode: String): Stat =
    return new Stat(
      0, fileName, totalRecords, totalMissingMandatory, totalTUValid, totalKBAValid,
      totalAmount, originalTotalRecords, batchId, submissionDate, bankCode, subscriberId, fileTypeCode)

  def toVE(accountNumber: String, r: Array[String]): VE =
    return new VE(0, r(0), r(1), r(6), r(2), r(5), r(3), r(4), accountNumber)

  def toOR(fileName: String, tuValid: Int, kbaValid: Int,
    bankCode: String, processedDate: String, fileType: String, recordDetail: String): OR =
    return new OR(0, fileName, tuValid,
      kbaValid, bankCode, processedDate, fileType, recordDetail)

  def getStats(fileName: String): Option[Stat] = using(DB(getDBConn)) { db =>
    {
      db readOnly { implicit session =>
        sql"select * from file_stat where file_name = ${fileName.trim}".map(
          rs => Stat(rs.long("file_stat_id"),
            rs.string("file_name"),
            rs.long("total_records"),
            rs.long("total_missing_mandatory"),
            rs.long("total_tu_valid"),
            rs.long("total_kba_valid"),
            rs.double("total_amount"),
            rs.long("original_total_records"),
            rs.long("batch_id"),
            rs.string("submission_date"),
            rs.string("bank_code"),
            rs.string("subscriber_id"),
            rs.string("file_type_code"))).first.apply
      }
    }
  }

  def createStat(stat: Stat) = using(DB(getDBConn)) { db =>
    {
      db.localTx { implicit session =>
        {
          sql"""
          	insert into file_stat(file_name, total_records, total_missing_mandatory, total_tu_valid, 
          	total_kba_valid, total_amount, original_total_records, 
          	batch_id, submission_date, bank_code, subscriber_id, file_type_code) values (${stat.fileName}, ${stat.totalRecords},
          	${stat.totalMissingMandatory}, ${stat.totalTUValid}, ${stat.totalKBAValid}, ${stat.totalAmount}, ${stat.originalTotalRecords},
          	${stat.batchId}, ${stat.submissionDate}, ${stat.bankCode}, ${stat.subscriberId}, ${stat.fileTypeCode})
          """
            .update.apply
        }
      }
    }
  }

  def updateStat(stat: Stat) = using(DB(getDBConn)) { db =>
    db.localTx { implicit session =>
      {
        sql"""
    	  update file_stat set 
        	total_records = ${stat.totalRecords},
    	  	total_missing_mandatory = ${stat.totalMissingMandatory},
    	  	total_tu_valid = ${stat.totalTUValid},
    	  	total_kba_valid = ${stat.totalKBAValid},
    	  	total_amount = ${stat.totalAmount},
    	  	original_total_records = ${stat.originalTotalRecords},
    	  	batch_id = ${stat.batchId},
    	  	submission_date = ${stat.submissionDate},
    	  	bank_code = ${stat.bankCode},
    	  	subscriber_id = ${stat.subscriberId},
    	  	file_type_code = ${stat.fileTypeCode} 
    	  where file_name = ${stat.fileName.trim}
    	  """.update.apply
      }
    }
  }

  def createValidationError(ves: Array[VE]) = using(DB(getDBConn)) { db =>
    db.localTx { implicit session =>
      ves.foreach(ve => {
        sql"""
    	  	insert into validation_error (file_name, affected_field, provided_value, error_type, error_message, advice, affected_field_label, account_number) 
        	values(${ve.fileName}, ${ve.affectedField}, ${ve.providedValue}, ${ve.errorType}, ${ve.errorMessage}, ${ve.advice}, ${ve.affectedFieldLabel}, ${ve.accountNumber})
    	  """.update.apply
      })
    }
  }

  def createOriginalRecord(ors: Array[OR]) = using(DB(getDBConn)) { db =>
    db.localTx { implicit session =>
      ors.foreach(or => {
        sql"""
  	  	insert into original_record(file_name, tu_valid, kba_valid, bank_code, processed_date, file_type, record_detail) 
        values(${or.fileName}, ${or.tuValid}, ${or.kbaValid}, ${or.bankCode}, ${or.processedDate}, ${or.fileType}, ${or.recordDetail})
  	  """.update.apply
      })
    }
  }

  def createIC(ics: Array[IC]) = using(DB(getDBConn)) { db =>
    db localTx { implicit session =>
      ics.foreach(ic => {
        sql"""
    	  insert into ic (
	        	batch_id,
	        	inst_code,
	        	subscriber_id,
	        	submission_date,
	        	file_type_code,
	        	kba_valid,
        		tu_valid,
	        	account_status,
	        	account_product_type,
	        	employee_industry_type,
	        	branch_code,
	        	account_indicator,
	        	account_number,
	        	surname,
	        	other_names,
	        	current_balance
        ) values (
        	${ic.batchId},
        	${ic.instCode},
        	${ic.subscriberId},
        	${ic.submissionDate},
        	${ic.fileTypeCode},
        	${ic.kbaValid},
        	${ic.tuValid},
        	${ic.accountStatus},
        	${ic.accountProductType},
        	${ic.employeeIndustryType},
        	${ic.branchCode},
        	${ic.accountIndicator},
        	${ic.accountNumber},
        	${ic.surname},
        	${ic.otherNames},
        	${ic.currentBalance}
        )
    	  """.update.apply
      })
    }
  }

  def createCI(cis: Array[CI]) = using(DB(getDBConn)) { db =>
    db localTx { implicit session =>
      cis.foreach(ci => {
        sql"""
        insert into ci (
        	batch_id,
        	inst_code,
        	subscriber_id,
        	submission_date,
        	file_type_code,
        	kba_valid,
        	tu_valid,
        	account_holder_type,
        	account_status,
        	account_product_type,
        	industry_code,
        	branch_code,
        	account_indicator,
        	account_number,
        	surname,
        	current_balance 
        ) values (
        	${ci.batchId},
        	${ci.instCode},
        	${ci.subscriberId},
        	${ci.submissionDate},
        	${ci.fileTypeCode},
        	${ci.kbaValid},
        	${ci.tuValid},
        	${ci.accountHolderType},
        	${ci.accountStatus},
        	${ci.accountProductType},
        	${ci.industryCode},
        	${ci.branchCode},
        	${ci.accountIndicator},
        	${ci.accountNumber},
        	${ci.surname},
        	${ci.currentBalance}
        )
    	  """.update.apply
      })
    }
  }

  def createCA(cas: Array[CA]) = using(DB(getDBConn)) { db =>
    db localTx { implicit session =>
      cas.foreach(ca => {
        sql"""
        	insert into ca (
        		batch_id,
        		inst_code,
        		subscriber_id,
        		submission_date,
        		file_type_code,
        		kba_valid,
        		tu_valid,
        		application_status,
        		branch_code,
        		product_type,
        		facility_type,
        		application_amount
        	) values (
        		${ca.batchId},
        		${ca.instCode},
        		${ca.subscriberId},
        		${ca.submissionDate},
        		${ca.fileTypeCode},
        		${ca.kbaValid},
        		${ca.tuValid},
        		${ca.applicationStatus},
        		${ca.branchCode},
        		${ca.productType},
        		${ca.facilityType},
        		${ca.applicationAmount}
        	)
    	  """.update.apply
      })
    }
  }

  def createBC(bcs: Array[BC]) = using(DB(getDBConn)) { db =>
    db localTx { implicit session =>
      bcs.foreach(bc => {
        sql"""
        	insert into bc (
        		batch_id,
        		inst_code,
        		subscriber_id,
        		submission_date,
        		file_type_code,
        		kba_valid,
        		tu_valid,
        		branch_code_on_cheque,
        		cheque_account_type,
        		cheque_amount
        	) values (
        		${bc.batchId},
        		${bc.instCode},
        		${bc.subscriberId},
        		${bc.submissionDate},
        		${bc.fileTypeCode},
        		${bc.kbaValid},
        		${bc.tuValid},
        		${bc.branchCodeOnCheque},
        		${bc.chequeAccountType},
        		${bc.chequeAmount}
        	) 	  
    	  """.update.apply
      })
    }
  }

  def createFA(fas: Array[FA]) = using(DB(getDBConn)) { db =>
    db localTx { implicit session =>
      fas.foreach(fa => {
        sql"""
        	insert into fa (
        		batch_id,
        		inst_code,
        		subscriber_id,
        		submission_date,
        		file_type_code,
        		kba_valid,
        		tu_valid,
        		fraud_status,
        		lender_branch_code,
        		amount,
        		loss_amount
        	) values (
        		${fa.batchId},
        		${fa.instCode},
        		${fa.subscriberId},
        		${fa.submissionDate},
        		${fa.fileTypeCode},
        		${fa.kbaValid},
        		${fa.tuValid},
        		${fa.fraudStatus},
        		${fa.lenderBranchCode},
        		${fa.amount},
        		${fa.lossAmount}
        	)
    	  """.update.apply
      })
    }
  }
}