package ke.tu.dl.utils

import scala.reflect.ScalaSignature

object Cfg {

  // Config base director
  val cfgBaseDir = "C:/ws/scala/dl-analytics/cfg/"
  val props = loadProperties

  val logBaseDir = props.getProperty("logs.basedir")

  // KAFKA
  val ZOOKEEPER_HOSTS = props.getProperty("zookeeper.hosts")
  val IC_TOPIC = props.getProperty("topic.ic")
  val IC_CONSUMER_GRP = props.getProperty("cg.ic")

  val CI_TOPIC = props.getProperty("topic.ci")
  val CI_CONSUMER_GRP = props.getProperty("cg.ci")
  val SI_TOPIC = props.getProperty("topic.si")
  val SI_CONSUMER_GRP = props.getProperty("cg.si")
  val GI_TOPIC = props.getProperty("topic.gi")
  val GI_CONSUMER_GRP = props.getProperty("cg.gi")
  val BC_TOPIC = props.getProperty("topic.bc")
  val BC_CONSUMER_GRP = props.getProperty("cg.bc")
  val CA_TOPIC = props.getProperty("topic.ca")
  val CA_CONSUMER_GRP = props.getProperty("cg.ca")
  val CR_TOPIC = props.getProperty("topic.cr")
  val CR_CONSUMER_GRP = props.getProperty("cg.cr")
  val FA_TOPIC = props.getProperty("topic.fa")
  val FA_CONSUMER_GRP = props.getProperty("cg.fa")

  // PotgreSQL  DATABASE
  val DB_DRIVER = "org.postgresql.Driver"
  val DB_URL = "jdbc:postgresql://localhost:5432/cis4_dataloader_scala"
  val DB_USER = "postgres"
  val DB_PASSWD = "0k5LLO12"
  val DB_POOL_INITSIZE = 20
  val DB_POOL_MAXIDLE = 10

  // SQL Server
  val SQLS_DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val SQLS_DB_URL = "jdbc:sqlserver://crbserver08:1433;databaseName=KE_CrbDB3"
  val SQLS_DB_USER = "CIS4Loader"
  val SQLS_DB_PASSWD = "P@738g#1"

  /**
   * Load properties
   */
  import java.util.Properties
  import java.io.FileInputStream
  def loadProperties(): Properties = {

    // Load subscriber properties
    val props = new Properties
    props.load(new FileInputStream(cfgBaseDir + "subscribers.properties"))

    // Load aggregation properties
    val aggProps = new Properties
    aggProps.load(new FileInputStream(cfgBaseDir + "agg.properties"))

    val globalProps = new Properties
    globalProps.load(new FileInputStream(cfgBaseDir + "global.properties"))

    props.putAll(aggProps)
    props.putAll(globalProps)

    return (props)
  }
}

