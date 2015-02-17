package ke.tu.dl.utils

import org.apache.commons.dbcp2.BasicDataSource
import com.mchange.v2.c3p0.ComboPooledDataSource
import scalikejdbc._

object localDBConn {
  import ke.tu.dl.utils.Cfg.{ DB_URL, DB_USER, DB_PASSWD }
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton(DB_URL, DB_USER, DB_PASSWD)

  val settings = ConnectionPoolSettings(
    initialSize = 5,
    maxSize = 20,
    connectionTimeoutMillis = 3000L)

  ConnectionPool.add('pgAnalytics, DB_URL, DB_USER, DB_PASSWD, settings)

  def getDBConn = ConnectionPool('pgAnalytics).borrow()
}

object productionDBConn {
  import ke.tu.dl.utils.Cfg.{ SQLS_DB_URL, SQLS_DB_USER, SQLS_DB_PASSWD }
  Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
  ConnectionPool.singleton(SQLS_DB_URL, SQLS_DB_USER, SQLS_DB_PASSWD)

  val settings = ConnectionPoolSettings(
    initialSize = 5,
    maxSize = 20,
    connectionTimeoutMillis = 3000L)

  ConnectionPool.add('sqlServerAnalytics, SQLS_DB_URL, SQLS_DB_USER, SQLS_DB_PASSWD, settings)

  def getDBConn = ConnectionPool.borrow()
}