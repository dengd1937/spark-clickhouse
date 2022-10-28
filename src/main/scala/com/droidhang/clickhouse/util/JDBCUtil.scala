package com.droidhang.clickhouse.util

import java.sql.{Connection, PreparedStatement, ResultSetMetaData, SQLException, Statement}
import java.util.Properties

import com.clickhouse.jdbc.ClickHouseDataSource
import com.droidhang.clickhouse.ClickHouseConfig
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

object JDBCUtil extends Logging {

  def getConnection(prop: Properties): Connection = {
    val url = prop.getProperty(ClickHouseConfig.CK_JDBC_URL)
    if (StringUtils.isEmpty(url)) {
      throw new IllegalArgumentException(ClickHouseConfig.CK_JDBC_URL + "must have value!")
    }
    val clickHouseDataSource = new ClickHouseDataSource(url, prop)
    clickHouseDataSource.getConnection
  }

  def closeBatchConnection(connection: Connection, statement: Statement): Unit = {
    try {
      if (connection != null) {
        statement.executeBatch()
      }
    } catch {
      case e: SQLException =>
        logError("SQLException while performing the commit for the task.")
        throw new RuntimeException(e);
    } finally {
      try {
        if (connection != null && statement != null) {
          statement.close()
          connection.close()
        }
      } catch {
        case ex: SQLException =>
          logError("SQLException while closing the connection for the task.");
          throw new RuntimeException(ex);
      }
    }
  }

  def getMetaData(table: String, prop: Properties): ResultSetMetaData = {
    val con = getConnection(prop)
    val prep = con.prepareStatement("select * from " + table + " where 1=0;")
    prep.executeQuery.getMetaData
  }

}
