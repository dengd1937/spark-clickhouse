package com.droidhang.clickhouse

import java.util.Properties

import com.droidhang.clickhouse.ClickHouseConfig._
import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}
import collection.JavaConverters._


class ClickHouseParams(parameters: Map[String, String]) extends Serializable {

  def this(options: java.util.Map[String, String]) = this(options.asScala.toMap)

  // ------------------------------------------------------------
  // Required parameters
  // ------------------------------------------------------------
  private val table = parameters.getOrElse(CK_JDBC_TABLE_NAME, throw new IllegalArgumentException(s"Option '$CK_JDBC_TABLE_NAME' is required."))
  private val url = parameters.getOrElse(CK_JDBC_URL, throw new IllegalArgumentException(s"Option '$CK_JDBC_URL' is required."))


  // ------------------------------------------------------------
  // Optional parameters
  // ------------------------------------------------------------
  private val driver = parameters.getOrElse(CK_JDBC_DRIVER, CK_JDBC_DEFAULT_DRIVER)
  private val username = parameters.getOrElse(CK_JDBC_USERNAME, CK_JDBC_DEFAULT_USERNAME)
  private val password = parameters.getOrElse(CK_JDBC_PASSWORD, CK_JDBC_DEFAULT_PASSWORD)
  private val batchSize = parameters.getOrElse(CK_JDBC_BATCH_INSERT_SIZE, CK_JDBC_DEFAULT_BATCH_INSERT_SIZE).toLong

  // jdbc necessary configuration
  val asConnectionProperties: Properties = {
    val properties = new Properties()
    properties.put(CK_JDBC_DRIVER, driver)
    properties.put(CK_JDBC_URL, url)
    if (username != null) properties.put(CK_JDBC_USERNAME, username)
    if (password != null) properties.put(CK_JDBC_PASSWORD, password)
    properties
  }

  def getTable: String = table

  def getURL: String = url

  def getDriver: String = driver

  def getUsername: String = username

  def getPassword: String = password

  def getBatchSize: Long = batchSize

  override def toString: String = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
    .append(CK_JDBC_TABLE_NAME, table)
    .append(CK_JDBC_DRIVER, driver)
    .append(CK_JDBC_URL, url)
    .append(CK_JDBC_USERNAME, username)
    .append(CK_JDBC_PASSWORD, password)
    .append(CK_JDBC_BATCH_INSERT_SIZE, batchSize)
    .toString

}
