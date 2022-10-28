package com.droidhang.clickhouse.util

import com.droidhang.clickhouse.ClickHouseParams
import com.droidhang.clickhouse.exception.UnsupportedDataTypeException
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object SchemaUtil {

  private val arrayTypePattern: Regex = """^Array\((.+)\)$""".r
  private val mapTypePattern: Regex = """^Map\((\w+),\s*(.+)\)$""".r
  private val dateTypePattern: Regex = """^Date$""".r
  private val dateTimeTypePattern: Regex = """^DateTime(64)?(\((.*)\))?$""".r
  private val decimalTypePattern: Regex = """^Decimal\((\d+),\s*(\d+)\)$""".r
  private val decimalTypePattern2: Regex = """^Decimal(32|64|128|256)\((\d+)\)$""".r
  private val enumTypePattern: Regex = """^Enum(8|16)\((.*)\)""".r
  private val fixedStringTypePattern: Regex = """^FixedString\((\d+)\)$""".r
  private val nullableTypePattern: Regex = """^Nullable\((.*)\)""".r

  def getSchema(params: ClickHouseParams): StructType = {
    val metaData = JDBCUtil.getMetaData(params.getTable, params.asConnectionProperties)
    val fields = new ArrayBuffer[StructField]()
    for (i <- 1 to metaData.getColumnCount) {
      val columnName = metaData.getColumnName(i)
      val columnTypeName = metaData.getColumnTypeName(i)
      val (dataType, nullable) = getSqlDataType(columnTypeName)
      fields += StructField(columnName, dataType, nullable)
    }
    StructType(fields)
  }

  def getSqlDataType(typeName: String): (DataType, Boolean) = {
    val (unwrapTypeName, nullable) = unwrapNullable(typeName)
    val columnType = unwrapTypeName match {
      case "String" | "UUID" | "IPv6" | fixedStringTypePattern(_*) | enumTypePattern(_*) => StringType
      case "Int8" => ByteType
      case "UInt8" | "Int16" => ShortType
      case "UInt16" | "Int32" => IntegerType
      case "UInt32" | "Int64" | "UInt64" | "IPv4" => LongType
      case "Int128" | "Int256" | "UInt256" =>
        throw new UnsupportedDataTypeException("Unsupported ck type: " + typeName + " convert to spark sql data type") // not support
      case "Float32" => FloatType
      case "Float64" => DoubleType
      case dateTypePattern() => DateType
      case dateTimeTypePattern(_, _, _) => TimestampType
      case decimalTypePattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case decimalTypePattern2(w, scale) => w match {
        case "32" => DecimalType(9, scale.toInt)
        case "64" => DecimalType(18, scale.toInt)
        case "128" => DecimalType(38, scale.toInt)
        case "256" => DecimalType(76, scale.toInt) // throw exception, spark support precision up to 38
      }
      case arrayTypePattern(nestedChType) =>
        val (_type, _nullable) = getSqlDataType(nestedChType)
        ArrayType(_type, _nullable)
      case mapTypePattern(keyChType, valueChType) =>
        val (_keyType, _keyNullable) = getSqlDataType(keyChType)
        require(!_keyNullable, s"Illegal type: $keyChType, the key type of Map should not be nullable")
        val (_valueType, _valueNullable) = getSqlDataType(valueChType)
        MapType(_keyType, _valueType, _valueNullable)
      case _ =>
        throw new UnsupportedDataTypeException("Unsupported ch type: " + typeName + " convert to spark sql data type") // not support
    }
    (columnType, nullable)
  }

  def unwrapNullable(mayNullableTypeName: String): (String, Boolean) = {
    mayNullableTypeName match {
      case nullableTypePattern(t) => (t, true)
      case _ => (mayNullableTypeName, false)
    }
  }

}
