package org.apache.spark.sql.clickhouse.v2

import java.sql.{Connection, PreparedStatement, SQLException}
import java.util

import com.droidhang.clickhouse.exception.UnsupportedDataTypeException
import com.droidhang.clickhouse.{ClickHouseHelper, ClickHouseParams}
import com.droidhang.clickhouse.util.JDBCUtil
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._

import collection.JavaConverters._

class ClickHouseDataWriter() extends DataWriter[InternalRow] with Logging{

  private var conn: Connection = _

  private var statement: PreparedStatement = _

  private var batchSize: Long = _

  private var fields: Array[StructField] = _

  private var numRecords = 0

  private var partitionId: Int = _

  private var taskId: Long = _

  def this(partitionId: Int, taskId: Long, schema: StructType, params: ClickHouseParams) {
    this()
    this.partitionId = partitionId
    this.taskId = taskId
    this.conn = JDBCUtil.getConnection(params.asConnectionProperties)
    this.statement = conn.prepareStatement(
      ClickHouseHelper.getInsertStatement(params.getTable, schema.fieldNames.toList.asJava)
    )
    this.fields = schema.fields
    this.batchSize = params.getBatchSize
  }

  override def write(record: InternalRow): Unit = {
    try {
      for (index <- fields.indices) {
        val dataType = fields(index).dataType
        dataType match {
          case StringType => statement.setString(index + 1, record.getUTF8String(index + 1).toString)
          case ByteType => statement.setByte(index + 1, record.getByte(index + 1))
          case ShortType => statement.setShort(index + 1, record.getShort(index + 1))
          case IntegerType => statement.setInt(index + 1, record.getInt(index + 1))
          case LongType => statement.setLong(index + 1, record.getLong(index + 1))
          case FloatType => statement.setFloat(index + 1, record.getFloat(index + 1))
          case DoubleType => statement.setDouble(index + 1, record.getDouble(index + 1))
          case _: DecimalType => statement.setBigDecimal(index + 1, BigDecimal(record.getDouble(index + 1)).bigDecimal)
          case _ =>
            throw new UnsupportedDataTypeException("Unsupported spark sql type: " + dataType + " insert to ch") // not support
        }
      }
      statement.addBatch()
      numRecords = numRecords + 1
      if (numRecords % batchSize == 0) {
        //每commitSize提交一次
        logWarning(s"==================${TaskContext.getPartitionId()} partition reached $numRecords will commit===================")
        statement.executeBatch()
        numRecords = 0
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("Exception while committing to database.", e)
    }

  }

  override def commit(): WriterCommitMessage = new WriterCommitMessageImpl(partitionId, taskId)

  override def abort(): Unit = {}

  override def close(): Unit = JDBCUtil.closeBatchConnection(conn, statement)

}
