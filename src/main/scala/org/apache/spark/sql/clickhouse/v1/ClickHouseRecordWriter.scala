package org.apache.spark.sql.clickhouse.v1

import java.io.{ByteArrayInputStream, DataInputStream}
import java.sql.{Connection, PreparedStatement, SQLException}

import com.clickhouse.client.data.ClickHouseBitmap
import com.droidhang.clickhouse.ClickHouseParams
import com.droidhang.clickhouse.util.JDBCUtil
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BinaryType
import org.roaringbitmap.RoaringBitmap

class ClickHouseRecordWriter extends Logging with Serializable {

  private var conn: Connection = _

  private var statement: PreparedStatement = _

  private var batchSize: Long = _

  private var numRecords = 0


  def this(params: ClickHouseParams, insertQuery: String, batchSize: Long) = {
    this()
    this.conn = JDBCUtil.getConnection(params.asConnectionProperties)
    this.statement = conn.prepareStatement(insertQuery)
    this.batchSize = batchSize
  }

  def write(partition: Iterator[Row]): Unit = {
    partition.foreach(row => {
      try {
        row.schema.fields.foreach(f => {
          val index = row.fieldIndex(f.name)
          f.dataType match {
            case BinaryType =>
              val rb: RoaringBitmap = binaryToBitmap(row, index)
              statement.setObject(index + 1, ClickHouseBitmap.wrap(rb.toArray: _*))
            case _ =>
              statement.setObject(index + 1, row.get(index))
          }
        })
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
          println("error data: " + row.toString())
          throw new RuntimeException("Exception while committing to database.", e)
      }
    })
    close()
  }

  private def binaryToBitmap(row: Row, index: Int) = {
    val rb = RoaringBitmap.bitmapOf(0)
    val value = row.get(index).asInstanceOf[Array[Byte]]
    if (value != null) {
      val bis = new ByteArrayInputStream(value)
      val bitmap = new RoaringBitmap()
      bitmap.deserialize(new DataInputStream(bis))
      rb.or(bitmap)
    }
    rb
  }

  private def close(): Unit = JDBCUtil.closeBatchConnection(conn, statement)

}
