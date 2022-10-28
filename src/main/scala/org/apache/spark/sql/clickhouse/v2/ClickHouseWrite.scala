package org.apache.spark.sql.clickhouse.v2

import com.droidhang.clickhouse.ClickHouseParams
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

case class ClickHouseWrite(schema: StructType,
                      params: ClickHouseParams) extends WriteBuilder with Write {
  override def build(): Write = this

  override def toBatch: BatchWrite = ClickHouseBatchWrite(schema, params)
}

case class ClickHouseBatchWrite(schema: StructType,
                           params: ClickHouseParams)
  extends BatchWrite with DataWriterFactory {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = this

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new ClickHouseDataWriter(partitionId, taskId, schema, params)
  }
}
