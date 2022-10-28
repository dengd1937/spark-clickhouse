package org.apache.spark.sql.clickhouse.v2

import java.util.Objects

import org.apache.spark.sql.connector.write.WriterCommitMessage

class WriterCommitMessageImpl(partitionId: Int, taskId: Long) extends WriterCommitMessage {

  def getPartitionId: Int = partitionId

  def getTaskId: Long = taskId

  override def hashCode(): Int = Objects.hash(partitionId)

  override def toString: String = "WriterCommitMessageImpl{" +
    "partitionId=" + partitionId +
    ", taskId=" + taskId +
    '}'

}
