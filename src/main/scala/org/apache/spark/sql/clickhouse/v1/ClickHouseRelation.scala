package org.apache.spark.sql.clickhouse.v1

import com.droidhang.clickhouse.{ClickHouseHelper, ClickHouseParams}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

import collection.JavaConverters._

case class ClickHouseRelation(context: SQLContext,
                              options: ClickHouseParams,
                              userSchema: StructType)
  extends BaseRelation
  with InsertableRelation with Serializable with Logging {

  override def sqlContext: SQLContext = context

  override def schema: StructType = userSchema

  /**
   * write
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val insert = ClickHouseHelper.getInsertStatement(options.getTable, data.schema.fieldNames.toList.asJava)
    logWarning(s"precompiled sql: $insert")

    data.rdd
      .foreachPartition(partition => {
        if (partition.nonEmpty) {
          val recordWriter = new ClickHouseRecordWriter(options, insert, options.getBatchSize)
          recordWriter.write(partition)
        }
      })
  }

}
