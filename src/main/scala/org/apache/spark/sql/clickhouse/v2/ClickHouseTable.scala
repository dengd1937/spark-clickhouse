package org.apache.spark.sql.clickhouse.v2

import java.util

import com.droidhang.clickhouse.ClickHouseParams
import com.google.common.collect.Lists
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

case class ClickHouseTable(tableSchema: StructType, params: ClickHouseParams)
  extends Table
    with SupportsWrite {

  override def name(): String = params.getTable

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = new util.HashSet[TableCapability](
    Lists.newArrayList(TableCapability.BATCH_WRITE)
  )

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ClickHouseWrite(tableSchema, params)

}
