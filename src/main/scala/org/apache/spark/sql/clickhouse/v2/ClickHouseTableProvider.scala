package org.apache.spark.sql.clickhouse.v2

import java.util

import com.droidhang.clickhouse.ClickHouseParams
import com.droidhang.clickhouse.util.SchemaUtil
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class ClickHouseTableProvider extends TableProvider with DataSourceRegister{

  private var params: ClickHouseParams = _

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    this.params = new ClickHouseParams(options)
    try {
      SchemaUtil.getSchema(params)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }

  override def getTable(schema: StructType, partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = ClickHouseTable(schema, params)

  override def shortName(): String = "clickhouse"
}
