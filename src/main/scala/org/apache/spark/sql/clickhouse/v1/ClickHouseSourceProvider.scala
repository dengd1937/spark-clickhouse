package org.apache.spark.sql.clickhouse.v1

import com.droidhang.clickhouse.ClickHouseParams
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class ClickHouseSourceProvider extends CreatableRelationProvider
  with DataSourceRegister{

  /**
   * save
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {
    if (!mode.equals(SaveMode.Append)) {
      throw new Exception("only support Append mode!")
    }

    val options = new ClickHouseParams(parameters)
    println(options.toString)
    val relation = ClickHouseRelation(sqlContext, options, data.schema)
    relation.insert(data, overwrite = false)
    relation
  }

  override def shortName(): String = "clickhouse_v1"

}
