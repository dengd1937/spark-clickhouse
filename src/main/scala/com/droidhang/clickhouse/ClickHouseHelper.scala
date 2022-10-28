package com.droidhang.clickhouse

import java.util

import com.google.common.base.Joiner
import com.google.common.collect.Lists
import org.apache.spark.internal.Logging

object ClickHouseHelper extends Serializable with Logging {

  /**
   * return precompiled insert sql
   */
  def getInsertStatement(target: String, columns: util.List[String]): String = {

    val parameterList = Lists.newArrayList[String]
    for (i <- 0 until columns.size) {
      parameterList.add("?")
    }

    String.format(
      "insert into %s (%s) values(%s)",
      target,
      Joiner.on(",")
        .join(columns),
      Joiner.on(",")
        .join(parameterList)
    )

  }

}
