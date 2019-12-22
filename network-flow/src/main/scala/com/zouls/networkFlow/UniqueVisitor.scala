package com.zouls.networkFlow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * UV统计
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //        val resource = getClass.getResource("/UserBehavior.csv")
    //        val dataStream = env.readTextFile(resource.getPath)
    val dataStream = env.readTextFile("file:///C:/Users/zouls/Projects/flink/user-behavior-analysis/hot-items/src/main/resources/UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(
          dataArray(0).trim.toLong,
          dataArray(1).trim.toLong,
          dataArray(2).trim.toInt,
          dataArray(3).trim,
          dataArray(4).trim.toLong
        )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    dataStream.print()
    env.execute("UniqueVisitor")
  }
}
