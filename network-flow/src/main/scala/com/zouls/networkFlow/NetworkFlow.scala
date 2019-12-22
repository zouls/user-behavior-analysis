package com.zouls.networkFlow

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * 热门页面统计,获取前TOP N个
 */
object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    val dataStream = env.readTextFile("file:///C:/Users/zouls/Projects/flink/user-behavior-analysis/network-flow/src/main/resources/apache.log")
      .map(data => {
        val dataArray = data.split(" ")
        ApacheLogEvent(
          dataArray(0),
          "",
          sdf.parse(dataArray(3)).getTime,
          dataArray(5),
          dataArray(6))
      })
      //        .print()
      // 数据的时间是乱序的

      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](
        //设定延迟时间,与具体的数据的乱序程度有关,这边延迟就不设置的特别高了,实际数据延迟可能有1分钟的
        Time.seconds(1)
      ) {
        // 定义一下用哪个字段来提取时间戳
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
    //      .assignAscendingTimestamps(_.eventTime)
    val processedStream = dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      // 允许延迟60秒
      .allowedLateness(Time.seconds(60))
      .aggregate(new UrlCount(), new UrlWindow())
      .keyBy(_.windowEnd)
      .process(new UrlTopN(5))

    processedStream.print()

    env.execute("NetworkFlow")
  }
}