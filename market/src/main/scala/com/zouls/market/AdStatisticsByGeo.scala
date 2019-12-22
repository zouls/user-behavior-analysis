package com.zouls.market

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * 页面广告点击量统计,根据用户的地理位置进行划分
 */
object AdStatisticsByGeo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 定义侧输出流的tag
    val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

    // 读取数据并转换成AdClickEvent
    val resource = getClass.getResource("/AdClickLog.csv")
    val adEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 自定义process function，过滤大量刷点击的行为
    val filterBlackListStream = adEventStream
      // 同时根据userId和adId进行分组
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100, blackListOutputTag))

    // 根据省份做分组，开窗聚合
    val adCountStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print("count")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blacklist")

    env.execute("ad statistics job")
  }
}
