package com.zouls.hotItems

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * 实时热门商品统计
 */
object HotItems {
  def main(args: Array[String]): Unit = {

    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 全局并行度设置为1
    env.setParallelism(1)
    // 设置流使用的时间是EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.1.110:9092")
    properties.setProperty("group.id", "test")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val myConsumer = new FlinkKafkaConsumer[String]("zouls-test", new SimpleStringSchema(), properties)

    // 从文件中读取
    //    val inputPath = "file:///C:/Users/zouls/Projects/flink/user-behavior-analysis/hot-items/src/main/resources/UserBehavior.csv"
    //    val dataStream = env.readTextFile(inputPath)

    // 从kafka中读取
    val dataStream = env.addSource(myConsumer)
      // 2.1 转换数据格式为UserBehavior
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
      // 2.2 设置时间语义,从数据中提取某个字段作为时间和watermark,该csv的数据是升序排列的数据,是有序的数据
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3.transform 处理数据
    val processedStream = dataStream
      // 3.1 过滤除了pv之外的数据
      .filter(_.behavior == "pv")
      // 3.2 根据itemId进行keyBy:区分不同的商品,分别统计每种商品的访问量
      .keyBy(_.itemId)
      // 3.3 滑动窗口, 统计一小时内的数据,每5秒统计一次:统计的时间段和频次,就是将分组后的访问量又按照时间窗口进行再一次的细分
      .timeWindow(Time.hours(1), Time.seconds(5))
      // 3.4 进行聚合,聚合成一个特殊的数据结构,同时加上windowEnd;第一个参数是预聚合,第二的参数输出想要的结果
      //   3.4.1 计算该商品在这个时间段的访问量的总和:CountAgg
      //   3.4.2 将这个总和连同窗口信息封装成指定的输出格式:WindowResult
      .aggregate(new CountAgg(), new WindowResult())
      // 3.5 按照窗口信息进行分组,这时候数据就变成了 <窗口1:list[item1的sum,item2的sum]>
      .keyBy(_.windowEnd)
      // 3.6
      .process(new TopNHotItems(3))

    // 4.sink
    processedStream.print()
    env.execute("HotItems")
  }
}
