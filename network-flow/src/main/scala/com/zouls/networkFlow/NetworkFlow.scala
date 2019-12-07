package com.zouls.networkFlow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

/**
 * 热门页面统计
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

class UrlCount() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class UrlWindow() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class UrlTopN(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  // 不用open,用lazy,另外一种实现方式
  lazy val stateList: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

  //  override def open(parameters: Configuration): Unit = {
  //    stateList = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))
  //  }

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    stateList.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    var list: ListBuffer[UrlViewCount] = new ListBuffer
    //    import scala.collection.JavaConversions._
    val itr = stateList.get().iterator()
    while (itr.hasNext) {
      list += itr.next()
    }
    //    for (entity <- stateList.get()) {
    //      list += entity
    //    }
    stateList.clear()

    val sortedList = list.sortWith(_.count > _.count).take(topSize)
    //    val sortedList = list.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    val result: StringBuilder = new StringBuilder

    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedList.indices) {

      result
        .append("No").append(i + 1).append(":")
        .append(" url=").append(sortedList(i).url)
        .append(" 访问量=").append(sortedList(i).count).append("\n")
    }
    result.append("===============")
    Thread.sleep(1000L)
    out.collect(result.toString())
  }
}