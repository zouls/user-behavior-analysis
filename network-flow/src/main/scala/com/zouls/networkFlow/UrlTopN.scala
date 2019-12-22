package com.zouls.networkFlow

import java.sql.Timestamp

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class UrlTopN(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  // 不用open,用lazy,另外一种实现方式
  lazy val stateList: ListState[UrlViewCount] = getRuntimeContext.getListState(
    new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount])
  )

  //  override def open(parameters: Configuration): Unit = {
  //    stateList = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))
  //  }

  override def processElement(value: UrlViewCount,
                              ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    stateList.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

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