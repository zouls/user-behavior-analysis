package com.zouls.market

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class FilterBlackListUser(maxCount: Int, blackListOutputTag: OutputTag[BlackListWarning]) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
  // 定义状态，保存当前用户对当前广告的点击量
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
  // 保存是否发送过黑名单的状态
  lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))
  // 保存定时器触发的时间戳
  lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

  override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
    // 取出count状态
    val curCount = countState.value()

    // 如果是第一次处理，注册定时器，每天00：00触发
    if (curCount == 0) {
      val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
      resetTimer.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    // 判断计数是否达到上限，如果到达则加入黑名单
    if (curCount >= maxCount) {
      // 判断是否发送过黑名单，只发送一次
      if (!isSentBlackList.value()) {
        isSentBlackList.update(true)
        // 输出到侧输出流
        ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today."))
      }
      return
    }
    // 计数状态加1，输出数据到主流
    countState.update(curCount + 1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
    // 定时器触发时，清空状态
    if (timestamp == resetTimer.value()) {
      isSentBlackList.clear()
      countState.clear()
      resetTimer.clear()
    }
  }
}