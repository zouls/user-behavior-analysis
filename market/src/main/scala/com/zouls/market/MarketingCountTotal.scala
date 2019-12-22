package com.zouls.market

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class MarketingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(window.getStart).toString
    val endTs = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()
    out.collect(MarketingViewCount(startTs, endTs, "app marketing", "total", count))
  }
}