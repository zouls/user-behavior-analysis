package com.zouls.networkFlow

import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class UvCountByWindow extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个scala set,用于保存所有的数据的userId并去重
    // 这里可能会有问题,如果一个小时内的数据量太大可能会撑爆内存
    var idSet = Set[Long]()

    // 把当前窗口中所有数据的ID收集到set中,最后输出set的大小
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }

    out.collect(UvCount(window.getEnd, idSet.size))
  }
}