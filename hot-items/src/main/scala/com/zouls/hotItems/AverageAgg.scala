package com.zouls.hotItems

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * 自定义预聚合函数计算平均数
 */
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Long), Double] {
  override def createAccumulator(): (Long, Long) = (0L, 0L)

  override def add(value: UserBehavior, accumulator: (Long, Long)): (Long, Long) =
    (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def getResult(accumulator: (Long, Long)): Double = accumulator._1 / accumulator._2

  override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = (a._1 + b._1, a._2 + b._2)
}