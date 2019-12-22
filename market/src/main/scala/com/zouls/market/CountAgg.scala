package com.zouls.market

import org.apache.flink.api.common.functions.AggregateFunction

class CountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}