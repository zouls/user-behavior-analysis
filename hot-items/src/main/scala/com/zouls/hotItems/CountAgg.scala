package com.zouls.hotItems

import org.apache.flink.api.common.functions.AggregateFunction

/**
 *
 * 自定义预聚合函数
 * 用这个函数是因为不能简单的用sum来求和,因为输入的时case class 不是一个简单的数值
 *
 * 三个泛型: [IN, ACC, OUT] 输入类型(被聚合的数据),计数器类型(中间聚合状态),输出类型(聚合结果)
 * AggregateFunction这个接口可以实现灵活的聚合方法
 * 它的三个值:输入,输出,中间状态的类型可以都不一样,你可以自己任何实现其中的逻辑
 * 它也支持分布式的状态的合并
 */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}