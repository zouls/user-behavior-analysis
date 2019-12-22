package com.zouls.hotItems


import java.sql.Timestamp

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * KeyedProcessFunction的实现类
 * 定义类输入流中的每个元素的处理原则
 * 触发定时器的执行操作等
 *
 * 具体来说:
 * 窗口中的每个元素都放入到状态列表中,同事给每个元素注册一个触发器
 * 当窗口触发的时候,将状态列表中的数据全部取出排序,取topN个,清空状态列表,然后按照指定格式输出
 *
 * key:是windowEnd为long
 * 输入ItemViewCount
 * 输出控制台打印所以用了string
 *
 * @param topSize 输出排名前几的商品
 */
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  /**
   * 状态列表
   */
  private var itemState: ListState[ItemViewCount] = _

  /**
   * 初始化操作,创建状态列表
   */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 创建一个状态列表,ListStateDescriptor构造方法,第一个参数是这个状态的唯一的名字,第二个参数是这个状态列表存的值的类型
    // getListState可以获取到状态列表
    itemState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount])
    )
  }

  /**
   * 处理输入流中的每一个元素
   * 对每一条数据的处理的函数:1.把每条数据存入状态列表 2.注册一个定时器
   *
   * @param value 输入的值
   * @param ctx   上下文对象,可以查询元素的时间戳,注册时间触发器
   * @param out   结果值的收集器
   */
  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {

    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  /**
   * 定时器触发时,对所有数据排序,并输出结果
   *
   * @param timestamp 触发计时器的时间,在timerService中注册的
   * @param ctx       上下文对象,可以查询元素的时间戳,注册时间触发器
   * @param out       结果值的收集器
   */
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    // 将所有的state的数据取出放到一个ListBuffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }
    // 按照count大小进行排序,并去除前topSize个
    val topN = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    // 清空状态
    itemState.clear()

    // 将排名结果格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    // 输出每一个商品的信息
    for (i <- topN.indices) {
      val itemViewCount = topN(i)
      result.append("No.").append(i + 1).append(":")
        .append(" 商品ID=").append(itemViewCount.itemId)
        .append(" 浏览量=").append(itemViewCount.count)
        .append("\n")
    }
    result.append("=======================")
    // 控制输出频率
    Thread.sleep(1000L)
    out.collect(result.toString())

  }
}