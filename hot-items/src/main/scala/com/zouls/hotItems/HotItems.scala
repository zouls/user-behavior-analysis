package com.zouls.hotItems

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 定义输入数据的样例类
 *
 * @param userId     用户ID
 * @param itemId     商品ID
 * @param categoryId 商品类别ID
 * @param behavior   用户行为
 * @param timestamp  事件事件
 */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

/**
 * 定义窗口聚合结果样例类
 *
 * @param itemId    商品ID
 * @param windowEnd 窗口结束时间
 * @param count     计数
 */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

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

/**
 *
 * 输入,输出,分区key,窗口类型
 * WindowFunction[IN, OUT, KEY, W <: Window]
 * 输入为CountAgg的输出
 */
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

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