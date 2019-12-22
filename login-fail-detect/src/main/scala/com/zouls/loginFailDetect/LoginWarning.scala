package com.zouls.loginFailDetect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  // 定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent])
  )

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {

    // 这种实现存在问题, 1.2秒检查一次,如果在2秒内多次触发报警,依然得等到2秒时间到了才能触发 2.即使已经多次触发报警,如果最后一次是成功的话依然会取消报警,但事实上第二次就应该触发报警了
    //    val loginFailList = loginFailState.get()
    //    // 判断类型是否是fail，只添加fail的事件到状态
    //    if( value.eventType == "fail" ){
    //      if( ! loginFailList.iterator().hasNext ){
    //        ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L + 2000L )
    //      }
    //      loginFailState.add( value )
    //    } else {
    //      // 如果是成功，清空状态
    //      loginFailState.clear()
    //    }

    //  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //    // 触发定时器的时候，根据状态里的失败个数决定是否输出报警
    //    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    //    val iter = loginFailState.get().iterator()
    //    while(iter.hasNext){
    //      allLoginFails += iter.next()
    //    }
    //
    //    // 判断个数
    //    if( allLoginFails.length >= maxFailTimes ){
    //      out.collect( Warning( allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length + " times." ) )
    //    }
    //    // 清空状态
    //    loginFailState.clear()
    //  }
    if (value.eventType == "fail") {
      // 如果是失败，判断之前是否有登录失败事件
      val iter = loginFailState.get().iterator()
      if (iter.hasNext) {
        // 如果已经有登录失败事件，就比较事件时间
        val firstFail = iter.next()
        if (value.eventTime < firstFail.eventTime + 2) {
          // 如果两次间隔小于2秒，输出报警
          out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds."))
        }
        // 更新最近一次的登录失败事件，保存在状态里
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        // 如果是第一次登录失败，直接添加到状态
        loginFailState.add(value)
      }
    } else {
      // 如果是成功，清空状态
      loginFailState.clear()
    }
  }


}