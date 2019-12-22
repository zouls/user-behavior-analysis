package com.zouls.hotItems


/**
 * 定义窗口聚合结果样例类
 *
 * @param itemId    商品ID
 * @param windowEnd 窗口结束时间
 * @param count     计数
 */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)