package com.zouls.hotItems

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