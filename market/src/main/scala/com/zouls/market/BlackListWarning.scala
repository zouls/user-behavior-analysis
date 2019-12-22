package com.zouls.market

// 输出的黑名单报警信息
case class BlackListWarning(userId: Long, adId: Long, msg: String)