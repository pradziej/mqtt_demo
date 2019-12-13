package com.proximetry.mqtt

object Tools {
    fun buildTopicPath(mainTopic: String, vararg subTopic: String): String {
        val s = subTopic.joinToString(separator = "/")
        return "$mainTopic/$s"
    }
}