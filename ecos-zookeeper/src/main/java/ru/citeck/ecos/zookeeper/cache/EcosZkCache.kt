package ru.citeck.ecos.zookeeper.cache

import ecos.com.fasterxml.jackson210.databind.JavaType

interface EcosZkCache {

    fun <T : Any> getValue(path: String, type: Class<out T>): T?

    fun <T : Any> getValue(path: String, type: JavaType): T?

    fun getChildren(path: String): List<String>
}
