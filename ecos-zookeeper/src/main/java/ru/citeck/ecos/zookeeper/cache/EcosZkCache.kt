package ru.citeck.ecos.zookeeper.cache

import ecos.com.fasterxml.jackson210.databind.JavaType
import ecos.org.apache.curator.framework.recipes.cache.CuratorCacheListener

interface EcosZkCache {

    fun <T : Any> getValue(path: String, type: Class<out T>): T?

    fun <T : Any> getValue(path: String, type: JavaType): T?

    fun addListener(listener: CuratorCacheListener)

    fun getChildren(path: String): List<String>
}
