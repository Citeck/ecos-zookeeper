package ru.citeck.ecos.zookeeper.cache

import com.fasterxml.jackson.databind.JavaType
import org.apache.curator.framework.recipes.cache.CuratorCacheListener

interface EcosZkCache {

    fun <T : Any> getValue(path: String, type: Class<out T>): T?

    fun <T : Any> getValue(path: String, type: JavaType): T?

    fun addListener(listener: CuratorCacheListener)

    fun getChildren(path: String): List<String>
}
