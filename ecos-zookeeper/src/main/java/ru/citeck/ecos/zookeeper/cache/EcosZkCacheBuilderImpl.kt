package ru.citeck.ecos.zookeeper.cache

import ecos.com.fasterxml.jackson210.databind.JavaType
import ecos.org.apache.curator.framework.recipes.cache.CuratorCache
import ecos.org.apache.curator.framework.recipes.cache.CuratorCacheBuilder
import ecos.org.apache.curator.framework.recipes.cache.CuratorCacheListener
import ru.citeck.ecos.commons.json.Json
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.streams.toList

class EcosZkCacheBuilderImpl(
    private val basePath: String,
    private val builder: CuratorCacheBuilder,
    private val readNodeValue: (path: String, data: ByteArray?, type: JavaType) -> Any?
) : EcosZkCacheBuilder {

    override fun build(): EcosZkCache {
        return EcosZkCacheImpl(builder.build())
    }

    private inner class EcosZkCacheImpl(private val cache: CuratorCache) : EcosZkCache {

        private val convertedValues = ConcurrentHashMap<CacheKey, ConvertedValue<*>>()
        private val baseCachePath = if (basePath == "/") { "" } else { basePath }

        init {
            cache.start()
            cache.listenable().addListener { type, valueBefore, data ->
                if (type == CuratorCacheListener.Type.NODE_DELETED) {
                    val path = valueBefore?.path ?: data?.path
                    if (path.isNullOrBlank()) {
                        val iterator = convertedValues.entries.iterator()
                        while (iterator.hasNext()) {
                            val entry = iterator.next()
                            if (entry.key.path == path) {
                                iterator.remove()
                            }
                        }
                    }
                }
            }
        }

        override fun <T : Any> getValue(path: String, type: Class<out T>): T? {
            return getValue(path, Json.mapper.getType(type))
        }

        override fun <T : Any> getValue(path: String, type: JavaType): T? {

            val valuePath = if (path == "/") {
                baseCachePath.ifEmpty { "/" }
            } else {
                baseCachePath + path
            }
            val childData = cache.get(valuePath).orElse(null) ?: return null
            val convertedValueCacheKey = CacheKey(path, type)
            val convertedValue = convertedValues[convertedValueCacheKey]
            val nodeDataVersion = childData.stat.version

            val resultValue = if (convertedValue == null || convertedValue.version != nodeDataVersion) {
                val newValue = readNodeValue(path, childData.data, Json.mapper.getType(type)) ?: return null
                convertedValues[convertedValueCacheKey] = ConvertedValue(newValue, nodeDataVersion)
                newValue
            } else {
                convertedValue.value
            }
            @Suppress("UNCHECKED_CAST")
            return resultValue as? T
        }

        override fun getChildren(path: String): List<String> {
            val pathToSearchChildren = if (path == "/") {
                baseCachePath
            } else {
                "$baseCachePath$path"
            } + "/"
            return cache.stream().filter {
                val childPath = it.path
                childPath.startsWith(pathToSearchChildren) &&
                    childPath.length > pathToSearchChildren.length &&
                    childPath.indexOf('/', pathToSearchChildren.length + 1) == -1
            }.map {
                it.path.substringAfterLast('/')
            }.toList()
        }
    }

    private class ConvertedValue<T : Any>(
        val value: T,
        val version: Int
    )

    private data class CacheKey(
        val path: String,
        val type: JavaType
    )
}
