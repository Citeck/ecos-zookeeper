package ru.citeck.ecos.zookeeper

import ecos.com.fasterxml.jackson210.databind.JavaType
import ecos.com.fasterxml.jackson210.databind.JsonNode
import ecos.com.fasterxml.jackson210.databind.node.NullNode
import ecos.com.fasterxml.jackson210.dataformat.cbor.CBORFactory
import ecos.curator.org.apache.zookeeper.*
import ecos.curator.org.apache.zookeeper.data.Stat
import ecos.org.apache.curator.framework.CuratorFramework
import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.json.exception.JsonMapperException
import ru.citeck.ecos.zookeeper.client.EcosZooKeeperClient
import ru.citeck.ecos.zookeeper.client.EcosZooKeeperClientProps
import ru.citeck.ecos.zookeeper.client.EcosZooKeeperWatcherKey
import ru.citeck.ecos.zookeeper.encoding.ContentEncoding
import ru.citeck.ecos.zookeeper.encoding.ZkContentEncoder
import ru.citeck.ecos.zookeeper.lock.EcosZkLock
import ru.citeck.ecos.zookeeper.lock.EcosZkLockImpl
import ru.citeck.ecos.zookeeper.mapping.ContentFormat
import ru.citeck.ecos.zookeeper.mapping.ZkContentMapper
import ru.citeck.ecos.zookeeper.value.ZkNodeContent
import ru.citeck.ecos.zookeeper.value.ZkNodePlainValue
import ru.citeck.ecos.zookeeper.value.ZkNodeValue
import ru.citeck.ecos.zookeeper.watcher.EcosZkWatcher
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.time.Instant

class EcosZooKeeper private constructor(
    private val props: EcosZooKeeperProperties,
    private val options: EcosZooKeeperOptions = EcosZooKeeperOptions.DEFAULT,
    private val client: EcosZooKeeperClient
) {

    companion object {

        private val log = KotlinLogging.logger {}

        private const val RAW_JSON_BYTES_BEGINING = "{\""

        private val zkNodeCborMapper = Json.newMapper {
            setFactory(CBORFactory())
            add(ZkNodeValue.Serializer())
            add(ZkNodeValue.Deserializer())
        }

        private val contentMapper = ZkContentMapper()
        private val contentEncoder = ZkContentEncoder()

        private fun createClient(props: EcosZooKeeperProperties): EcosZooKeeperClient {
            return EcosZooKeeperClient(
                EcosZooKeeperClientProps(
                    props.host,
                    props.port
                )
            )
        }
    }

    private var hasParent = false
    private val encoderOptions = contentEncoder.parseOptions(options.encoding, options.encodingOptions)

    @JvmOverloads
    constructor(
        props: EcosZooKeeperProperties,
        options: EcosZooKeeperOptions = EcosZooKeeperOptions.DEFAULT
    ) : this(
        props, options, createClient(props)
    )

    private constructor(
        parent: EcosZooKeeper,
        options: EcosZooKeeperOptions
    ) : this(
        parent.props,
        options,
        parent.client
    ) {
        hasParent = true
    }

    fun getClient(): CuratorFramework {
        return client.getClient(options.namespace)
    }

    fun withNamespace(ns: String): EcosZooKeeper {
        return withOptions { this.withNamespace(ns) }
    }

    fun withOptions(options: EcosZooKeeperOptions.Builder.() -> Unit): EcosZooKeeper {
        val newOptions = this.options.copy(options)
        return EcosZooKeeper(this, newOptions)
    }

    @JvmOverloads
    fun setValue(path: String, value: Any?, persistent: Boolean = true) {

        val now = Instant.now()

        val current: Stat? = getClient().checkExists().forPath(path)

        if (current == null) {

            getClient().create()
                .creatingParentContainersIfNeeded()
                .withMode(if (persistent) CreateMode.PERSISTENT else CreateMode.EPHEMERAL)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path, createNodeValue(value, now, now))
        } else {

            val currentValue = getValue(path, ZkNodeContent::class.java)
            val created = currentValue?.created ?: now

            getClient().setData()
                .withVersion(current.version)
                .forPath(path, createNodeValue(value, created, now))
        }
    }

    private fun createNodeValue(value: Any?, created: Instant, modified: Instant): ByteArray {
        val bytes = if (options.format == ContentFormat.JSON && options.encoding == ContentEncoding.PLAIN) {
            // legacy mode
            Json.mapper.toBytesNotNull(ZkNodePlainValue(created, modified, DataValue.create(value)))
        } else {
            val newValue = ZkNodeValue(
                options.format,
                options.encoding,
                createContentData(value, created, modified)
            )
            zkNodeCborMapper.toBytesNotNull(newValue)
        }
        return bytes
    }

    private fun createContentData(value: Any?, created: Instant, modified: Instant): ByteArray {
        val content = ZkNodeContent(DataValue.create(value), created, modified)
        val bout = ByteArrayOutputStream()
        contentMapper.writeValue(
            content,
            options.format,
            contentEncoder.enhanceOutput(bout, options.encoding, encoderOptions)
        )
        return bout.toByteArray()
    }

    @JvmOverloads
    fun deleteValue(path: String, recursive: Boolean = false) {
        try {
            if (recursive) {
                getClient().delete()
                    .deletingChildrenIfNeeded()
                    .forPath(path)
            } else {
                getClient().delete()
                    .forPath(path)
            }
        } catch (e: KeeperException.NoNodeException) {
            // already deleted. do nothing
        }
    }

    fun getValue(path: String): DataValue {
        return getValue(path, DataValue::class.java) ?: DataValue.NULL
    }

    fun <T : Any> getValue(path: String, type: Class<T>): T? {
        return getValue(path, Json.mapper.getType(type))
    }

    fun <T : Any> getValue(path: String, type: JavaType): T? {

        @Suppress("UNCHECKED_CAST")
        val clazz: Class<T> = type.rawClass as Class<T>

        if (clazz == Unit::class.java) {
            @Suppress("UNCHECKED_CAST")
            return Unit as T
        }

        val existsStat: Stat? = getClient().checkExists().forPath(path)
        var data: ByteArray? = null
        if (existsStat != null) {
            data = getClient().data.forPath(path)
        }
        if (data == null || data.isEmpty()) {
            if (clazz.isAssignableFrom(DataValue::class.java)) {
                return clazz.cast(DataValue.NULL)
            }
            if (clazz.isAssignableFrom(JsonNode::class.java)) {
                return clazz.cast(NullNode.getInstance())
            }
            return null
        }

        if (isRawJsonValue(data)) {
            val plainValueObj = try {
                Json.mapper.readNotNull(data, DataValue::class.java)
            } catch (e: JsonMapperException) {
                logMapperException(path, e)
                return null
            }
            val plainValue = ZkNodePlainValue(
                plainValueObj["created"].getAs(Instant::class.java) ?: Instant.EPOCH,
                plainValueObj["modified"].getAs(Instant::class.java) ?: Instant.EPOCH,
                plainValueObj["data"].getAs(DataValue::class.java) ?: DataValue.NULL
            )
            return if (clazz == ZkNodeContent::class.java) {
                @Suppress("UNCHECKED_CAST")
                ZkNodeContent(plainValue.data, plainValue.created, plainValue.modified) as T
            } else {
                if (plainValue.data.isNull()) {
                    return null
                }
                Json.mapper.convertNotNull(plainValue.data, type)
            }
        }

        val zNodeValue = try {
            zkNodeCborMapper.readNotNull(data, ZkNodeValue::class.java)
        } catch (e: JsonMapperException) {
            logMapperException(path, e)
            return null
        }

        var contentInStream: InputStream = ByteArrayInputStream(zNodeValue.content)
        contentInStream = contentEncoder.enhanceInput(contentInStream, zNodeValue.encoding)
        val content = contentMapper.readValue(contentInStream, zNodeValue.format)

        if (clazz == ZkNodeContent::class.java) {
            @Suppress("UNCHECKED_CAST")
            return content as? T
        }

        return Json.mapper.convertNotNull(content.value, type)
    }

    private fun logMapperException(path: String, e: JsonMapperException) {
        log.error(e) { "Exception while reading value by key $path. ZooKeeper options: $options" }
    }

    private fun isRawJsonValue(bytes: ByteArray): Boolean {
        if (bytes.size < RAW_JSON_BYTES_BEGINING.length) {
            return false
        }
        var idx = 0
        for (char in RAW_JSON_BYTES_BEGINING) {
            if (bytes[idx++].toUInt() != char.code.toUInt()) {
                return false
            }
        }
        return true
    }

    /**
     * Return children keys without full path.
     * You should use **getValue(path + "/" + getChildren(path).get(0))**
     * to get value of first child or use getChildren* methods with expected value type
     */
    fun getChildren(path: String): List<String> {
        return try {
            getClient().children.forPath(path).orEmpty()
        } catch (e: KeeperException.NoNodeException) {
            emptyList()
        }
    }

    fun <T : Any> getChildren(path: String, type: Class<T>): Map<String, T> {
        return getChildren(path, Json.mapper.getType(type))
    }

    fun <T : Any> getChildren(path: String, type: JavaType): Map<String, T> {

        val childrenKeys = getChildren(path)
        val result = linkedMapOf<String, T>()
        for (key in childrenKeys) {
            val childPath = if (path == "/") {
                "/$key"
            } else {
                "$path/$key"
            }
            getValue<T>(childPath, type)?.let { result[key] = it }
        }
        return result
    }

    fun watchChildren(path: String, action: (WatchedEvent) -> Unit) {
        watchChildrenWithWatcher(path, action)
    }

    fun watchChildrenWithWatcher(path: String, action: (WatchedEvent) -> Unit): EcosZkWatcher {
        return client.addWatcher(EcosZooKeeperWatcherKey(options.namespace, path, false), action)
    }

    fun watchChildrenRecursive(path: String, action: (WatchedEvent) -> Unit) {
        watchChildrenRecursiveWithWatcher(path, action)
    }

    fun watchChildrenRecursiveWithWatcher(path: String, action: (WatchedEvent) -> Unit): EcosZkWatcher {
        return client.addWatcher(EcosZooKeeperWatcherKey(options.namespace, path, true), action)
    }

    fun <T : Any> watchValue(path: String, type: Class<T>, action: (T?) -> Unit) {
        watchValueWithWatcher(path, type, action)
    }

    fun <T : Any> watchValueWithWatcher(path: String, type: Class<T>, action: (T?) -> Unit): EcosZkWatcher {
        return client.addWatcher(EcosZooKeeperWatcherKey(options.namespace, path, false)) {
            action.invoke(getValue(it.path, type))
        }
    }

    fun createLock(path: String): EcosZkLock {
        return EcosZkLockImpl(path, getClient())
    }

    fun dispose() {
        if (hasParent) {
            log.warn { "Disposing is not allowed. You should call dispose() on main EcosZooKeeper" }
        } else {
            client.dispose()
        }
    }

    fun getOptions(): EcosZooKeeperOptions {
        return options
    }
}
