package ru.citeck.ecos.zookeeper

import ecos.com.fasterxml.jackson210.dataformat.cbor.CBORFactory
import ecos.curator.org.apache.zookeeper.*
import ecos.curator.org.apache.zookeeper.data.Stat
import ecos.org.apache.curator.framework.CuratorFramework
import ecos.org.apache.curator.framework.CuratorFrameworkFactory
import ecos.org.apache.curator.framework.api.CuratorWatcher
import ecos.org.apache.curator.retry.RetryForever
import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.zookeeper.encoding.ContentEncoding
import ru.citeck.ecos.zookeeper.encoding.ZkContentEncoder
import ru.citeck.ecos.zookeeper.mapping.ContentFormat
import ru.citeck.ecos.zookeeper.mapping.ZkContentMapper
import ru.citeck.ecos.zookeeper.value.ZkNodeContent
import ru.citeck.ecos.zookeeper.value.ZkNodePlainValue
import ru.citeck.ecos.zookeeper.value.ZkNodeValue
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class EcosZooKeeper private constructor(
    private val props: EcosZooKeeperProperties,
    private val options: EcosZooKeeperOptions = EcosZooKeeperOptions.DEFAULT,
    innerClient: CuratorFramework
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

        private fun createClient(props: EcosZooKeeperProperties): CuratorFramework {
            return createClient("${props.host}:${props.port}", props)
        }

        private fun createClient(connectString: String, props: EcosZooKeeperProperties): CuratorFramework {
            val retryPolicy = RetryForever(
                Duration.ofSeconds(5).toMillis().toInt()
            )
            log.info {
                "\n" +
                    "================Ecos Zookeeper Init======================\n" +
                    "URL: $connectString\n" +
                    "Startup will be stopped until Zookeeper will be available\n" +
                    "=========================================================\n"
            }
            val client = CuratorFrameworkFactory
                .newClient(connectString, retryPolicy)
            client.start()
            return client
        }
    }

    private var initialized = AtomicBoolean()
    private val innerClient: CuratorFramework = innerClient.usingNamespace(options.namespace)

    private val encoderOptions = contentEncoder.parseOptions(options.encoding, options.encodingOptions)

    @JvmOverloads
    constructor(
        props: EcosZooKeeperProperties,
        options: EcosZooKeeperOptions = EcosZooKeeperOptions.DEFAULT
    ) : this(
        props, options, createClient(props)
    )

    @JvmOverloads
    constructor(
        connectString: String,
        props: EcosZooKeeperProperties = EcosZooKeeperProperties(),
        options: EcosZooKeeperOptions = EcosZooKeeperOptions.DEFAULT
    ) : this(
        props, options, createClient(connectString, props)
    )

    private constructor(
        parent: EcosZooKeeper,
        options: EcosZooKeeperOptions
    ) : this(
        parent.props,
        options,
        parent.innerClient
    )

    fun getClient(): CuratorFramework {
        if (!initialized.get()) {
            if (!innerClient.blockUntilConnected(2, TimeUnit.SECONDS)) {
                do {
                    log.warn { "Waiting until ZooKeeper will be available" }
                } while (!innerClient.blockUntilConnected(1, TimeUnit.MINUTES))
            }
            initialized.set(true)
        }
        return innerClient
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
                ?: error("Existence was checked but null value was returned by path $path")

            getClient().setData()
                .withVersion(current.version)
                .forPath(path, createNodeValue(value, currentValue.created, now))
        }
    }

    private fun createNodeValue(value: Any?, created: Instant, modified: Instant): ByteArray {
        val bytes = if (options.format == ContentFormat.JSON && options.encoding == ContentEncoding.PLAIN) {
            // legacy mode
            Json.mapper.toBytes(ZkNodePlainValue(created, modified, DataValue.create(value)))
        } else {
            val newValue = ZkNodeValue(
                options.format,
                options.encoding,
                createContentData(value, created, modified)
            )
            zkNodeCborMapper.toBytes(newValue)
        }
        return bytes ?: error(
            "Error while conversion of ZkNodeValue to bytes. " +
                "Created: $created Value: $value Options: $options"
        )
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

    fun <T : Any> getValue(path: String, type: Class<T>): T? {

        if (type == Unit::class.java) {
            @Suppress("UNCHECKED_CAST")
            return Unit as T
        }

        val data = getClient().data.forPath(path)
        if (data == null || data.isEmpty()) {
            return null
        }

        if (isRawJsonValue(data)) {
            val plainValueObj = Json.mapper.read(data, DataValue::class.java) ?: return null
            val plainValue = ZkNodePlainValue(
                plainValueObj.get("created").getAs(Instant::class.java) ?: Instant.EPOCH,
                plainValueObj.get("modified").getAs(Instant::class.java) ?: Instant.EPOCH,
                plainValueObj.get("data").getAs(DataValue::class.java) ?: DataValue.NULL
            )
            return if (type == ZkNodeContent::class.java) {
                @Suppress("UNCHECKED_CAST")
                ZkNodeContent(plainValue.data, plainValue.created, plainValue.modified) as? T
            } else {
                plainValue.data.getAs(type)
            }
        }

        val zNodeValue = zkNodeCborMapper.read(data, ZkNodeValue::class.java) ?: return null

        var contentInStream: InputStream = ByteArrayInputStream(zNodeValue.content)
        contentInStream = contentEncoder.enhanceInput(contentInStream, zNodeValue.encoding)
        val content = contentMapper.readValue(contentInStream, zNodeValue.format)

        if (type == ZkNodeContent::class.java) {
            @Suppress("UNCHECKED_CAST")
            return content as? T
        }

        return content.value.getAs(type)
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

    fun <T : Any> getChildren(path: String, type: Class<T>): Map<String, T?> {
        val childrenKeys = getChildren(path)
        val result = linkedMapOf<String, T?>()
        for (key in childrenKeys) {
            result[key] = getValue("$path/$key", type)
        }
        return result
    }

    fun watchChildren(path: String, action: (WatchedEvent) -> Unit) {
        getClient().watchers()
            .add()
            .withMode(AddWatchMode.PERSISTENT)
            .usingWatcher(CuratorWatcher { action.invoke(it) })
            .forPath(path)
    }

    fun watchChildrenRecursive(path: String, action: (WatchedEvent) -> Unit) {
        getClient().watchers()
            .add()
            .withMode(AddWatchMode.PERSISTENT_RECURSIVE)
            .usingWatcher(CuratorWatcher { action.invoke(it) })
            .forPath(path)
    }

    fun <T : Any> watchValue(path: String, type: Class<T>, action: (T?) -> Unit) {
        getClient().watchers()
            .add()
            .withMode(AddWatchMode.PERSISTENT)
            .usingWatcher(
                CuratorWatcher { event ->
                    action.invoke(getValue(event.path, type))
                }
            ).forPath(path)
    }

    fun getOptions(): EcosZooKeeperOptions {
        return options
    }
}
