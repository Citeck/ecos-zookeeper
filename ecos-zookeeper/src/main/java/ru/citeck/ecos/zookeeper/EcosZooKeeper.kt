package ru.citeck.ecos.zookeeper

import ecos.curator.org.apache.zookeeper.*
import ecos.curator.org.apache.zookeeper.data.Stat
import ecos.org.apache.curator.framework.CuratorFramework
import ecos.org.apache.curator.framework.api.CuratorWatcher
import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import java.time.Instant
import java.util.concurrent.TimeUnit

class EcosZooKeeper(private val innerClient: CuratorFramework) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private var initialized = false

    private fun getClient(): CuratorFramework {
        if (!initialized) {
            if (!innerClient.blockUntilConnected(2, TimeUnit.SECONDS)) {
                do {
                    log.warn { "Waiting until ZooKeeper will be available" }
                } while (!innerClient.blockUntilConnected(1, TimeUnit.MINUTES))
            }
            initialized = true
        }
        return innerClient
    }

    fun withNamespace(ns: String): EcosZooKeeper {
        return EcosZooKeeper(innerClient.usingNamespace(ns))
    }

    @JvmOverloads
    fun setValue(path: String, value: Any?, persistent: Boolean = true) {

        val now = Instant.now()

        val current: Stat? = getClient().checkExists().forPath(path)

        if (current == null) {

            val newValue = ZNodeValue(now, now, DataValue.create(value))

            getClient().create()
                .creatingParentContainersIfNeeded()
                .withMode(if (persistent) CreateMode.PERSISTENT else CreateMode.EPHEMERAL)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path, Json.mapper.toBytes(newValue))
        } else {

            val currentValue = getValue(path, ZNodeValue::class.java)
                ?: error("Existence was checked but null value was returned by path $path")

            val newValue = ZNodeValue(currentValue.created, now, DataValue.create(value))

            getClient().setData()
                .withVersion(current.version)
                .forPath(path, Json.mapper.toBytes(newValue))
        }
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
        if (data.isEmpty()) {
            return null
        }
        val znodeValue = Json.mapper.read(data, ZNodeValue::class.java)

        if (type == ZNodeValue::class.java) {
            @Suppress("UNCHECKED_CAST")
            return znodeValue as T?
        }

        return znodeValue?.data?.getAs(type)
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
}
