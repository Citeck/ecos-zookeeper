package ru.citeck.ecos.zookeeper

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.zookeeper.*
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import java.time.Instant

class EcosZooKeeper(private val client: CuratorFramework) {

    fun withNamespace(ns: String) : EcosZooKeeper {
        return EcosZooKeeper(client.usingNamespace(ns))
    }

    @JvmOverloads
    fun setValue(path: String, value: Any?, persistent: Boolean = true) {

        val zNodeValue = ZNodeValue(Instant.now(), DataValue.create(value))
        val valueBytes = Json.mapper.toBytes(zNodeValue)

        val current = client.checkExists().forPath(path)

        if (current == null) {
            client.create()
                .creatingParentContainersIfNeeded()
                .withMode(if (persistent) CreateMode.PERSISTENT else CreateMode.EPHEMERAL)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path, valueBytes)
        } else {
            client.setData()
                .withVersion(current.version)
                .forPath(path, valueBytes)
        }
    }

    fun <T : Any> getValue(path: String, type: Class<T>) : T? {

        val data = client.data.forPath(path)
        val znodeValue = Json.mapper.read(data, ZNodeValue::class.java)

        return znodeValue?.data?.getAs(type)
    }

    fun getChildren(path: String) : List<String> {
        return try {
            client.children.forPath(path).orEmpty()
        } catch (e : KeeperException.NoNodeException) {
            emptyList()
        }
    }

    fun watchChildren(path: String, action: (List<String>) -> Unit) {
        client.watchers()
            .add()
            .withMode(AddWatchMode.PERSISTENT)
            .usingWatcher(CuratorWatcher { event ->
                val res = client.children.forPath(event.path)
                action.invoke(res ?: emptyList())
            }).forPath(path)
    }

    fun <T : Any> watchValue(path: String, type: Class<T>, action: (T?) -> Unit) {
        client.watchers()
            .add()
            .withMode(AddWatchMode.PERSISTENT)
            .usingWatcher(CuratorWatcher { event ->
                action.invoke(getValue(event.path, type))
            }).forPath(path)
    }
}
