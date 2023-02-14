package ru.citeck.ecos.zookeeper.test

import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.test.commons.containers.container.zookeeper.ZooKeeperContainer
import ru.citeck.ecos.test.commons.listener.EcosTestExecutionListener
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import ru.citeck.ecos.zookeeper.EcosZooKeeperProperties
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

object EcosZooKeeperTest {

    private var zooKeeper = Collections.synchronizedMap(LinkedHashMap<Pair<Any, Thread>, EcosZooKeeper>())
    private var containerByZooKeeper = Collections.synchronizedMap(IdentityHashMap<EcosZooKeeper, ZooKeeperContainer>())
    private val mainContainerReserved = AtomicBoolean()

    @JvmStatic
    fun createZooKeeper(): EcosZooKeeper {
        return createZooKeeper("", true) {}
    }

    @JvmStatic
    @JvmOverloads
    fun createZooKeeper(key: Any, closeAfterTest: Boolean = true): EcosZooKeeper {
        return createZooKeeper(key, closeAfterTest) {}
    }

    @JvmStatic
    @JvmOverloads
    fun createZooKeeper(closeAfterTest: Boolean = true, beforeClose: () -> Unit): EcosZooKeeper {
        return createZooKeeper("", closeAfterTest, beforeClose)
    }

    @JvmStatic
    @JvmOverloads
    fun createZooKeeper(key: Any, closeAfterTest: Boolean = true, beforeClose: () -> Unit): EcosZooKeeper {
        val container = TestContainers.getZooKeeper(key)
        val props = EcosZooKeeperProperties(container.getHost(), container.getMainPort())
        val zooKeeper = EcosZooKeeper(props)
        containerByZooKeeper[zooKeeper] = container
        val wasClosed = AtomicBoolean(false)
        val closeImpl = {
            if (wasClosed.compareAndSet(false, true)) {
                beforeClose.invoke()
                zooKeeper.dispose()
                container.release()
                containerByZooKeeper.remove(zooKeeper)
            }
        }
        if (closeAfterTest) {
            EcosTestExecutionListener.doWhenExecutionFinished { _, _ -> closeImpl() }
        }
        container.doBeforeStop(closeImpl)
        return zooKeeper
    }

    @JvmStatic
    fun getContainer(ecosZooKeeper: EcosZooKeeper): ZooKeeperContainer? {
        return containerByZooKeeper[ecosZooKeeper]
    }

    @JvmStatic
    @JvmOverloads
    @Synchronized
    fun getZooKeeper(key: Any = ""): EcosZooKeeper {
        val thread = Thread.currentThread()
        val zkKey = key to thread
        val zooKeeper = this.zooKeeper[zkKey]
        if (zooKeeper == null) {
            val nnZooKeeper = createZooKeeper(key, true) { this.zooKeeper.remove(zkKey) }
            if (key == "" && mainContainerReserved.compareAndSet(false, true)) {
                val container = containerByZooKeeper[nnZooKeeper]!!
                container.reserve()
                EcosTestExecutionListener.doWhenTestPlanExecutionFinished { container.release() }
            }
            this.zooKeeper[zkKey] = nnZooKeeper
            return nnZooKeeper
        }
        return zooKeeper
    }
}
