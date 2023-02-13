package ru.citeck.ecos.zookeeper.test

import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.test.commons.containers.container.zookeeper.ZooKeeperContainer
import ru.citeck.ecos.test.commons.listener.EcosTestExecutionListener
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import ru.citeck.ecos.zookeeper.EcosZooKeeperProperties
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

object EcosZooKeeperTest {

    private var zooKeeper = Collections.synchronizedMap(IdentityHashMap<Thread, EcosZooKeeper>())

    @JvmStatic
    @JvmOverloads
    fun createZooKeeper(closeAfterTest: Boolean = true): EcosZooKeeper {
        return createZooKeeper(closeAfterTest) {}
    }

    @JvmStatic
    @JvmOverloads
    fun createZooKeeper(closeAfterTest: Boolean = true, beforeClose: () -> Unit): EcosZooKeeper {
        val container = getContainer()
        val props = EcosZooKeeperProperties(container.getHost(), container.getMainPort())
        val zooKeeper = EcosZooKeeper(props)
        val wasClosed = AtomicBoolean(false)
        val closeImpl = {
            if (wasClosed.compareAndSet(false, true)) {
                beforeClose.invoke()
                zooKeeper.dispose()
            }
        }
        if (closeAfterTest) {
            EcosTestExecutionListener.doWhenExecutionFinished { _, _ -> closeImpl() }
        }
        container.doBeforeStop(closeImpl)
        return zooKeeper
    }

    fun getContainer(): ZooKeeperContainer {
        return TestContainers.getZooKeeper()
    }

    @JvmStatic
    @Synchronized
    fun getZooKeeper(): EcosZooKeeper {
        val thread = Thread.currentThread()
        val zooKeeper = this.zooKeeper[thread]
        if (zooKeeper == null) {
            val nnZooKeeper = createZooKeeper(false) { this.zooKeeper.remove(thread) }
            this.zooKeeper[thread] = nnZooKeeper
            return nnZooKeeper
        }
        return zooKeeper
    }
}
