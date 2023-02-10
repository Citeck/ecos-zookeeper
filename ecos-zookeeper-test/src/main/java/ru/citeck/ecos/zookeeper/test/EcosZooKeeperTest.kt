package ru.citeck.ecos.zookeeper.test

import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.test.commons.listener.EcosTestExecutionListener
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import ru.citeck.ecos.zookeeper.EcosZooKeeperProperties

object EcosZooKeeperTest {

    private var zooKeeper: EcosZooKeeper? = null

    fun createZooKeeper(): EcosZooKeeper {
        return createZooKeeper {}
    }

    fun createZooKeeper(afterClosed: () -> Unit): EcosZooKeeper {
        val container = TestContainers.getZooKeeper()
        val props = EcosZooKeeperProperties(container.getHost(), container.getMainPort())
        val zooKeeper = EcosZooKeeper(props)
        this.zooKeeper = zooKeeper
        EcosTestExecutionListener.doWhenExecutionFinished { _, _ ->
            zooKeeper.dispose()
            afterClosed.invoke()
        }
        return zooKeeper
    }

    fun getZooKeeper(): EcosZooKeeper {
        val zooKeeper = this.zooKeeper
        if (zooKeeper == null) {
            val nnZooKeeper = createZooKeeper {
                this.zooKeeper = null
            }
            this.zooKeeper = nnZooKeeper
            return nnZooKeeper
        }
        return zooKeeper
    }
}
