package ru.citeck.ecos.zookeeper

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.zookeeper.test.EcosZooKeeperTest

class ZookeeperCacheTest {

    @Test
    fun test() {

        val ecosZooKeeper = EcosZooKeeperTest.getZooKeeper().withNamespace("webapps")

        val cache = ecosZooKeeper.createCache("/instances").build()
        val rootCache = ecosZooKeeper.createCache("/").build()

        val instanceInfoPath = "/emodel/123456"
        val value = cache.getValue(instanceInfoPath, InstanceInfo::class.java)
        assertThat(value).isNull()

        val newInfo = InstanceInfo("localhost", 1234)
        ecosZooKeeper.setValue("/instances$instanceInfoPath", newInfo)

        Thread.sleep(500)

        assertThat(cache.getValue(instanceInfoPath, InstanceInfo::class.java))
            .isEqualTo(newInfo)
            .isEqualTo(rootCache.getValue("/instances$instanceInfoPath", InstanceInfo::class.java))

        assertThat(cache.getChildren("/")).containsExactly("emodel")
        assertThat(cache.getChildren("/emodel")).containsExactly("123456")

        ecosZooKeeper.deleteValue("/instances$instanceInfoPath")

        Thread.sleep(500)

        val value3 = cache.getValue(instanceInfoPath, InstanceInfo::class.java)
        assertThat(value3).isNull()
    }

    data class InstanceInfo(
        val host: String,
        val port: Int
    )
}
