package ru.citeck.ecos.zookeeper

import ecos.org.apache.curator.test.TestingServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ZookeeperCacheTest {

    @Test
    fun test() {

        val zkServer = TestingServer()
        val service = EcosZooKeeper(zkServer.connectString).withNamespace("webapps")

        val cache = service.createCache("/instances").build()
        val rootCache = service.createCache("/").build()

        val instanceInfoPath = "/emodel/123456"
        val value = cache.getValue(instanceInfoPath, InstanceInfo::class.java)
        assertThat(value).isNull()

        val newInfo = InstanceInfo("localhost", 1234)
        service.setValue("/instances$instanceInfoPath", newInfo)

        Thread.sleep(500)

        assertThat(cache.getValue(instanceInfoPath, InstanceInfo::class.java))
            .isEqualTo(newInfo)
            .isEqualTo(rootCache.getValue("/instances$instanceInfoPath", InstanceInfo::class.java))

        assertThat(cache.getChildren("/")).containsExactly("emodel")
        assertThat(cache.getChildren("/emodel")).containsExactly("123456")

        service.deleteValue("/instances$instanceInfoPath")

        Thread.sleep(500)

        val value3 = cache.getValue(instanceInfoPath, InstanceInfo::class.java)
        assertThat(value3).isNull()
    }

    data class InstanceInfo(
        val host: String,
        val port: Int
    )
}
