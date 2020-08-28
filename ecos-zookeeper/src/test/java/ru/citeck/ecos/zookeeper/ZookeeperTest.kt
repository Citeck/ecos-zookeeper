package ru.citeck.ecos.zookeeper

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ZookeeperTest {

    var zkServer: TestingServer? = null

    @BeforeAll
    fun setUp() {
        zkServer = TestingServer(2181, true)
    }

    @AfterAll
    fun tearDown() {
        zkServer!!.stop()
    }

    @Test
    fun check() {
        val retryPolicy: RetryPolicy = RetryForever(7_000)

        val client = CuratorFrameworkFactory
            .newClient("localhost:2181", retryPolicy)
        client.start()
        val service = EcosZooKeeperService(client).withNamespace("ecos")

        service.watchChildren("/aa/bb") {
            println("TRIGGER = $it")
        }

        service.setValue("/aa/bb/cc", TestData("aaa", "bbb", 232))
        println(service.getValue("/aa/bb/cc", TestData::class.java))

        service.setValue("/aa/bb/cc", TestData("ccc", "ddd", 555))
        println(service.getValue("/aa/bb/cc", TestData::class.java))

        service.setValue("/aa/bb/eee", TestData("ccc", "ddd", 555))
        println(service.getValue("/aa/bb/eee", TestData::class.java))

        println(service.getChildren("/aa/bb"))
    }

    data class TestData(
        val field0: String,
        val field1: String,
        val field2: Int
    )
}
