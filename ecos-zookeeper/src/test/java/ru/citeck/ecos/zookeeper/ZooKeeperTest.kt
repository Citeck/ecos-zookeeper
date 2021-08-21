package ru.citeck.ecos.zookeeper

import ecos.org.apache.curator.RetryPolicy
import ecos.org.apache.curator.framework.CuratorFrameworkFactory
import ecos.org.apache.curator.retry.RetryForever
import ecos.org.apache.curator.test.TestingServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ZooKeeperTest {

    private var zkServer: TestingServer? = null

    private lateinit var service : EcosZooKeeper

    @BeforeAll
    fun setUp() {
        zkServer = TestingServer()

        val retryPolicy: RetryPolicy = RetryForever(7_000)

        val client = CuratorFrameworkFactory
            .newClient(zkServer!!.connectString, retryPolicy)
        client.start()

        service = EcosZooKeeper(client).withNamespace("ecos")
    }

    @AfterAll
    fun tearDown() {
        zkServer!!.stop()
    }

    @Test
    fun check() {

        service.watchChildren("/aa/bb") {
            println("TRIGGER = $it")
        }
        service.getChildren("/aa/bb/cc/dd/ee")

        val valuesByPath = listOf(
            "/aa/bb/cc" to TestData("aaa", "bbb", 232),
            "/aa/bb/cc" to TestData("ccc", "ddd", 555),
            "/aa/bb/eee" to TestData("ccc", "ууу", 123),
        )

        valuesByPath.forEach {
            service.setValue(it.first, it.second)
            val valueFromService = service.getValue(it.first, TestData::class.java)!!
            assertThat(valueFromService).isEqualTo(it.second)
        }

        val expectedChildrenMap = hashMapOf<String, TestData>()
        valuesByPath.forEach { expectedChildrenMap[it.first] = it.second }

        val children: Map<String, TestData> = service.getChildren("/aa/bb").map {
            val key = "/aa/bb/$it"
            key to service.getValue(key, TestData::class.java)!!
        }.toMap(hashMapOf())

        assertThat(children).isEqualTo(expectedChildrenMap)
    }

    data class TestData(
        val field0: String,
        val field1: String,
        val field2: Int
    )
}
