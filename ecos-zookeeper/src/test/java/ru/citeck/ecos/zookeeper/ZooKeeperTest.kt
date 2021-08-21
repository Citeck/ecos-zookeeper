package ru.citeck.ecos.zookeeper

import ecos.curator.org.apache.zookeeper.KeeperException
import ecos.org.apache.curator.RetryPolicy
import ecos.org.apache.curator.framework.CuratorFrameworkFactory
import ecos.org.apache.curator.retry.RetryForever
import ecos.org.apache.curator.test.TestingServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*

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

        assertThat(getChildrenByPath("/aa/bb")).isEqualTo(expectedChildrenMap)

        val keyToRemove = "/aa/bb/cc"
        expectedChildrenMap.remove(keyToRemove)
        assertThat(expectedChildrenMap).hasSize(1)
        service.deleteValue(keyToRemove)

        assertThat(getChildrenByPath("/aa/bb")).isEqualTo(expectedChildrenMap)

        // should not throw exception
        service.deleteValue("/unknown/path/abc")

        assertThrows<KeeperException.NotEmptyException> {
            service.deleteValue("/aa/bb")
        }
        service.deleteValue("/aa/bb", true)
    }

    private fun getChildrenByPath(path: String): Map<String, TestData> {
        return service.getChildren(path, TestData::class.java)
            .entries
            .associate {
                "$path/${it.key}" to it.value!!
            }
    }

    data class TestData(
        val field0: String,
        val field1: String,
        val field2: Int
    )
}
