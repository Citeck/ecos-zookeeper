package ru.citeck.ecos.zookeeper

import com.github.luben.zstd.ZstdInputStream
import ecos.com.fasterxml.jackson210.dataformat.cbor.CBORFactory
import ecos.curator.org.apache.zookeeper.CreateMode
import ecos.curator.org.apache.zookeeper.KeeperException
import ecos.curator.org.apache.zookeeper.ZooDefs
import ecos.org.apache.curator.framework.CuratorFramework
import ecos.org.apache.curator.test.TestingServer
import mu.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.json.JsonMapper
import ru.citeck.ecos.zookeeper.encoding.ContentEncoding
import ru.citeck.ecos.zookeeper.lock.EcosZkLockService
import ru.citeck.ecos.zookeeper.mapping.ContentFormat
import ru.citeck.ecos.zookeeper.value.ZkNodeContent
import ru.citeck.ecos.zookeeper.value.ZkNodePlainValue
import ru.citeck.ecos.zookeeper.value.ZkNodeValue
import java.io.ByteArrayInputStream
import java.time.Duration
import java.time.Instant
import kotlin.concurrent.thread
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ZooKeeperTest {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private lateinit var zkServer: TestingServer

    private lateinit var service: EcosZooKeeper
    private lateinit var client: CuratorFramework

    @BeforeAll
    fun setUp() {
        zkServer = TestingServer()
        service = EcosZooKeeper(zkServer.connectString)
        client = service.getClient().usingNamespace("")
    }

    @Test
    fun abc() {
        val typesService = service.withNamespace("ecos/model/types")

        typesService.watchChildrenRecursive("/") { event ->
            println(event)
        }
        Thread.sleep(1000)
        typesService.setValue("/general-case", "abcd")
        typesService.setValue("/general-case", "abcd123")
        val children = typesService.getChildren("/")
        println(children)
    }

    @AfterAll
    fun tearDown() {
        zkServer.stop()
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

    @Test
    fun testLegacyValues() {

        val created = Instant.parse("2022-01-01T00:01:02Z")
        val modified = Instant.parse("2022-01-02T00:01:02Z")
        val data = DataValue.createObj()
            .set("str", "value")
            .set("num", 123)
            .set("boolTrue", true)
            .set("boolFalse", false)

        val legacyValue = ObjectData.create()
            .set("created", created)
            .set("modified", modified)
            .set("data", data)

        client.create()
            .creatingParentContainersIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
            .forPath("/ecos/config/legacy-test-config", Json.mapper.toBytes(legacyValue))

        val content = service.getValue("/config/legacy-test-config", ZkNodeContent::class.java)!!
        assertThat(content.created).isEqualTo(created)
        assertThat(content.modified).isEqualTo(modified)
        assertThat(content.value).isEqualTo(data)

        val dataFromService = service.getValue("/config/legacy-test-config", DataValue::class.java)!!
        assertThat(dataFromService).isEqualTo(data)
    }

    @Test
    fun formatEncodingTest() {

        val pathPrefix = "/format-enc-test/"

        val data = Json.mapper.read(
            ZooKeeperTest::class.java.getResourceAsStream("/test-data.json")!!.readBytes(),
            DataValue::class.java
        )

        val jsonPlain = FormatEncoding("json_plain", ContentFormat.JSON, ContentEncoding.PLAIN)
        val cborPlain = FormatEncoding("cbor_plain", ContentFormat.CBOR, ContentEncoding.PLAIN)
        val jsonZstd = FormatEncoding("json_zstd", ContentFormat.JSON, ContentEncoding.ZSTD)
        val cborZstd = FormatEncoding("cbor_zstd", ContentFormat.CBOR, ContentEncoding.ZSTD)

        val encFormatOptions = listOf(jsonPlain, cborPlain, jsonZstd, cborZstd)

        val services = encFormatOptions.associateWith {
            service.withOptions {
                withEncoding(it.encoding)
                withFormat(it.format)
            }
        }
        for (entry in services.entries) {
            entry.value.setValue(pathPrefix + entry.key.id, data)
        }
        for (entry in services.entries) {
            val dataFromService = entry.value.getValue(pathPrefix + entry.key.id, DataValue::class.java)
            assertThat(dataFromService).isEqualTo(data)
        }

        val cborMapper = Json.newMapper {
            setFactory(CBORFactory())
            add(ZkNodeValue.Deserializer())
        }
        val readZkNodeValue: (fmtEnc: FormatEncoding) -> ZkNodeValue = { fmtEnc ->
            val contentBytes = client.data.forPath("/ecos$pathPrefix${fmtEnc.id}")
            if (contentBytes[0].toInt().toChar() == '{') {
                val plainValue = Json.mapper.read(contentBytes, ZkNodePlainValue::class.java)!!
                val content = ObjectData.create()
                    .set(ZkNodeContent.Field.VALUE.shortName, plainValue.data)
                    .set(ZkNodeContent.Field.CREATED.shortName, plainValue.created)
                    .set(ZkNodeContent.Field.MODIFIED.shortName, plainValue.modified)
                ZkNodeValue(ContentFormat.JSON, ContentEncoding.PLAIN, Json.mapper.toBytes(content)!!)
            } else {
                cborMapper.read(contentBytes, ZkNodeValue::class.java)!!
            }
        }

        for (key in services.keys) {
            val value = readZkNodeValue(key)
            assertThat(value.encoding).isEqualTo(key.encoding)
            assertThat(value.format).isEqualTo(key.format)
            log.info { "${key.id} content size: ${value.content.size} bytes" }
        }

        val checkContentData = { contentData: ObjectData? ->

            assertThat(contentData).isNotNull
            contentData!!

            listOf("c", "m").forEach {
                assertThat(contentData.get(it).asText()).isNotBlank
            }
            assertThat(contentData.get("v")).isEqualTo(data)
        }

        val jsonValue = readZkNodeValue(jsonPlain)
        assertThat(readObjectOrNull(jsonValue.content, cborMapper)).isNull()
        val jsonContentData = Json.mapper.read(jsonValue.content, ObjectData::class.java)

        checkContentData(jsonContentData)

        val cborValue = readZkNodeValue(cborPlain)
        assertThat(readObjectOrNull(cborValue.content, Json.mapper)).isNull()
        val cborContentData = cborMapper.read(cborValue.content, ObjectData::class.java)

        checkContentData(cborContentData)

        val cborZstdValue = readZkNodeValue(cborZstd)
        val inputStream = ZstdInputStream(ByteArrayInputStream(cborZstdValue.content))
        val cborZstdContent = cborMapper.read(inputStream, ObjectData::class.java)

        checkContentData(cborZstdContent)

        assertThat(cborZstdValue.content.size).isLessThan(jsonValue.content.size / 2)
    }

    @Test
    fun scalarsTest() {

        val pathPrefix = "/scalars"

        service.setValue("$pathPrefix/str", "str-value")
        assertThat(service.getValue("$pathPrefix/str", String::class.java)).isEqualTo("str-value")

        service.setValue("$pathPrefix/int", 123)
        assertThat(service.getValue("$pathPrefix/int", Int::class.java)).isEqualTo(123)

        service.setValue("$pathPrefix/bool", true)
        assertThat(service.getValue("$pathPrefix/bool", Boolean::class.java)).isEqualTo(true)

        service.setValue("$pathPrefix/double", 123.123)
        assertThat(service.getValue("$pathPrefix/double", Double::class.java)).isEqualTo(123.123)

        service.setValue("$pathPrefix/null", null)
        assertThat(service.getValue("$pathPrefix/null", Any::class.java)).isNull()
    }

    @Test
    fun lockTest() {

        val lock0 = service.createLock("/lock-0")

        lock0.acquire()
        assertFalse(lock0.acquire(Duration.ZERO))
        lock0.release()
        assertTrue(lock0.acquire(Duration.ZERO))
        lock0.release()

        val lockPath = "/lock-path"

        val zkService0 = EcosZooKeeper(zkServer.connectString)
        val lockZk0 = zkService0.createLock(lockPath)
        val zkService1 = EcosZooKeeper(zkServer.connectString)
        val lockZk1 = zkService1.createLock(lockPath)

        lockZk0.acquire()
        assertFalse(lockZk1.acquire(Duration.ZERO))
        lockZk0.release()
        assertTrue(lockZk1.acquire(Duration.ZERO))
        lockZk1.release()

        lockZk0.acquire()

        val waitingStartedTime = System.currentTimeMillis()
        thread(start = true) {
            Thread.sleep(2000)
            lockZk0.release()
        }
        lockZk1.acquire(Duration.ofSeconds(10))
        val elapsedWaitingTime = System.currentTimeMillis() - waitingStartedTime

        assertThat(elapsedWaitingTime > 2000)
        assertThat(elapsedWaitingTime < 2100)
        lockZk1.release()

        val lockServiceTestStart = System.currentTimeMillis()

        var counter = 0
        val lockService = EcosZkLockService("some-scope", zkService0)
        val threads = (0 until 10).map {
            thread(start = false) {
                for (i in 0 until 10) {
                    lockService.doInSync("counter", Duration.ofSeconds(30)) {
                        counter += 1
                    }
                }
            }
        }
        threads.forEach { it.start() }
        threads.forEach { it.join() }

        log.info { "Lock service test duration: " + (System.currentTimeMillis() - lockServiceTestStart) }

        assertThat(counter).isEqualTo(100)
    }

    private fun readObjectOrNull(bytes: ByteArray, mapper: JsonMapper): ObjectData? {
        return try {
            mapper.read(bytes, ObjectData::class.java)
        } catch (e: Exception) {
            return null
        }
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

    data class FormatEncoding(
        val id: String,
        val format: ContentFormat,
        val encoding: ContentEncoding
    )
}
