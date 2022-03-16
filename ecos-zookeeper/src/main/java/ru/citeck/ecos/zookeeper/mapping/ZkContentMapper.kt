package ru.citeck.ecos.zookeeper.mapping

import ecos.com.fasterxml.jackson210.dataformat.cbor.CBORFactory
import ru.citeck.ecos.commons.json.DeserFeature
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.json.JsonMapper
import ru.citeck.ecos.commons.json.JsonOptions
import ru.citeck.ecos.zookeeper.value.ZkNodeContent
import java.io.InputStream
import java.io.OutputStream

class ZkContentMapper {

    private val jsonMapper = Json.newMapper(
        JsonOptions.create {
            disable(DeserFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            add(ZkNodeContent.Serializer())
            add(ZkNodeContent.Deserializer())
        }
    )
    private val cborMapper = Json.newMapper(
        JsonOptions.create {
            setFactory(CBORFactory())
            disable(DeserFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            add(ZkNodeContent.Serializer())
            add(ZkNodeContent.Deserializer())
        }
    )

    fun writeValue(value: ZkNodeContent, format: ContentFormat, output: OutputStream) {
        getMapper(format).write(output, value)
    }

    fun readValue(input: InputStream, format: ContentFormat): ZkNodeContent {
        return getMapper(format).read(input, ZkNodeContent::class.java)
            ?: error("Incorrect data format. Expected: $format")
    }

    private fun getMapper(format: ContentFormat): JsonMapper {
        return when (format) {
            ContentFormat.CBOR -> cborMapper
            ContentFormat.JSON -> jsonMapper
        }
    }
}
