package ru.citeck.ecos.zookeeper.value

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import ru.citeck.ecos.commons.data.DataValue
import java.time.Instant

class ZkNodeContent(
    val value: DataValue,
    val created: Instant,
    val modified: Instant
) {

    enum class Field(val shortName: String) {

        VALUE("v"),
        CREATED("c"),
        MODIFIED("m");

        companion object {
            fun getByShortName(shortName: String?): Field? {
                shortName ?: return null
                return when (shortName) {
                    VALUE.shortName -> VALUE
                    CREATED.shortName -> CREATED
                    MODIFIED.shortName -> MODIFIED
                    else -> null
                }
            }
        }
    }

    class Serializer : StdSerializer<ZkNodeContent>(ZkNodeContent::class.java) {
        override fun serialize(value: ZkNodeContent, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeStartObject()
            gen.writeNumberField(Field.CREATED.shortName, value.created.toEpochMilli())
            gen.writeNumberField(Field.MODIFIED.shortName, value.modified.toEpochMilli())
            gen.writeFieldName(Field.VALUE.shortName)
            gen.writeTree(value.value.value)
            gen.writeEndObject()
        }
    }

    class Deserializer : StdDeserializer<ZkNodeContent>(ZkNodeContent::class.java) {

        override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ZkNodeContent {

            var value: DataValue = DataValue.NULL
            var created: Instant = Instant.EPOCH
            var modified: Instant = Instant.EPOCH

            while (p.nextValue() != null) {
                val name = p.currentName ?: continue
                val field = Field.getByShortName(name) ?: continue
                when (field) {
                    Field.CREATED -> created = Instant.ofEpochMilli(p.longValue)
                    Field.MODIFIED -> modified = Instant.ofEpochMilli(p.longValue)
                    Field.VALUE -> value = DataValue.create(p.readValueAsTree<JsonNode>())
                }
            }

            return ZkNodeContent(value, created, modified)
        }
    }
}
