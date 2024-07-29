package ru.citeck.ecos.zookeeper.value

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import ru.citeck.ecos.zookeeper.encoding.ContentEncoding
import ru.citeck.ecos.zookeeper.mapping.ContentFormat

/**
 * Value with metadata about format and encoding of content.
 * This value should contain less as possible fields because it is always saved without encoding.
 */
class ZkNodeValue(
    val format: ContentFormat,
    val encoding: ContentEncoding,
    val content: ByteArray
) {

    enum class Field(val shortName: String) {

        FORMAT("f"),
        ENCODING("e"),
        CONTENT("c");

        companion object {
            fun getByShortName(shortName: String?): Field? {
                shortName ?: return null
                return when (shortName) {
                    FORMAT.shortName -> FORMAT
                    ENCODING.shortName -> ENCODING
                    CONTENT.shortName -> CONTENT
                    else -> null
                }
            }
        }
    }

    class Serializer : StdSerializer<ZkNodeValue>(ZkNodeValue::class.java) {
        override fun serialize(value: ZkNodeValue, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeStartObject()
            gen.writeNumberField(Field.FORMAT.shortName, value.format.ordinal)
            gen.writeNumberField(Field.ENCODING.shortName, value.encoding.ordinal)
            gen.writeBinaryField(Field.CONTENT.shortName, value.content)
            gen.writeEndObject()
        }
    }

    class Deserializer : StdDeserializer<ZkNodeValue>(ZkNodeValue::class.java) {

        companion object {
            private val EMPTY_CONTENT = ByteArray(0)
        }

        override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ZkNodeValue {

            var format: ContentFormat = ContentFormat.JSON
            var encoding: ContentEncoding = ContentEncoding.PLAIN
            var content: ByteArray = EMPTY_CONTENT

            while (p.nextValue() != null) {
                val field = Field.getByShortName(p.currentName) ?: continue
                when (field) {
                    Field.FORMAT -> format = ContentFormat.values()[p.intValue]
                    Field.ENCODING -> encoding = ContentEncoding.values()[p.intValue]
                    Field.CONTENT -> content = p.binaryValue
                }
            }

            return ZkNodeValue(format, encoding, content)
        }
    }
}
