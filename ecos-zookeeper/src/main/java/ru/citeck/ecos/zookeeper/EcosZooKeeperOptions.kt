package ru.citeck.ecos.zookeeper

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.zookeeper.encoding.ContentEncoding
import ru.citeck.ecos.zookeeper.mapping.ContentFormat

data class EcosZooKeeperOptions(
    val namespace: String,
    val format: ContentFormat,
    val encoding: ContentEncoding,
    val encodingOptions: ObjectData
) {
    companion object {

        val DEFAULT = EcosZooKeeperOptions(
            "ecos",
            ContentFormat.JSON,
            ContentEncoding.PLAIN,
            ObjectData.create()
        )

        private val log = KotlinLogging.logger {}
    }

    fun copy(action: Builder.() -> Unit): EcosZooKeeperOptions {
        val builder = Builder(this)
        action(builder)
        return builder.build()
    }

    class Builder() {

        var namespace: String = DEFAULT.namespace
        var format: ContentFormat = DEFAULT.format
        var encoding: ContentEncoding = DEFAULT.encoding
        var encodingOptions: ObjectData = ObjectData.create()

        constructor(base: EcosZooKeeperOptions) : this() {
            this.namespace = base.namespace
            this.format = base.format
            this.encoding = base.encoding
            this.encodingOptions = base.encodingOptions.deepCopy()
        }

        fun withNamespace(namespace: String?): Builder {
            this.namespace = namespace ?: DEFAULT.namespace
            return this
        }

        fun withFormat(format: ContentFormat?): Builder {
            this.format = format ?: DEFAULT.format
            return this
        }

        fun withEncoding(encoding: ContentEncoding?): Builder {
            this.encoding = encoding ?: DEFAULT.encoding
            return this
        }

        fun withEncodingOptions(encodingOptions: Any?): Builder {
            this.encodingOptions = encodingOptions?.let { ObjectData.create(it) } ?: ObjectData.create()
            return this
        }

        fun build(): EcosZooKeeperOptions {
            return EcosZooKeeperOptions(
                namespace = namespace,
                format = format,
                encoding = encoding,
                encodingOptions = encodingOptions
            )
        }
    }
}
