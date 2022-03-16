package ru.citeck.ecos.zookeeper

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.utils.LibsUtils
import ru.citeck.ecos.zookeeper.encoding.ContentEncoding
import ru.citeck.ecos.zookeeper.mapping.ContentFormat

data class EcosZooKeeperConfig(
    val namespace: String,
    val format: ContentFormat,
    val encoding: ContentEncoding,
    val encodingOptions: ObjectData
) {
    companion object {

        val DEFAULT = EcosZooKeeperConfig(
            EcosZooKeeper.DEFAULT_NAMESPACE,
            ContentFormat.JSON,
            evalDefaultEncoding(),
            ObjectData.create()
        )

        private const val ZSTD_CLASS_TO_CHECK = "com.github.luben.zstd.ZstdOutputStream"
        private val SUPPORTED_ZSTD_LIBS = listOf("com.github.luben:zstd-jni")

        private val log = KotlinLogging.logger {}

        private fun evalDefaultEncoding(): ContentEncoding {
            return if (LibsUtils.isClassPresent(ZSTD_CLASS_TO_CHECK)) {
                ContentEncoding.ZSTD
            } else {
                log.warn {
                    "ZSTD library is not found in classpath. " +
                        "ZooKeeper values will be saved without compression. " +
                        "Supported ZSTD libraries: $SUPPORTED_ZSTD_LIBS"
                }
                ContentEncoding.PLAIN
            }
        }
    }

    fun copy(action: Builder.() -> Unit): EcosZooKeeperConfig {
        val builder = Builder(this)
        action(builder)
        return builder.build()
    }

    class Builder() {

        var namespace: String = DEFAULT.namespace
        var format: ContentFormat = DEFAULT.format
        var encoding: ContentEncoding = DEFAULT.encoding
        var encodingOptions: ObjectData = ObjectData.create()

        constructor(base: EcosZooKeeperConfig) : this() {
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

        fun build(): EcosZooKeeperConfig {
            return EcosZooKeeperConfig(
                namespace = namespace,
                format = format,
                encoding = encoding,
                encodingOptions = encodingOptions
            )
        }
    }
}
