package ru.citeck.ecos.zookeeper.encoding

import com.github.luben.zstd.ZstdInputStream
import com.github.luben.zstd.ZstdOutputStream
import ru.citeck.ecos.commons.data.ObjectData
import java.io.InputStream
import java.io.OutputStream

class ZkContentEncoder {

    fun parseOptions(encoding: ContentEncoding, options: ObjectData): Options {
        return when (encoding) {
            ContentEncoding.PLAIN -> EmptyOptions
            ContentEncoding.ZSTD -> options.getAs(ZstdOptions::class.java)
        } ?: error("Incorrect options: $options")
    }

    fun enhanceOutput(output: OutputStream, encoding: ContentEncoding, options: Options): OutputStream {
        return when (encoding) {
            ContentEncoding.PLAIN -> output
            ContentEncoding.ZSTD -> {
                options as ZstdOptions
                ZstdOutputStream(output, options.level)
            }
        }
    }

    fun enhanceInput(input: InputStream, encoding: ContentEncoding): InputStream {
        return when (encoding) {
            ContentEncoding.PLAIN -> input
            ContentEncoding.ZSTD -> ZstdInputStream(input)
        }
    }

    class ZstdOptions(
        val level: Int = 3
    ) : Options

    object EmptyOptions : Options

    interface Options
}
