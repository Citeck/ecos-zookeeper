package ru.citeck.ecos.zookeeper

import ecos.com.fasterxml.jackson210.databind.annotation.JsonDeserialize
import ru.citeck.ecos.commons.data.DataValue
import java.time.Instant

@JsonDeserialize(builder = ZNodeValue.Builder::class)
class ZNodeValue(
    val created: Instant,
    val modified: Instant,
    val data: DataValue
) {
    companion object {

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): ZNodeValue {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    fun copy(): Builder {
        return Builder(this)
    }

    fun copy(builder: Builder.() -> Unit): ZNodeValue {
        val builderObj = Builder(this)
        builder.invoke(builderObj)
        return builderObj.build()
    }

    class Builder() {

        var created: Instant = Instant.EPOCH
        var modified: Instant = Instant.EPOCH
        var data: DataValue = DataValue.NULL

        constructor(base: ZNodeValue) : this() {
            this.created = base.created
            this.modified = base.modified
            this.data = base.data.copy()
        }

        fun withCreated(created: Instant?): Builder {
            this.created = created ?: Instant.EPOCH
            return this
        }

        fun withModified(modified: Instant?): Builder {
            this.modified = modified ?: Instant.EPOCH
            return this
        }

        fun withData(data: DataValue?): Builder {
            this.data = data ?: DataValue.NULL
            return this
        }

        fun build(): ZNodeValue {
            return ZNodeValue(created, modified, data)
        }
    }
}
