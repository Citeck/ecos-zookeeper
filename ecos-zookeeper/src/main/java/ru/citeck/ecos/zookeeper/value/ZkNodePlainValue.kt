package ru.citeck.ecos.zookeeper.value

import ru.citeck.ecos.commons.data.DataValue
import java.time.Instant

class ZkNodePlainValue(
    val created: Instant,
    val modified: Instant,
    val data: DataValue
)
