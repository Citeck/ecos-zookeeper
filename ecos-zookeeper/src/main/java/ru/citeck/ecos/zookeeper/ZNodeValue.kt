package ru.citeck.ecos.zookeeper

import ru.citeck.ecos.commons.data.DataValue
import java.time.Instant

class ZNodeValue(
    val created: Instant,
    val data: DataValue
)
