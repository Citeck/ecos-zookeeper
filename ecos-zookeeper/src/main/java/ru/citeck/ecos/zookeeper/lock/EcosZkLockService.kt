package ru.citeck.ecos.zookeeper.lock

import mu.KotlinLogging
import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.webapp.api.lock.EcosLock
import ru.citeck.ecos.webapp.api.lock.EcosLockApi
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import java.util.concurrent.ConcurrentHashMap

class EcosZkLockService(scope: String, ecosZooKeeper: EcosZooKeeper) : EcosLockApi {

    companion object {
        private val NAME_ESC = NameUtils.getEscaperWithAllowedChars("-")

        private val log = KotlinLogging.logger {}
    }

    private val localZk = ecosZooKeeper.withNamespace("ecos/locks/${NAME_ESC.escape(scope)}")
    private val locks = ConcurrentHashMap<String, EcosLock>()

    override fun getLock(key: String): EcosLock {
        return locks.computeIfAbsent(key) { localZk.createLock("/" + NAME_ESC.escape(key)) }
    }
}
