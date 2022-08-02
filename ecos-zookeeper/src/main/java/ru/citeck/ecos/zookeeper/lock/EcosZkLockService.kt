package ru.citeck.ecos.zookeeper.lock

import mu.KotlinLogging
import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.webapp.api.lock.EcosLockService
import ru.citeck.ecos.webapp.api.lock.exception.AcquireTimeoutException
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

class EcosZkLockService(
    private val scope: String,
    ecosZooKeeper: EcosZooKeeper
) : EcosLockService {

    companion object {
        private val NAME_ESC = NameUtils.getEscaperWithAllowedChars("-")

        private val log = KotlinLogging.logger {}
    }

    private val localZk = ecosZooKeeper.withNamespace("ecos/locks/${NAME_ESC.escape(scope)}")
    private val locks = ConcurrentHashMap<String, EcosZkLock>()

    override fun <T> doInSync(key: String, timeout: Duration, action: () -> T): T {
        val lock = getLock(key)
        if (!lock.acquire(timeout)) {
            throw AcquireTimeoutException(key, timeout)
        }
        try {
            return action.invoke()
        } finally {
            release(lock)
        }
    }

    override fun doInSyncOrSkip(key: String, timeout: Duration, action: () -> Unit): Boolean {
        val lock = getLock(key)
        if (!lock.acquire(timeout)) {
            return false
        }
        try {
            action.invoke()
        } finally {
            release(lock)
        }
        return true
    }

    private fun getLock(key: String): EcosZkLock {
        return locks.computeIfAbsent(key) { localZk.createLock("/" + NAME_ESC.escape(key)) }
    }

    private fun release(lock: EcosZkLock) {
        try {
            lock.release()
        } catch (e: Exception) {
            log.warn(e) { "Exception while lock releasing. Path: '${lock.getPath()}' Lock service scope: '$scope'" }
        }
    }
}
