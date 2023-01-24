package ru.citeck.ecos.zookeeper.lock

import ecos.org.apache.curator.framework.CuratorFramework
import ecos.org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import mu.KotlinLogging
import java.time.Duration
import java.util.concurrent.TimeUnit

class EcosZkLockImpl(
    private val path: String,
    private val client: CuratorFramework
) : EcosZkLock {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val mutex = InterProcessSemaphoreMutex(client, path)

    override fun getKey(): String {
        return path
    }

    override fun acquire(timeout: Duration): Boolean {
        return mutex.acquire(timeout.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun release() {
        if (mutex.isAcquiredInThisProcess) {
            try {
                mutex.release()
            } catch (e: Throwable) {
                log.warn(e) { "Exception while lock releasing. Path: '$path' Client namespace: '${client.namespace}'" }
            }
        } else {
            log.warn { "Mutex is not acquired in this process. Path: $path" }
        }
    }

    override fun isAcquiredInThisProcess(): Boolean {
        return mutex.isAcquiredInThisProcess
    }

    override fun dispose() {
        release()
    }
}
