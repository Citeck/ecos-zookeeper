package ru.citeck.ecos.zookeeper.lock

import ecos.org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import java.time.Duration
import java.util.concurrent.TimeUnit

class EcosZkLockImpl(
    private val path: String,
    private val impl: InterProcessSemaphoreMutex
) : EcosZkLock {

    override fun getPath(): String {
        return path
    }

    override fun acquire() {
        impl.acquire()
    }

    override fun acquire(timeout: Duration): Boolean {
        return impl.acquire(timeout.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun release() {
        return impl.release()
    }

    override fun isAcquiredInThisProcess(): Boolean {
        return impl.isAcquiredInThisProcess
    }
}
