package ru.citeck.ecos.zookeeper.lock

import java.time.Duration

/**
 * A NON re-entrant mutex that works across JVMs. Uses Zookeeper to hold the lock.
 * All processes in all JVMs that use the same lock path will achieve an inter-process critical section.
 */
interface EcosZkLock {

    fun getPath(): String

    /**
     * Acquire the mutex - blocking until it's available.
     * Each call to acquire must be balanced by a call
     * to [.release]
     *
     * @throws Exception ZK errors, connection interruptions
     */
    fun acquire()

    /**
     * Acquire the mutex - blocks until it's available or the given time expires.
     * Each call to acquire that returns true must be balanced by a call
     * to [.release]
     *
     * @param timeout time to wait
     * @return true if the mutex was acquired, false if not
     * @throws Exception ZK errors, connection interruptions
     */
    fun acquire(timeout: Duration): Boolean

    /**
     * Perform one release of the mutex.
     *
     * @throws Exception ZK errors, interruptions
     */
    fun release()

    /**
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    fun isAcquiredInThisProcess(): Boolean
}
