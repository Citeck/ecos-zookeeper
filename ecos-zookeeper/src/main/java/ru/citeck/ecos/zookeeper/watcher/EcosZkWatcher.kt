package ru.citeck.ecos.zookeeper.watcher

interface EcosZkWatcher {

    /**
     * Remove this watcher.
     * After this action watcher won't receive any events
     */
    fun remove()
}
