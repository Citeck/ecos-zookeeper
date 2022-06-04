package ru.citeck.ecos.zookeeper.watcher

class EcosZkWatcherImpl(
    private val removeAction: () -> Unit
) : EcosZkWatcher {

    override fun remove() {
        removeAction()
    }
}
