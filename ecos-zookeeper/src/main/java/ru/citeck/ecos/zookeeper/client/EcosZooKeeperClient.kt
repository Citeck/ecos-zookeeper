package ru.citeck.ecos.zookeeper.client

import ecos.curator.org.apache.zookeeper.AddWatchMode
import ecos.curator.org.apache.zookeeper.WatchedEvent
import ecos.curator.org.apache.zookeeper.Watcher.Event.EventType
import ecos.org.apache.curator.SessionFailedRetryPolicy
import ecos.org.apache.curator.framework.CuratorFramework
import ecos.org.apache.curator.framework.CuratorFrameworkFactory
import ecos.org.apache.curator.framework.api.CuratorWatcher
import ecos.org.apache.curator.framework.state.ConnectionState
import ecos.org.apache.curator.retry.RetryForever
import mu.KotlinLogging
import ru.citeck.ecos.zookeeper.watcher.EcosZkWatcher
import ru.citeck.ecos.zookeeper.watcher.EcosZkWatcherImpl
import java.lang.Exception
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

internal class EcosZooKeeperClient(props: EcosZooKeeperClientProps) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val client: CuratorFramework
    private val eventListeners = ConcurrentHashMap<EcosZooKeeperWatcherKey, MutableList<(WatchedEvent) -> Unit>>()
    private val eventsQueue = LinkedBlockingQueue<WatcherEvent>()
    private val clientDisposed = AtomicBoolean()

    private val initialized = AtomicBoolean()
    private val connectionLost = AtomicBoolean()
    private val reconnectedAfterLostActionsRequired = AtomicBoolean()

    private val reconnectedListeners: MutableList<() -> Unit> = CopyOnWriteArrayList()

    init {
        val retryPolicy = SessionFailedRetryPolicy(
            RetryForever(
                Duration.ofSeconds(5).toMillis().toInt()
            )
        )
        val connectString = props.host + ":" + props.port
        log.info {
            "\n" +
                "================Ecos Zookeeper Init======================\n" +
                "URL: $connectString\n" +
                "Startup will be stopped until Zookeeper will be available\n" +
                "=========================================================\n"
        }
        client = CuratorFrameworkFactory
            .newClient(connectString, retryPolicy)
        client.start()

        client.connectionStateListenable.addListener { _, newState ->
            if (newState == ConnectionState.LOST) {
                connectionLost.set(true)
            } else if (connectionLost.get() && newState == ConnectionState.RECONNECTED) {
                connectionLost.set(false)
                reconnectedAfterLostActionsRequired.set(true)
            }
        }

        thread(name = "ecos-zookeeper-events", start = true) {
            while (!clientDisposed.get()) {
                if (reconnectedAfterLostActionsRequired.compareAndSet(true, false)) {
                    log.info { "Reinitialize watchers" }
                    synchronized(eventListeners) {
                        eventListeners.forEach { (k, _) ->
                            registerWatcher(k)
                        }
                    }
                    log.info { "Call reconnected listeners" }
                    reconnectedListeners.forEach { listener ->
                        try {
                            listener.invoke()
                        } catch (e: Throwable) {
                            log.error("Error in reconnection listener", e)
                        }
                    }
                }
                val event = eventsQueue.poll(1, TimeUnit.SECONDS)
                if (event != null) {
                    synchronized(eventListeners) {
                        eventListeners[event.key]?.forEach {
                            try {
                                it.invoke(event.event)
                            } catch (e: Throwable) {
                                log.error(e) { "Exception in event listener. Key: ${event.key} Event: ${event.event}" }
                            }
                        }
                    }
                }
            }
        }
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                dispose()
            }
        })
    }

    fun doWhenReconnected(action: () -> Unit) {
        this.reconnectedListeners.add(action)
    }

    fun addWatcher(key: EcosZooKeeperWatcherKey, action: (WatchedEvent) -> Unit): EcosZkWatcher {
        return synchronized(eventListeners) {
            val listeners = eventListeners.computeIfAbsent(key) { ArrayList() }
            if (listeners.isEmpty()) {
                registerWatcher(key)
            }
            listeners.add(action)
            EcosZkWatcherImpl {
                synchronized(eventListeners) {
                    listeners.remove(action)
                }
            }
        }
    }

    private fun registerWatcher(key: EcosZooKeeperWatcherKey) {
        getClient(key.namespace).watchers()
            .add()
            .withMode(
                if (key.recursive) {
                    AddWatchMode.PERSISTENT_RECURSIVE
                } else {
                    AddWatchMode.PERSISTENT
                }
            )
            .usingWatcher(
                CuratorWatcher {
                    if (it.type != null && it.type != EventType.None) {
                        eventsQueue.add(WatcherEvent(key, it))
                    }
                }
            )
            .forPath(key.path)
    }

    fun getClient(namespace: String): CuratorFramework {
        if (!initialized.get()) {
            if (!client.blockUntilConnected(2, TimeUnit.SECONDS)) {
                do {
                    log.warn { "Waiting until ZooKeeper will be available" }
                } while (!client.blockUntilConnected(1, TimeUnit.MINUTES))
            }
            initialized.set(true)
        }
        return if (namespace.isEmpty()) {
            client
        } else {
            client.usingNamespace(namespace)
        }
    }

    fun dispose() {
        if (clientDisposed.get()) {
            return
        }
        try {
            this.client.close()
        } catch (e: Exception) {
            log.warn(e) { "Exception while client closing" }
        }
        clientDisposed.set(true)
    }

    private data class WatcherEvent(
        val key: EcosZooKeeperWatcherKey,
        val event: WatchedEvent
    )
}
