package ru.citeck.ecos.zookeeper.client

data class EcosZooKeeperWatcherKey(
    val namespace: String,
    val path: String,
    val recursive: Boolean
)
