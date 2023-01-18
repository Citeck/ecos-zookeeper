package ru.citeck.ecos.zookeeper

object ZkTestUtils {

    fun createZkClient(connectString: String): EcosZooKeeper {
        val hostPort = connectString.split(":")
        return EcosZooKeeper(
            EcosZooKeeperProperties(
                hostPort[0],
                hostPort[1].toInt()
            )
        )
    }
}
