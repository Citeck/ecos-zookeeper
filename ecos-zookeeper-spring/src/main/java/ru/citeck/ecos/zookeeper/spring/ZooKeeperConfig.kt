package ru.citeck.ecos.zookeeper.spring

import ecos.org.apache.curator.framework.CuratorFramework
import ecos.org.apache.curator.framework.CuratorFrameworkFactory
import ecos.org.apache.curator.retry.ExponentialBackoffRetry
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.citeck.ecos.zookeeper.EcosZooKeeper

/**
 * @author Roman Makarskiy
 */
@Configuration
open class ZooKeeperConfig {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    @Value("\${ecos.zookeeper.host:zookeeper-app}")
    private lateinit var host: String

    @Value("\${ecos.zookeeper.port:2181}")
    private var port: Int = 2181

    @Value("\${ecos.zookeeper.namespace:ecos}")
    private lateinit var namespace: String

    @Value("\${ecos.zookeeper.curator.retry-policy.base-sleep:5000}")
    private var baseSleepTime: Int = 5000

    @Value("\${ecos.zookeeper.curator.retry-policy.max-retries:10}")
    private var maxRetries: Int = 10

    @Bean
    open fun curatorFramework(): CuratorFramework {
        val retryPolicy = ExponentialBackoffRetry(baseSleepTime, maxRetries)
        val connectString = "$host:$port"

        log.info("================Ecos Zookeeper Init======================")
        log.info("Connect to Zookeeper with params")
        log.info("URL: $connectString")
        log.info("namespace: $namespace")
        log.info("baseSleepTime: $baseSleepTime")
        log.info("maxRetries: $maxRetries")
        log.info("Startup will be stopped until Zookeeper will be available")
        log.info("=========================================================")

        val client: CuratorFramework = CuratorFrameworkFactory
            .newClient(connectString, retryPolicy)
        client.start()
        return client
    }

    @Bean
    open fun ecosZookeeper(curatorFramework: CuratorFramework): EcosZooKeeper {
        return EcosZooKeeper(curatorFramework).withNamespace(namespace)
    }

}
