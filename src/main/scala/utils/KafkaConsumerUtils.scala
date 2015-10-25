package utils

import java.util.{UUID, Properties}

import com.typesafe.config._
import kafka.consumer._
import kafka.serializer.StringDecoder

object KafkaConsumerUtils {

  private def properties(groupId: String,
                 maybeConsumerId: Option[String],
                 socketTimeoutMs: Int,
                 socketReceiveBufferBytes: Int,
                 fetchMessageMaxBytes: Int,
                 numConsumerFetchers: Int,
                 autoCommitEnable: Boolean,
                 autoCommitIntervalMs: Int,
                 queuedMaxMessageChunks: Int,
                 rebalanceMaxRetries: Int,
                 fetchMinBytes: Int,
                 fetchWaitMaxMs: Int,
                 rebalanceBackoffMs: Int,
                 refreshLeaderBackoffMs: Int,
                 autoOffsetReset: String,
                 consumerTimeoutMs: Int,
                 excludeInternalTopics: Boolean,
                 partitionAssignmentStrategy: String,
                 clientId: String,
                 zookeeperSessionTimeoutMs: Int,
                 zookeeperConnectionTimeoutMs: Int,
                 zookeeperSyncTimeMs: Int,
                 offsetsStorage: String,
                 offsetsChannelBackoffMs: Int,
                 offsetsChannelSocketTimeoutMs: Int,
                 offsetsCommitMaxRetries: Int,
                 dualCommitEnabled: Boolean
                  ) = {

    val props = new Properties()

    props.put("zookeeper.connect", zookeperConnect)


    maybeConsumerId foreach (consumerId => props.put("consumer.consumerId", consumerId.toString))
    props.put("group.id", groupId)
    props.put("socket.timeout.ms", socketTimeoutMs.toString)
    props.put("socket.receive.buffer.bytes", socketReceiveBufferBytes.toString)
    props.put("fetch.message.max.bytes", fetchMessageMaxBytes.toString)
    props.put("num.consumer.fetchers", numConsumerFetchers.toString)
    props.put("auto.commit.enable", autoCommitEnable.toString)
    props.put("auto.commit.interval.ms", autoCommitIntervalMs.toString)
    props.put("queued.max.message.chunks", queuedMaxMessageChunks.toString)
    props.put("rebalance.max.retries", rebalanceMaxRetries.toString)
    props.put("fetch.min.bytes", fetchMinBytes.toString)
    props.put("fetch.wait.max.ms", fetchWaitMaxMs.toString)
    props.put("rebalance.backoff.ms", rebalanceBackoffMs.toString)
    props.put("refresh.leader.backoff.ms", refreshLeaderBackoffMs.toString)
    props.put("auto.offset.reset", autoOffsetReset)
    props.put("consumer.timeout.ms", consumerTimeoutMs.toString)
    props.put("exclude.internal.topics", excludeInternalTopics.toString)
    props.put("partition.assignment.strategy", partitionAssignmentStrategy)
    props.put("client.id", clientId.toString)
    props.put("zookeeper.session.timeout.ms", zookeeperSessionTimeoutMs.toString)
    props.put("zookeeper.connection.timeout.ms", zookeeperConnectionTimeoutMs.toString)
    props.put("zookeeper.sync.time.ms", zookeeperSyncTimeMs.toString)
    props.put("offsets.storage", offsetsStorage)
    props.put("offsets.channel.backoff.ms", offsetsChannelBackoffMs.toString)
    props.put("offsets.channel.socket.timeout.ms", offsetsChannelSocketTimeoutMs.toString)
    props.put("offsets.commit.max.retries", offsetsCommitMaxRetries.toString)
    props.put("dual.commit.enabled", dualCommitEnabled.toString)

    props
  }

  def create(groupId: String = UUID.randomUUID().toString,
             consumerId: Option[String] = None,
             socketTimeoutMs: Int = 30 * 1000,
             socketReceiveBufferBytes: Int = 64 * 1024,
             fetchMessageMaxBytes: Int = 1024 * 1024,
             numConsumerFetchers: Int = 1,
             autoCommitEnable: Boolean = true,
             autoCommitIntervalMs: Int = 60 * 1000,
             queuedMaxMessageChunks: Int = 2,
             rebalanceMaxRetries: Int = 4,
             fetchMinBytes: Int = 1,
             fetchWaitMaxMs: Int = 100,
             rebalanceBackoffMs: Int = 2000,
             refreshLeaderBackoffMs: Int = 200,
             autoOffsetReset: String = "largest",
             consumerTimeoutMs: Int = -1,
             excludeInternalTopics: Boolean = true,
             partitionAssignmentStrategy: String = "range",
             clientId: String = "groupidvalue",
             zookeeperSessionTimeoutMs: Int = 6000,
             zookeeperConnectionTimeoutMs: Int = 6000,
             zookeeperSyncTimeMs: Int = 2000,
             offsetsStorage: String = "zookeeper",
             offsetsChannelBackoffMs: Int = 1000,
             offsetsChannelSocketTimeoutMs: Int = 10000,
             offsetsCommitMaxRetries: Int = 5,
             dualCommitEnabled: Boolean = true) = {

    Consumer.create(new ConsumerConfig(properties(
      groupId,
      consumerId,
      socketTimeoutMs,
      socketReceiveBufferBytes,
      fetchMessageMaxBytes,
      numConsumerFetchers,
      autoCommitEnable,
      autoCommitIntervalMs,
      queuedMaxMessageChunks,
      rebalanceMaxRetries,
      fetchMinBytes,
      fetchWaitMaxMs,
      rebalanceBackoffMs,
      refreshLeaderBackoffMs,
      autoOffsetReset,
      consumerTimeoutMs,
      excludeInternalTopics,
      partitionAssignmentStrategy,
      clientId,
      zookeeperSessionTimeoutMs,
      zookeeperConnectionTimeoutMs,
      zookeeperSyncTimeMs,
      offsetsStorage,
      offsetsChannelBackoffMs,
      offsetsChannelSocketTimeoutMs,
      offsetsCommitMaxRetries,
      dualCommitEnabled
    )))
  }
}
