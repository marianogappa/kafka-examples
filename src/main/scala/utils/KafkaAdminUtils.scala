package utils

import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

object KafkaAdminUtils {

  val config = ConfigFactory.load()

  val zkHost = config.getString("zookeeper.host")
  val zkPort = config.getString("zookeeper.port")

  val sessionTimeoutMs = 10000
  val connectionTimeoutMs = 10000

  val zkClient = new ZkClient(s"$zkHost:$zkPort", sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer)

  def createTopic(topic: String, numPartitions: Int = 1, replicationFactor: Int = 1, topicConfig: Properties = new Properties) = {
    AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, topicConfig)
  }

  def deleteTopic(topic: String) = AdminUtils.deleteTopic(zkClient, topic)
  def topicExists(topic: String) = AdminUtils.topicExists(zkClient, topic)

}
