package utils

import com.typesafe.config._
import java.util.Properties

import kafka.message._
import kafka.producer._

object KafkaProducerUtils {

  lazy val config = ConfigFactory.load()

  lazy val brokerHost = config.getString("kafka-broker.host")
  lazy val brokerPort = config.getString("kafka-broker.port")

  def properties(requestRequiredAcks: Int,
                         requestTimeoutMs: Int,
                         producerType: String,
                         serializerClass: String,
                         keySerializerClass: String,
                         partitionerClass: String,
                         compressionCodec: String,
                         compressedTopics: String,
                         messageSendMaxRetries: Int,
                         retryBackoffMs: Int,
                         topicMetadataRefreshIntervalMs: Int,
                         queueBufferingMaxMs: Int,
                         queueBufferingMaxMessages: Int,
                         queueEnqueueTimeoutMs: Int,
                         batchNumMessages: Int,
                         sendBufferBytes: Int,
                         clientId: String
                          ) = {

    val props = new Properties()

    props.put("metadata.broker.list", s"$brokerHost:$brokerPort")

    props.put("request.required.acks", requestRequiredAcks.toString)
    props.put("request.timeout.ms", requestTimeoutMs.toString)
    props.put("producer.type", producerType)
    props.put("serializer.class", serializerClass)
    props.put("key.serializer.class", keySerializerClass)
    props.put("partitioner.class", partitionerClass)
    props.put("compression.codec", compressionCodec)
    props.put("compressed.topics", compressedTopics)
    props.put("message.send.max.retries", messageSendMaxRetries.toString)
    props.put("retry.backoff.ms", retryBackoffMs.toString)
    props.put("topic.metadata.refresh.interval.ms", topicMetadataRefreshIntervalMs.toString)
    props.put("queue.buffering.max.ms", queueBufferingMaxMs.toString)
    props.put("queue.buffering.max.messages", queueBufferingMaxMessages.toString)
    props.put("queue.enqueue.timeout.ms", queueEnqueueTimeoutMs.toString)
    props.put("batch.num.messages", batchNumMessages.toString)
    props.put("send.buffer.bytes", sendBufferBytes.toString)
    props.put("client.id", clientId)

    props
  }

  def create(requestRequiredAcks: Int = 0,
                     requestTimeoutMs: Int = 10000,
                     producerType: String = "sync",
                     serializerClass: String = "kafka.serializer.DefaultEncoder",
                     keySerializerClass: String = "kafka.serializer.DefaultEncoder",
                     partitionerClass: String = "kafka.producer.DefaultPartitioner",
                     compressionCodec: String = "none",
                     compressedTopics: String = "",
                     messageSendMaxRetries: Int = 3,
                     retryBackoffMs: Int = 100,
                     topicMetadataRefreshIntervalMs: Int = 600 * 1000,
                     queueBufferingMaxMs: Int = 5000,
                     queueBufferingMaxMessages: Int = 10000,
                     queueEnqueueTimeoutMs: Int = -1,
                     batchNumMessages: Int = 200,
                     sendBufferBytes: Int = 100 * 1024,
                     clientId: String = ""
                      ) = {

    new Producer[Array[Byte], Array[Byte]](new ProducerConfig(properties(
      requestRequiredAcks,
      requestTimeoutMs,
      producerType,
      serializerClass,
      keySerializerClass,
      partitionerClass,
      compressionCodec,
      compressedTopics,
      messageSendMaxRetries,
      retryBackoffMs,
      topicMetadataRefreshIntervalMs,
      queueBufferingMaxMs,
      queueBufferingMaxMessages,
      queueEnqueueTimeoutMs,
      batchNumMessages,
      sendBufferBytes,
      clientId
    )))
  }
}
