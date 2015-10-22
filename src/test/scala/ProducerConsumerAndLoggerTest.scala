import java.util.UUID

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.scalatest.{FunSpec, ShouldMatchers}
import utils.{AwaitCondition, KafkaAdminUtils, KafkaConsumerUtils, KafkaProducerUtils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ProducerConsumerAndLoggerTest extends FunSpec with ShouldMatchers with AwaitCondition {
  describe("A consumer group") {
    it("should produce and consume messages, and a logger should sniff and print it out") {

      val topic = s"topic-${UUID.randomUUID()}"
      val consumerTopic = s"consumerTopic-${UUID.randomUUID()}"

      List(topic, consumerTopic) foreach (KafkaAdminUtils.createTopic(_))

      val producer = KafkaProducerUtils.create()
      val consumer = KafkaConsumerUtils.create(consumerTimeoutMs = 5000, autoOffsetReset = "smallest")

      val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new StringDecoder, new StringDecoder).head

      val producerFuture = Future {
        (1 to 10) foreach { number ⇒
          Thread.sleep(50)
          producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, s"Message $number".getBytes("UTF-8")))
        }
      }.andThen { case _ ⇒ println(s"Finished producing messages") }

      var consumedMessages = 0

      val consumerFuture = Future {
        stream foreach { item ⇒
          consumedMessages += 1
          Thread.sleep(10)
          producer.send(new KeyedMessage[Array[Byte], Array[Byte]](consumerTopic, s"Message ${item.message()}".getBytes("UTF-8")))
        }
      }.andThen { case _ ⇒ println(s"Shutting down Consumer"); consumer.shutdown() }

      val logger = KafkaConsumerUtils.create(consumerTimeoutMs = 5000, autoOffsetReset = "smallest")
      val loggerStream = logger.createMessageStreamsByFilter(
        new Whitelist(s"$topic,$consumerTopic"), 1, new StringDecoder, new StringDecoder
      ).head

      val loggerFuture = Future {
        loggerStream foreach { item ⇒
          item.topic match {
            case s if s == topic =>
              println(s"[LOGGER] Producer produced ${item.message()}")
            case s if s == consumerTopic =>
              println(s"[LOGGER] Consumer consumed ${item.message()}")
          }
        }
      }.andThen { case _ ⇒ println(s"Shutting down Logger"); consumer.shutdown() }


      awaitCondition("Didn't consume 10 messages!", 10.seconds) {
        consumedMessages shouldBe 10
      }

      producer.close()
      List(producerFuture, consumerFuture, loggerFuture) foreach (Await.ready(_, 10.second))
      KafkaAdminUtils.deleteTopic(topic)
    }
  }
}
