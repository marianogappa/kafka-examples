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

      val MessageCount = 25

      val topic = s"topic-${UUID.randomUUID()}"
      val consumerTopic = s"consumerTopic-${UUID.randomUUID()}"

      List(topic, consumerTopic) foreach (KafkaAdminUtils.createTopic(_))


      val producer = KafkaProducerUtils.create()
      val producerFuture = Future {
        (1 to MessageCount) foreach { number ⇒
          producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, s"Message $number".getBytes("UTF-8")))
          Thread.sleep(50) // N.B.: Unnecessary; it's here to show the parallelism in the tests
        }
      }.andThen { case _ ⇒
        println(s"Finished producing messages")
      }


      val consumer = KafkaConsumerUtils.create(consumerTimeoutMs = 5000, autoOffsetReset = "smallest")
      val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new StringDecoder, new StringDecoder).head
      val consumerFuture = Future {
        stream foreach { item ⇒
          producer.send(new KeyedMessage[Array[Byte], Array[Byte]](consumerTopic, s"Message ${item.message()}".getBytes("UTF-8")))
        }
      }.andThen { case _ ⇒
        println(s"Shutting down Consumer")
        producer.close()
      }


      var loggedMessages = 0
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

          loggedMessages += 1
        }
      }.andThen { case _ ⇒
        println(s"Shutting down Logger")
      }


      awaitCondition(s"Didn't consume $MessageCount messages!", 15.seconds) {
        loggedMessages shouldBe MessageCount * 2
      }

      val shutdownFutures = List(consumer, logger) map (c => Future ( c.shutdown() ) )
      KafkaAdminUtils.deleteTopic(topic)
      (shutdownFutures :+ producerFuture :+ consumerFuture :+ loggerFuture) foreach (Await.ready(_, 10.second))
    }
  }
}
