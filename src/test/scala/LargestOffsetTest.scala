import java.util.UUID

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.scalatest.{FunSpec, ShouldMatchers}
import utils.{AwaitCondition, KafkaAdminUtils, KafkaConsumerUtils, KafkaProducerUtils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class LargestOffsetTest extends FunSpec with ShouldMatchers with AwaitCondition {
  describe("Largest Offset") {
    it("should not consume produced messages before the consumer was created") {

      /*
      This example illustrates the `autoOffsetReset = 'largest'` property. There is still one Producer and
      one Consumer (the latter living in a Future so that it doesn't block execution), but in this case we
      produce an initial batch of messages, then start the Consumer, and finally we produce a second batch
      of messages. The Consumer should thus only consume the second batch of messages.
      */

      val topic = s"topic-${UUID.randomUUID()}"
      KafkaAdminUtils.createTopic(topic)


      val producer = KafkaProducerUtils.create()
      (1 to 5) foreach { number ⇒
        println(s"Producing Message $number")
        Thread.sleep(50) // N.B.: Unnecessary; it's here to show the parallelism in the tests
        producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, s"Message $number".getBytes("UTF-8")))
      }
      println(s"Finished first batch of producing messages")


      var consumedMessages = 0

      val consumer = KafkaConsumerUtils.create(consumerTimeoutMs = 5000, autoOffsetReset = "largest")
      val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new StringDecoder, new StringDecoder).head
      val consumerFuture = Future {
        println("Consuming")
        stream foreach { item ⇒
          println(s"Consumed ${item.message()}")
          consumedMessages += 1
        }
      }.andThen { case _ ⇒ println(s"Shutting down Consumer") }

      Thread.sleep(1000) // N.B.: Necessary; otherwise a message might be produced before the stream is actually opened

      (6 to 10) foreach { number ⇒
        println(s"Producing Message $number")
        Thread.sleep(50) // N.B.: Unnecessary; it's here to show the parallelism in the tests
        producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, s"Message $number".getBytes("UTF-8")))
      }
      println(s"Finished second batch of producing messages")
      producer.close()


      awaitCondition("Didn't consume 5 messages!", 10.seconds) {
        consumedMessages shouldBe 5
      }

      consumer.shutdown()
      KafkaAdminUtils.deleteTopic(topic)
      Await.ready(consumerFuture, 10.second)
    }
  }
}
