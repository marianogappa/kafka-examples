import java.util.UUID

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import org.scalatest.{ ShouldMatchers, FunSpec }
import utils.{AwaitCondition, KafkaAdminUtils, KafkaProducerUtils, KafkaConsumerUtils}
import kafka.serializer.StringDecoder

class BasicWorkflowTest extends FunSpec with ShouldMatchers with AwaitCondition {
  describe("Basic Workflow") {

    /*
    A base case: one producer, one consumer, one execution thread. If we don't produce the messages first,
    the consumer will block our thread until the 5 second timeout kicks in. Note that it's not necessary
    to create the topic, as the producer will trigger its creation upon sending the first message (although
    you may see some error logs complaining that the metadata can't be found).
     */

    it("should produce and consume messages synchronously") {

      val topic = s"topic-${UUID.randomUUID()}"
      KafkaAdminUtils.createTopic(topic)


      val producer = KafkaProducerUtils.create()
      (1 to 10) foreach { number ⇒
        println(s"Producing Message $number")
        producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, s"Message $number".getBytes("UTF-8")))
      }
      producer.close()


      var consumedMessages = 0
      val consumer = KafkaConsumerUtils.create(consumerTimeoutMs = 5000, autoOffsetReset = "smallest")
      val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new StringDecoder, new StringDecoder).head
      println("Consuming")
      stream foreach { item ⇒
        println(s"Consumed ${item.message()}")
        consumedMessages += 1

        if (consumedMessages >= 10)
          consumer.shutdown()
      }


      KafkaAdminUtils.deleteTopic(topic)

      consumedMessages shouldBe 10
    }
  }
}
