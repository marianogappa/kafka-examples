# Kafka Examples

Apache Kafka is often unnecessarily hard to get acquainted with for newcomers. Although the documentation is straightforward, there are no good Scala examples out there, and it's very easy to get confused when the code doesn't do what one expects.

The examples in this repo should "just work" for a number of common scenarios, giving you the easiest jump start possible into implementing Kafka by yourself. They are aimed at Scala developers; if you're not comfortable with Futures and combinators this might not be useful to you.


## Requirements

- SBT (for running tests)
http://www.scala-sbt.org/
- Docker (for starting a Kafka cluster) 
https://docs.docker.com/installation/
- Docker Compose (to easily start 2 containers with specific configs) 
https://docs.docker.com/compose/install/

Note: if you don't like Docker, just start a Kafka Cluster however you like. The tests will work as long as they can talk to a broker and to ZooKeeper.


## Setup

Note: The examples assume a working Kafka cluster with at least one broker; so before running the tests we need to start Zookeeper/Kafka. We will start them in docker containers.

- Clone the `kafka-docker` repo by wurstmeister
```
git clone git@github.com:wurstmeister/kafka-docker.git
```
- run docker compose on the root folder of the `kafka-docker` repo (you may choose a different yml file)
```
docker-compose --file docker-compose-single-broker.yml up
```
- Clone this repo
```
git clone git@github.com:MarianoGappa/kafka-examples.git
```
- Open the `application.conf` file and set the correct hostname:port addresses for Kafka and Zookeper. The ports should be fine if you used the suggested yml file, but the IP address will depend on your system.
```
$ cat src/main/resources/application.conf
metadata-broker-list = "192.168.59.103:9092"
zookeeper-connect = "192.168.59.103:2181"
```
- Run `sbt` in the root folder of this repo
- Ready to start tests!
```
  > testOnly BasicWorkflowTest
  > testOnly BasicAsyncWorkflowTest
  > testOnly LargestOffsetTest
  > testOnly ConsumerGroupTheGhettoWayTest
  > testOnly ConsumerGroupTest
  > testOnly ProducerConsumerAndLoggerTest
```

## Tests

#### BasicWorkflowTest
A base case: one producer, one consumer, one execution thread. If we don't produce the messages first, the consumer will block our thread until the 5 second timeout kicks in. Note that it's not necessary to create the topic, as the producer will trigger its creation upon sending the first message (although you may see some error logs complaining that the metadata can't be found).


#### BasicAsyncWorkflowTest
Same as BasicWorkflowTest, but the Producer and the Consumer now live inside Futures. In this case, the consumer stream will not block execution, so we need to poll-check with `awaitCondition` until all messages are consumed. You may now try reordering the Producer and Consumer code, and it should still work.


#### LargestOffsetTest
This example illustrates the `autoOffsetReset = 'largest'` property. There is still one Producer and one Consumer (the latter living in a Future so that it doesn't block execution), but in this case we produce an initial batch of messages, then start the Consumer, and finally we produce a second batch of messages. The Consumer should thus only consume the second batch of messages.


#### ConsumerGroupTheGhettoWayTest
In this case we have 1 producer and a group of 3 consumers, consuming from a single topic. This is the common "load balancing" scenario. Note that explicit topic creation is necessary in this case, since we need 3 partitions. Also note that we are not using keys when producing messages: Kafka hashes the key to decide which partition to send the message to. We workaround this issue by refreshing the topic metadata 10 times per second; this is a ghetto way of randomising the recipient consumer for our messages. Don't use this solution.


#### ConsumerGroupTest
Almost identical to ConsumerGroupTheGhettoWayTest, but in this case instead of refreshing the topic metadata we actually produce a key using the message number. Use this solution instead of the previous one to jump start a consumer group use case.


#### ProducerConsumerAndLoggerTest
In this case, we have the basic Producer-Consumer pair, but we don't print out anything that goes on with them. Instead, we have a second Consumer we call Logger, that will sniff the other two and print out their actions. Because consumers don't actually produce any events, we manually produce a "Consumed Event" message in a separate topic each time the Consumer reads something. With this pattern, it becomes easy to log an entire application's data flows, as long as they are implemented with Kafka Queues.


## Code notes and caveats

- Note that the KafkaAdminUtils' methods utilise a zkClient instantiated with a ZKStringSerializer. It is not obvious that this component is necessary to make topic creation work, and it has been the source of hours of head scratching.
- Note that the `consumer.shutdown()` invocations are inside `Future`s when there is more than one consumer. This is not pedantic; adding/removing consumers triggers a rebalancing process that closes all sockets, sorts out who gets to consume what and then reassigns and reopens sockets. By doing the shutdowns in parallel we avoid rebalancing timeout exceptions and consumer timeout exceptions.
- Only a few minimal Utils classes have been added to make the code more clear. It's very convenient to instantiate Producers and Consumers explicitly overriding the properties that are relevant to the use case, leaving every other property to the documentation's default values.
- There has been no drive to make the code within these tests beautiful. There are many ways to abstract the details; but in order to grok how Kafka works I believe it's better to have one big method where everything can be understood without the need for comments.
- There are a bunch of unnecessary `Thread.sleep`s on the tests. You can remove them, but chances are the `println`s will end up sequential between components, making you think there is no asynchonicity involved.
- You may see some rebalancing errors from Kafka logs if you run all tests at once (i.e. `sbt test`); this is normal. We will be shutting down some consumers while others still live and finishing tests quickly, and Rebalancing might not work properly; the tests should pass though.


## Contribute

- Please report issues, propose new tests, share your thoughts, etc.
- Star the repo if you found it useful! 
- I accept PRs.
