import com.typesafe.config.ConfigFactory

package object utils {
  lazy val config = ConfigFactory.load()

  lazy val zookeperConnect = config.getString("zookeeper-connect")
  lazy val metadataBrokerList = config.getString("metadata-broker-list")

}
