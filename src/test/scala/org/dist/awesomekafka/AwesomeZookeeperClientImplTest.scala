package org.dist.awesomekafka

import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.utils.ZkUtils.Broker
import org.mockito.Mockito

class AwesomeZookeeperClientImplTest extends ZookeeperTestHarness {

  private val brokers: IndexedSeq[Broker] = (1 to 10).map { n => Broker(n, s"10.10.10.${10 + n}", 8000 + n) }

  test("should add new broker information to controller on change") {
    val zookeeperClient = new AwesomeZookeeperClientImpl(zkClient)
    brokers.foreach(zookeeperClient.registerBroker)

    TestUtils.waitUntilTrue(() => zookeeperClient.getAllBrokers().size == 10,"Timeout!",1000)
    zookeeperClient.getAllBrokers().foreach { broker =>
      assertResult(s"10.10.10.${10 + broker.id}")(broker.host)
      assertResult(8000 + broker.id)(broker.port)
    }

  }

  test("should be notified when new brokers are added") {
    val zookeeperClient = new AwesomeZookeeperClientImpl(zkClient)
    val controller = new AwesomeKafkaController(zookeeperClient,1)
    val brokerChangeListener = new AwesomeBrokerChangeListener(controller,zookeeperClient)

    controller.onBecomingLeader()
    zookeeperClient.subscribeBrokerChangeListener(brokerChangeListener)

    brokers.foreach(zookeeperClient.registerBroker)
    TestUtils.waitUntilTrue(() => controller.liveBrokers.size == 10,"TOut!",20000)
  }
}
