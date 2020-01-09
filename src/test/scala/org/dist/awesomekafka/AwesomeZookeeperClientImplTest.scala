package org.dist.awesomekafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.utils.ZkUtils.Broker

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


}
