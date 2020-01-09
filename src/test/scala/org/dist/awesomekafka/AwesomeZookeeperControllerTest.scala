package org.dist.awesomekafka

import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}

class AwesomeZookeeperControllerTest extends ZookeeperTestHarness {

  private val brokers: IndexedSeq[Broker] = (1 to 10).map { n => Broker(n, s"10.10.10.${10 + n}", 8000 + n) }

  test("Should notifiy when new leader elected") {}

}
