package org.dist.awesomekafka

import org.dist.queue.utils.ZkUtils.Broker

class AwesomeKafkaController(zkClient: AwesomeZookeeperClientImpl){

  var liveBrokers  = Set[Broker]()

  def onBecomingLeader(){
    zkClient.subscribeBrokerChangeListener(new AwesomeBrokerChangeListener(this,zkClient))
  }
}
