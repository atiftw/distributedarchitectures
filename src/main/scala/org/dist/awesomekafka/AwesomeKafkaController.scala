package org.dist.awesomekafka

import org.dist.queue.utils.ZkUtils.Broker

case class ControllerExistsException(controllerId:String) extends RuntimeException

class AwesomeKafkaController(zookeeperClient: AwesomeZookeeperClientImpl, brokerId: Int){
  def setCurrent(existingControllerId: Int): Unit = {
    this.currentLeader = existingControllerId
  }


  var currentLeader = -1
  var liveBrokers  = Set[Broker]()

  def startup(): Unit = {
    zookeeperClient.subscribeControllerChangeListener(this)
    elect()
  }

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

  def onBecomingLeader(){
    zookeeperClient.subscribeBrokerChangeListener(new AwesomeBrokerChangeListener(this,zookeeperClient))
  }
}
