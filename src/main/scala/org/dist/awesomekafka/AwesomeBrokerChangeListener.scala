package org.dist.awesomekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging
import org.dist.queue.utils.ZkUtils.Broker

import scala.jdk.CollectionConverters._

class AwesomeBrokerChangeListener(controller:AwesomeKafkaController, zookeperClient: AwesomeZookeeperClientImpl)  extends IZkChildListener with Logging {

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    info("Awesome Broker change listener called for path %s with children %s".format(parentPath, currentChilds))

    info("Updating current brokers")
    val newLiveBrokers = currentChilds.asScala.map{_.toInt}.toSet
    val currentBrokers = controller.liveBrokers

    val newBrokerIds = newLiveBrokers -- controller.liveBrokers.map(_.id)
    val diedBrokerIds = controller.liveBrokers.map(_.id) -- newLiveBrokers
    info(s"Brokers added ${newBrokerIds }")
    info(s"Brokers died ${diedBrokerIds}")

    updateBrokers(controller,newBrokerIds,diedBrokerIds)
  }

  private def updateBrokers(controller: AwesomeKafkaController, newBrokerIds: Set[Int], diedBrokerIds: Set[Int]): Unit = {
    controller.liveBrokers =
      controller.liveBrokers ++
        newBrokerIds.map(zookeperClient.getBrokerInfo) --
       diedBrokerIds.map(zookeperClient.getBrokerInfo)
  }
}
