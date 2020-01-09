package org.dist.awesomekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging
import org.dist.queue.utils.ZkUtils
import scala.jdk.CollectionConverters._

class AwesomeBrokerChangeListener extends IZkChildListener with Logging {
  var liveBrokers  = Set[Int]()
  val BrokerIdsPath =  ZkUtils.BrokerIdsPath

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    info("Awesome Broker change listener called for path %s with children %s".format(parentPath, currentChilds))

    info("Updating current brokers")
    val newLiveBrokers = currentChilds.asScala.map{_.toInt}.toSet

    info(s"Brokers added ${newLiveBrokers -- liveBrokers}")
    info(s"Brokers died ${liveBrokers -- newLiveBrokers}")


    liveBrokers = newLiveBrokers


//    info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.asScala.mkString(",")))
//    try {
//
//      val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
//      val newBrokerIds = curBrokerIds -- controller.liveBrokers.map(broker  => broker.id)
//      val newBrokers = newBrokerIds.map(zookeeperClient.getBrokerInfo(_))
//
//      newBrokers.foreach(controller.addBroker(_))
//
//      if (newBrokerIds.size > 0)
//        controller.onBrokerStartup(newBrokerIds.toSeq)
//
//    } catch {
//      case e: Throwable => error("Error while handling broker changes", e)
//    }
  }
}
