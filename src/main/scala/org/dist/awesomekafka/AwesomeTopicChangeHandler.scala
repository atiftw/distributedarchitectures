package org.dist.awesomekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener

import scala.jdk.CollectionConverters._

case class AwesomeTopicChangeHandler(zookeeperClient: AwesomeZookeeperClientImpl, onTopicChange: (String, Seq[PartitionReplicas]) => Unit) extends IZkChildListener {
  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    currentChilds.asScala.foreach(topicName => {
      val replicas: List[PartitionReplicas] = zookeeperClient.getPartitionAssignmentsFor(topicName)
      onTopicChange(topicName, replicas)
    })
  }

}
