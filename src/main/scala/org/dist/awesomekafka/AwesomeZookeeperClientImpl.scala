package org.dist.awesomekafka

import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, ZkClient}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.awesomekafka.{ControllerExistsException,PartitionReplicas}

import scala.jdk.CollectionConverters._


trait AwesomeZookeeperClient {
  val BrokerIdsPath = "/brokers/ids"
  val ControllerPath = "/controller"
  val TopicsPath = "/topics"

  def getAllBrokerIds(): Set[Int]

  def getBrokerPath(id: Int) = {
    s"$BrokerIdsPath/$id"
  }

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]]

  def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas])

  def registerSelf(): Unit

  def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas]

  def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]]

  def tryCreatingControllerPath(data: String): Unit
}

private[awesomekafka] class AwesomeZookeeperClientImpl(zkClient: ZkClient) extends AwesomeZookeeperClient {
  def subscribeControllerChangeListener(controller: AwesomeKafkaController) = {
    zkClient.subscribeDataChanges(ControllerPath, new AwesomeControllerChangeListener(controller))
  }

  def tryCreatingControllerPath(leaderId: String) = {
    try {
      createEphemeralPath(zkClient, ControllerPath, leaderId)
    } catch {
      case e: ZkNodeExistsException => {
        val existingControllerId: String = zkClient.readData(ControllerPath)
        throw new ControllerExistsException(existingControllerId)
      }
    }
  }


  def registerBroker(broker: Broker): Unit = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }


  class AwesomeControllerChangeListener(controller: AwesomeKafkaController) extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: Any): Unit = {
      val existingControllerId: String = zkClient.readData(dataPath)
      controller.setCurrent(existingControllerId.toInt)
    }

    override def handleDataDeleted(dataPath: String): Unit = {
      controller.elect()
    }
  }

  override def getAllBrokerIds(): Set[Int] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(_.toInt).toSet
  }

  def getAllBrokers(): Set[Broker] = {
    getAllBrokerIds().map(getBrokerInfo)
  }

  def getBrokerInfo(brokerId: Int): Broker = {
    val data: String = zkClient.readData(getBrokerPath(brokerId))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }


  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  override def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  def getTopicPath(topicName: String) = s"$TopicsPath/$topicName"

  override def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]) = {
    val topicsPath = getTopicPath(topicName)
    val topicsData = JsonSerDes.serialize(partitionReplicas)
    createPersistentPath(zkClient, topicsPath, topicsData)
  }

  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }

  override def registerSelf(): Unit = ???

  override def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas] = ???

  override def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]] = ???
}
