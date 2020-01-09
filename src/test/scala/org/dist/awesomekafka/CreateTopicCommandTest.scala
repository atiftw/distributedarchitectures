package org.dist.awesomekafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.ZookeeperTestHarness
import org.dist.queue.utils.ZkUtils.Broker
import org.scalatest.FunSuite


class CreateAwesomeTopicCommandTest extends FunSuite with ZookeeperTestHarness{
  val brokers = (1 to 10).map{n=>Broker(n,"blah",7000+n)}

  test("should assign set of replicas for partitions of topic") {
    val brokerIds = List(0, 1, 2)
    val zookeeperClient = new AwesomeZookeeperClientImpl(zkClient)
    brokers.foreach(zookeeperClient.registerBroker)
    val createCommandTest = new CreateAwesomeTopicCommand(zookeeperClient)
    val noOfPartitions = 3
    val replicationFactor = 2
    createCommandTest.createTopic("topic1", noOfPartitions, replicationFactor)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val partitionReplicasJson: String = zkClient.readData("/topics/topic1")
    val replicationSets = mapper.readValue(partitionReplicasJson,classOf[Set[PartitionReplicas]])
    assert(replicationSets.size == 3)
  }
//
//  test("should assign partitions assigned to ") {
//    val brokerIds = List(0, 1, 2)
//    val zookeeperClient = new TestZookeeperClient(brokerIds)
//    val createCommandTest = new CreateAwesomeTopicCommand(zookeeperClient)
//    val noOfPartitions = 3
//    val replicationFactor = 2
//    createCommandTest.createTopic("topic1", noOfPartitions, replicationFactor)
//    assert(zookeeperClient.topicName == "topic1")
//    assert(zookeeperClient.partitionReplicas.size == noOfPartitions)
//    zookeeperClient.partitionReplicas.map(p => p.brokerIds).foreach(_.size == replicationFactor)
//  }
}
