package org.dist.awesomekafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.dist.queue.ZookeeperTestHarness
import org.dist.queue.utils.ZkUtils.Broker
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.scalatest.{FunSuite}


class CreateAwesomeTopicCommandTest extends FunSuite with ZookeeperTestHarness {

  val brokers = (1 to 10).map { n => Broker(n, "blah", 7000 + n) }

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
    val replicationSets = mapper.readValue(partitionReplicasJson, classOf[Set[PartitionReplicas]])
    assert(replicationSets.size == 3)

  }

}
