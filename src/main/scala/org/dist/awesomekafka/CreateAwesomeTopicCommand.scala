package org.dist.awesomekafka

import java.util.Random

import org.dist.awesomekafka.ReplicationStrategies.ReplicaAssignmentStrategy

case class PartitionReplicas(partitionId: Int, brokerIds: List[Int])


class CreateAwesomeTopicCommand(zookeeperClient: AwesomeZookeeperClient
                                , partitionAssigner: ReplicaAssignmentStrategy = ReplicationStrategies.DefaultReplicaAssignmentStrategy) {
  val rand = new Random

  def createTopic(topicName: String, noOfPartitions: Int, replicationFactor: Int) = {
    val brokerIds = zookeeperClient.getAllBrokerIds()
    val partitionReplicas = partitionAssigner.assignReplicasToBrokers(brokerIds.toList, noOfPartitions, replicationFactor)
    // register topic with partition assignments to zookeeper
    zookeeperClient.setPartitionReplicasForTopic(topicName, partitionReplicas)

  }

}

