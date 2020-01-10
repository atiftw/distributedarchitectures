package org.dist.awesomekafka

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.LeaderAndReplicas

object TopicChangeMetadata {

  case class UpdateMetadataRequest(aliveBrokers: List[Broker], leaderReplicas: List[LeaderAndReplicas])

  case class PartitionInfo(leader: Broker, allReplicas: List[Broker])

  case class LeaderAndReplicas(topicPartition: TopicAndPartition, partitionStateInfo: PartitionInfo)

  case class LeaderAndReplicaRequest(leaderReplicas: List[LeaderAndReplicas])

}
