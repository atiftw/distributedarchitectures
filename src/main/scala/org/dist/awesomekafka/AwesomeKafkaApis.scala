package org.dist.awesomekafka

import org.I0Itec.zkclient.ZkClient
import org.dist.queue.api._
import org.dist.queue.common._
import org.dist.queue.controller.{Controller, PartitionStateInfo}
import org.dist.queue.message.MessageSet
import org.dist.queue.utils.ZkUtils.Broker

import scala.collection.{Map, mutable}


class AwesomeKafkaApis(
                        val zkClient: ZkClient,
                        brokerId: Int,
                        val controller: Controller) extends Logging {
  def close(): Unit = {}

  /* following 3 data structures are updated by the update metadata request
   * and is queried by the topic metadata request. */
  var leaderCache: mutable.Map[TopicAndPartition, PartitionStateInfo] =
    new mutable.HashMap[TopicAndPartition, PartitionStateInfo]()
  private val aliveBrokers: mutable.Map[Int, Broker] = new mutable.HashMap[Int, Broker]()
  private val partitionMetadataLock = new Object

  def handleFetchRequest(fetchRequest: FetchRequest): FetchResponse = ???


  /**
   * Read from a single topic/partition at the given offset upto maxSize bytes
   */
  private def readMessageSet(topic: String,
                             partition: Int,
                             offset: Long,
                             maxSize: Int,
                             fromReplicaId: Int): (MessageSet, Long) = ???

  /**
   * Read from all the offset details given and return a map of
   * (topic, partition) -> PartitionData
   */
  private def readMessageSets(fetchRequest: FetchRequest): Map[TopicAndPartition, FetchResponsePartitionData] = ???

  def handle(req: RequestOrResponse): RequestOrResponse = {
    val request: RequestOrResponse = req
    request.requestId match {
      case RequestKeys.UpdateMetadataKey => {
        debug(s"Handling FetchRequest ${request.messageBodyJson}")
      }

      case RequestKeys.LeaderAndIsrKey => {
        debug(s"Handling FetchRequest ${request.messageBodyJson}")
      }

      case RequestKeys.GetMetadataKey => {
        debug(s"Handling FetchRequest ${request.messageBodyJson}")
      }
      case RequestKeys.ProduceKey => {
        debug(s"Handling FetchRequest ${request.messageBodyJson}")
      }
      case RequestKeys.FetchKey => {
        debug(s"Handling FetchRequest ${request.messageBodyJson}")
      }
    }
    null

  }
}
