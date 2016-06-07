/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.migration

import kafka.integration.KafkaServerTestHarness

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.security.JaasUtils
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.checkpoint.kafka.{KafkaCheckpointManager, KafkaCheckpointLogKey, KafkaCheckpointManagerFactory}
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.coordinator.MockSystemFactory
import org.apache.samza.coordinator.stream.messages.SetMigrationMetaMessage
import org.apache.samza.coordinator.stream._
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.storage.ChangelogPartitionManager
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util._
import org.apache.samza.Partition
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConversions._
import scala.collection._

class TestKafkaCheckpointMigration extends KafkaServerTestHarness {

  val checkpointTopic = "checkpoint-topic"
  val serdeCheckpointTopic = "checkpoint-topic-invalid-serde"
  val checkpointTopicConfig = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(null)

  var producerConfig: KafkaProducerConfig = null
  var metadataStore: TopicMetadataStore = null
  var brokers: String = null

  val partition = new Partition(0)
  val partition2 = new Partition(1)
  val cp1 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "123"))
  val cp2 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "12345"))

  protected def numBrokers: Int = 3

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, true)
    props.map(KafkaConfig.fromProps)
  }

  @Before
  override def setUp {
    super.setUp
    val config = new java.util.HashMap[String, Object]()
    brokers = brokerList.split(",").map(p => "localhost" + p).mkString(",")

    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    config.put("acks", "all")
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    config.put(ProducerConfig.RETRIES_CONFIG, (new Integer(java.lang.Integer.MAX_VALUE-1)).toString)
    config.putAll(KafkaCheckpointManagerFactory.INJECTED_PRODUCER_PROPERTIES)
    producerConfig = new KafkaProducerConfig("kafka", "i001", config)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")
  }

  val systemStreamPartitionGrouperFactoryString = classOf[GroupByPartitionFactory].getCanonicalName


  private def writeChangeLogPartitionMapping(changelogMapping: Map[TaskName, Integer], cpTopic: String = checkpointTopic) = {
    val producer: Producer[Array[Byte], Array[Byte]] = new KafkaProducer(producerConfig.getProducerProperties)
    val record = new ProducerRecord(
      cpTopic,
      0,
      KafkaCheckpointLogKey.getChangelogPartitionMappingKey().toBytes(),
      new CheckpointSerde().changelogPartitionMappingToBytes(changelogMapping)
    )
    try {
      producer.send(record).get()
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      producer.close()
    }
  }

  @Test
  def testMigrationWithNoCheckpointTopic() {
    val mapConfig = Map[String, String](
      "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
      JobConfig.JOB_NAME -> "test",
      JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
      JobConfig.JOB_CONTAINER_COUNT -> "2",
      "task.inputs" -> "test.stream1",
      "task.checkpoint.system" -> "test",
      SystemConfig.SYSTEM_FACTORY.format("test") -> classOf[MockSystemFactory].getCanonicalName,
      "systems.test.producer.bootstrap.servers" -> brokers,
      "systems.test.consumer.zookeeper.connect" -> zkConnect,
      SystemConfig.SYSTEM_FACTORY.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName)

    // Enable consumer caching
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache()

    val config: MapConfig = new MapConfig(mapConfig)
    val migrate = new KafkaCheckpointMigration
    migrate.migrate(config)
    val consumer = new CoordinatorStreamSystemFactory().getCoordinatorStreamSystemConsumer(config, new NoOpMetricsRegistry)
    consumer.register()
    consumer.start()
    consumer.bootstrap()
    val bootstrappedStream = consumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE)
    assertEquals(1, bootstrappedStream.size())

    val expectedMigrationMessage = new SetMigrationMetaMessage("CHECKPOINTMIGRATION", "CheckpointMigration09to10", "true")
    assertEquals(expectedMigrationMessage, bootstrappedStream.head)
    consumer.stop()
  }

  @Test
  def testMigration() {
    try {
      val mapConfig = Map(
        "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
        JobConfig.JOB_NAME -> "test",
        JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
        JobConfig.JOB_CONTAINER_COUNT -> "2",
        "task.inputs" -> "test.stream1",
        "task.checkpoint.system" -> "test",
        "systems.test.producer.bootstrap.servers" -> brokers,
        "systems.test.consumer.zookeeper.connect" -> zkConnect,
        SystemConfig.SYSTEM_FACTORY.format("test") -> classOf[MockSystemFactory].getCanonicalName,
        SystemConfig.SYSTEM_FACTORY.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName)
      // Enable consumer caching
      MockCoordinatorStreamSystemFactory.enableMockConsumerCache()

      val config = new MapConfig(mapConfig)
      val checkpointTopicName = KafkaUtil.getCheckpointTopic("test", "1")
      val checkpointManager = new KafkaCheckpointManagerFactory().getCheckpointManager(config, new NoOpMetricsRegistry).asInstanceOf[KafkaCheckpointManager]

      // Write a couple of checkpoints in the old checkpoint topic
      val task1 = new TaskName(partition.toString)
      val task2 = new TaskName(partition2.toString)
      checkpointManager.start
      checkpointManager.register(task1)
      checkpointManager.register(task2)
      checkpointManager.writeCheckpoint(task1, cp1)
      checkpointManager.writeCheckpoint(task2, cp2)

      // Write changelog partition info to the old checkpoint topic
      val changelogMapping = Map(task1 -> 1.asInstanceOf[Integer], task2 -> 10.asInstanceOf[Integer])
      writeChangeLogPartitionMapping(changelogMapping, checkpointTopicName)
      checkpointManager.stop

      // Initialize coordinator stream
      val coordinatorFactory = new CoordinatorStreamSystemFactory()
      val coordinatorSystemConsumer = coordinatorFactory.getCoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
      val coordinatorSystemProducer = coordinatorFactory.getCoordinatorStreamSystemProducer(config, new MetricsRegistryMap)
      coordinatorSystemConsumer.register()
      coordinatorSystemConsumer.start()

      assertEquals(coordinatorSystemConsumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE).size, 0)
      coordinatorSystemConsumer.stop

      // Start the migration
      val migrationInstance = new KafkaCheckpointMigration
      migrationInstance.migrate(config)

      // Verify if the changelogPartitionInfo has been migrated
      val newChangelogManager = new ChangelogPartitionManager(coordinatorSystemProducer, coordinatorSystemConsumer, "test")
      newChangelogManager.start
      val newChangelogMapping = newChangelogManager.readChangeLogPartitionMapping()
      newChangelogManager.stop
      assertEquals(newChangelogMapping.toMap, changelogMapping)

      // Check for migration message
      coordinatorSystemConsumer.register()
      coordinatorSystemConsumer.start()
      assertEquals(coordinatorSystemConsumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE).size, 1)
      coordinatorSystemConsumer.stop()
    }
    finally {
      MockCoordinatorStreamSystemFactory.disableMockConsumerCache()
    }
  }

  class MockKafkaCheckpointMigration extends KafkaCheckpointMigration{
    var migrationCompletionMarkFlag: Boolean = false
    var migrationVerificationMarkFlag: Boolean = false

    override def migrationCompletionMark(coordinatorStreamProducer: CoordinatorStreamSystemProducer) = {
      migrationCompletionMarkFlag = true
      super.migrationCompletionMark(coordinatorStreamProducer)
    }

    override def migrationVerification(coordinatorStreamConsumer: CoordinatorStreamSystemConsumer): Boolean = {
      migrationVerificationMarkFlag = true
      super.migrationVerification(coordinatorStreamConsumer)
    }
  }
}
