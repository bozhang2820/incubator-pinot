/**
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
package org.apache.pinot.grigio.keyCoordinator.internal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.pinot.grigio.common.OffsetInfo;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.rpcQueue.KeyCoordinatorQueueConsumer;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorMasterSlaveOffsetManager;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorParticipantMastershipManager;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class MessageConsumingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumingManager.class);

  protected int _fetchMsgDelayMs;
  protected int _fetchMsgMaxCount;

  protected KeyCoordinatorConf _conf;
  protected Configuration _consumerConfig;
  protected BlockingQueue<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> _consumerRecordBlockingQueue;
  protected Map<TopicPartition, MessageFetcher> _messageFetcherMap;
  protected KeyCoordinatorParticipantMastershipManager _mastershipManager;
  protected KeyCoordinatorMasterSlaveOffsetManager _offsetManager;
  protected GrigioKeyCoordinatorMetrics _metrics;

  public MessageConsumingManager(KeyCoordinatorConf conf, Configuration consumerConfig,
                                 KeyCoordinatorParticipantMastershipManager mastershipManager,
                                 KeyCoordinatorMasterSlaveOffsetManager offsetManager,
                                 GrigioKeyCoordinatorMetrics metrics) {
    this(conf, consumerConfig, new ArrayBlockingQueue<>(conf.getConsumerBlockingQueueSize()), mastershipManager, offsetManager, metrics);
  }

  @VisibleForTesting
  protected MessageConsumingManager(KeyCoordinatorConf conf, Configuration consumerConfig,
                                    BlockingQueue<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> consumerRecordBlockingQueue,
                                    KeyCoordinatorParticipantMastershipManager mastershipManager,
                                    KeyCoordinatorMasterSlaveOffsetManager offsetManager,
                                    GrigioKeyCoordinatorMetrics metrics) {
    _conf = conf;
    _consumerConfig = consumerConfig;
    _messageFetcherMap = new HashMap<>();
    _mastershipManager = mastershipManager;
    _offsetManager = offsetManager;

    _metrics = metrics;
    _consumerRecordBlockingQueue = consumerRecordBlockingQueue;

    _fetchMsgDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_DELAY_MS_DEFAULT);
    _fetchMsgMaxCount = conf.getInt(KeyCoordinatorConf.FETCH_MSG_MAX_BATCH_SIZE,
        KeyCoordinatorConf.FETCH_MSG_MAX_BATCH_SIZE_DEFAULT);

    LOGGER.info("Starting with fetch delay: {}, fetch max count: {}", _fetchMsgDelayMs,
        _fetchMsgMaxCount);
  }

  /**
   * Subscribe to a partition of the key coordinator message topic
   */
  public synchronized void subscribe(String topic, Integer partition) {
    LOGGER.info("Trying to subscribe to key coordinator message topic {} partition {}", topic, partition);
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    if (!_messageFetcherMap.containsKey(topicPartition)) {
      KeyCoordinatorQueueConsumer consumer = newConsumer(_consumerConfig);
      MessageFetcher messageFetcher =
          new MessageFetcher(_conf, _consumerRecordBlockingQueue, topic, partition, _mastershipManager, _offsetManager, consumer, _metrics);
      messageFetcher.start();
      _messageFetcherMap.put(topicPartition, messageFetcher);
    } else {
      LOGGER.warn("Failed to subscribe, key coordinator was already subscribed to topic {} partition {}", topic, partition);
    }
  }

  @VisibleForTesting
  protected void subscribe(String topic, Integer partition, KeyCoordinatorQueueConsumer consumer) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    MessageFetcher messageFetcher =
        new MessageFetcher(_conf, _consumerRecordBlockingQueue, topic, partition, _mastershipManager, _offsetManager, consumer, _metrics);
    messageFetcher.start();
    _messageFetcherMap.put(topicPartition, messageFetcher);
  }

  /**
   * Unsubscribe to a partition of the key coordinator message topic
   */
  public synchronized void unsubscribe(String topic, Integer partition) {
    LOGGER.info("Trying to unsubscribe to key coordinator message topic {}, partition {}", topic, partition);
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    if (_messageFetcherMap.containsKey(topicPartition)) {
      MessageFetcher messageFetcher = _messageFetcherMap.remove(topicPartition);
      messageFetcher.stop();
    } else {
      LOGGER.warn("Failed to unsubscribe, key coordinator was not subscribed to topic {} partition {}", topic, partition);
    }
  }

  /**
   * Get a set of topic partitions to which this key coordinator instance is currently subscribed
   */
  public Set<TopicPartition> getSubscribedTopicPartitions() {
    return _messageFetcherMap.keySet();
  }

  private KeyCoordinatorQueueConsumer newConsumer(Configuration consumerConfig) {
    KeyCoordinatorQueueConsumer consumer = new KeyCoordinatorQueueConsumer();
    consumer.init(consumerConfig, _metrics);
    return consumer;
  }

  /**
   * get a list of messages read by the consumer ingestion thread
   * @param deadlineInMs linux epoch time we should stop the ingestion and return it to caller with the data we have so far
   * @return list of messages to be processed
  */
  public MessageAndOffset<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> getMessages(long deadlineInMs) {
    List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> buffer = new ArrayList<>(_fetchMsgMaxCount);
    while(System.currentTimeMillis() < deadlineInMs && buffer.size() < _fetchMsgMaxCount) {
      _consumerRecordBlockingQueue.drainTo(buffer, _fetchMsgMaxCount - buffer.size());
      if (buffer.size() < _fetchMsgMaxCount) {
        Uninterruptibles.sleepUninterruptibly(_fetchMsgDelayMs, TimeUnit.MILLISECONDS);
      }
    }
    OffsetInfo offsetInfo = new OffsetInfo();
    for (QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg> record: buffer) {
      offsetInfo.updateOffsetIfNecessary(record);
    }
    return new MessageAndOffset<>(buffer, offsetInfo);
  }

  /**
   * commit the current ingestion progress to internal offset storage
   * @param offsetInfo
   */
  public void ackOffset(OffsetInfo offsetInfo) {
    if (_messageFetcherMap.isEmpty()) {
      LOGGER.error("Cannot ack offset because there is no message fetcher available");
      return;
    }
    MessageFetcher messageFetcher = _messageFetcherMap.entrySet().iterator().next().getValue();
    messageFetcher.ackOffset(offsetInfo);
  }

  public void stopFetchers() {
    for (MessageFetcher messageFetcher : _messageFetcherMap.values()) {
      messageFetcher.stop();
    }
  }

  /**
   * class wrap around the message and offset associated information
   * @param <K>
   */
  public static class MessageAndOffset<K> {
    private List<K> _messages;
    private OffsetInfo _offsetInfo;

    /**
     * @param messages list of messages for the current batch
     * @param offsetInfo the largest offset for each partition in current set of messages
     */
    public MessageAndOffset(List<K> messages, OffsetInfo offsetInfo) {
      _messages = messages;
      _offsetInfo  = offsetInfo;
    }

    public List<K> getMessages() {
      return _messages;
    }

    public OffsetInfo getOffsetInfo() {
      return _offsetInfo;
    }
  }
}
