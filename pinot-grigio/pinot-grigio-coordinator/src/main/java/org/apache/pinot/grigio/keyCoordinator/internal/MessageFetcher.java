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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.common.TopicPartition;
import org.apache.pinot.grigio.common.OffsetInfo;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.metrics.GrigioGauge;
import org.apache.pinot.grigio.common.rpcQueue.KeyCoordinatorQueueConsumer;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorMasterSlaveOffsetManager;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorParticipantMastershipManager;
import org.apache.pinot.grigio.keyCoordinator.helix.State;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * class to fetch messages from a specific partition of the key coordinator message topic and
 * store in a shared queue using a separate thread
 */
public class MessageFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageFetcher.class);
  private static final long TERMINATION_WAIT_MS = 10000;

  protected int _fetchMsgDelayMs;
  protected int _fetchMsgMaxDelayMs;
  protected int _fetchMsgWaitMs;

  protected BlockingQueue<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> _consumerRecordBlockingQueue;
  protected KeyCoordinatorQueueConsumer _inputKafkaConsumer;
  protected GrigioKeyCoordinatorMetrics _metrics;
  protected ExecutorService _consumerThread;
  protected KeyCoordinatorParticipantMastershipManager _mastershipManager;
  protected String _topic;
  protected Integer _partition;
  protected KeyCoordinatorMasterSlaveOffsetManager _offsetManager;

  protected volatile State _state;

  public MessageFetcher(KeyCoordinatorConf conf,
                        BlockingQueue<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> consumerRecordBlockingQueue,
                        String topic, Integer partition,
                        KeyCoordinatorParticipantMastershipManager mastershipManager,
                        KeyCoordinatorMasterSlaveOffsetManager offsetManager,
                        KeyCoordinatorQueueConsumer consumer, GrigioKeyCoordinatorMetrics metrics) {
    this(conf, consumerRecordBlockingQueue, topic, partition, mastershipManager, offsetManager, consumer, Executors.newSingleThreadExecutor(), metrics);
  }

  @VisibleForTesting
  protected MessageFetcher(KeyCoordinatorConf conf,
                           BlockingQueue<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> consumerRecordBlockingQueue,
                           String topic, Integer partition,
                           KeyCoordinatorParticipantMastershipManager mastershipManager,
                           KeyCoordinatorMasterSlaveOffsetManager offsetManager,
                           KeyCoordinatorQueueConsumer consumer, ExecutorService service, GrigioKeyCoordinatorMetrics metrics) {
    _inputKafkaConsumer = consumer;
    _metrics = metrics;
    _consumerThread = service;
    _consumerRecordBlockingQueue = consumerRecordBlockingQueue;
    _topic = topic;
    _partition = partition;
    _mastershipManager = mastershipManager;
    _offsetManager = offsetManager;

    _fetchMsgDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_DELAY_MS_DEFAULT);
    _fetchMsgMaxDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS_DEFAULT);
    _fetchMsgWaitMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_WAIT_MS, KeyCoordinatorConf.FETCH_MSG_WAIT_MS_DEFAULT);

    _state = State.INIT;
    LOGGER.info("starting with fetch delay: {} max delay: {}", _fetchMsgDelayMs, _fetchMsgMaxDelayMs);
  }

  public void start() {
    Preconditions.checkState(_state == State.INIT, "message fetcher is not in correct state");
    _state = State.RUNNING;
    _inputKafkaConsumer.subscribe(_topic, _partition);
    _consumerThread.submit(this::consumerIngestLoop);
  }

  public void stop() {
    _state = State.SHUTTING_DOWN;
    _inputKafkaConsumer.unsubscribe(_topic, _partition);
    _inputKafkaConsumer.close();
    _consumerThread.shutdown();
    try {
      _consumerThread.awaitTermination(TERMINATION_WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      LOGGER.error("failed to wait for key coordinator thread to shutdown", ex);
    }
    _consumerThread.shutdownNow();
  }

  public void ackOffset(OffsetInfo offsetInfo) {
    _inputKafkaConsumer.ackOffset(offsetInfo);
  }

  private void consumerIngestLoop() {
    while (_state == State.RUNNING) {
      try {
        _metrics.setValueOfGlobalGauge(GrigioGauge.MESSAGE_PROCESS_QUEUE_SIZE, _consumerRecordBlockingQueue.size());
        List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> records =
            _inputKafkaConsumer.getRequests(_fetchMsgMaxDelayMs, TimeUnit.MILLISECONDS);
        if (records.size() == 0) {
          LOGGER.info("no message found in kafka consumer, sleep and wait for next batch");
          Uninterruptibles.sleepUninterruptibly(_fetchMsgDelayMs, TimeUnit.MILLISECONDS);
        } else {
          long offsetProcessed = _offsetManager.getOffsetProcessedFromPropertyStore(_partition);
          boolean isMaster = _mastershipManager.isParticipantMaster(_partition);
          boolean shouldRewind = false;
          for (QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg> record : records) {
            try {
              if (isMaster || record.getOffset() <= offsetProcessed) {
                _consumerRecordBlockingQueue.put(record);
              } else {
                shouldRewind = true;
              }
            } catch (InterruptedException e) {
              LOGGER.warn("exception while trying to put message to queue", e);
            }
          }
          if (shouldRewind) {
            _inputKafkaConsumer.seek(new TopicPartition(_topic, _partition), offsetProcessed + 1);
            Uninterruptibles.sleepUninterruptibly(_fetchMsgWaitMs, TimeUnit.MILLISECONDS);
          }
          _metrics.setValueOfGlobalGauge(GrigioGauge.KC_INPUT_MESSAGE_LAG_MS,
              System.currentTimeMillis() - records.get(records.size() - 1).getTimestamp());
        }
      } catch (Exception ex) {
        LOGGER.error("encountered exception in consumer ingest loop, will retry", ex);
      }
    }
    LOGGER.info("exiting consumer ingest loop");
  }
}
