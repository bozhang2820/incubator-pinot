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
package org.apache.pinot.grigio.keyCoordinator.helix;

import com.google.common.base.Preconditions;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.pinot.grigio.common.rpcQueue.KafkaQueueConsumer;
import org.apache.pinot.grigio.keyCoordinator.internal.MessageConsumingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * State model for key coordinator to handle:
 * 1. start of the key coordinator cluster (initial assignment of key coordinator message segments)
 * 2. fail over of a key coordinator instance
 */

public class KeyCoordinatorMessageStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorMessageStateModelFactory.class);

  private final MessageConsumingManager _messageConsumingManager;
  private final KeyCoordinatorParticipantMastershipManager _mastershipManager;
  private final String _keyCoordinatorMessageTopic;

  private static final String HELIX_PARTITION_SEPARATOR = "_";

  public KeyCoordinatorMessageStateModelFactory(MessageConsumingManager messageConsumingManager,
                                                KeyCoordinatorParticipantMastershipManager mastershipManager,
                                                String keyCoordinatorMessageTopic) {
    _messageConsumingManager = messageConsumingManager;
    _mastershipManager = mastershipManager;
    _keyCoordinatorMessageTopic = keyCoordinatorMessageTopic;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LOGGER.info("Creating new state model with resource {} and partition {}", resourceName, partitionName);
    return new KeyCoordinatorMessageStateModel(partitionName);
  }

  @StateModelInfo(states = "{'MASTER', 'SLAVE', 'OFFLINE', 'DROPPED'}", initialState = "OFFLINE")
  public class KeyCoordinatorMessageStateModel extends StateModel {

    private final String _partitionName;

    public KeyCoordinatorMessageStateModel(String partitionName) {
      LOGGER.info("Creating a Key coordinator message state model with partition: {}", partitionName);
      _partitionName = partitionName;
    }

    @Transition(from = "SLAVE", to = "MASTER")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      LOGGER.info("Key coordinator message onBecomeMasterFromSlave with partition: {}", _partitionName);
      _mastershipManager
          .setParticipantMaster(getKafkaPartitionNumberFromHelixPartition(_partitionName), true);
    }

    @Transition(from = "MASTER", to = "SLAVE")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      LOGGER.info("Key coordinator message onBecomeSlaveFromMaster with partition: {}", _partitionName);
      _mastershipManager
          .setParticipantMaster(getKafkaPartitionNumberFromHelixPartition(_partitionName), false);
    }

    @Transition(from = "OFFLINE", to = "SLAVE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      LOGGER.info("Key coordinator message onBecomeSlaveFromOffline with partition: {}", _partitionName);
      _messageConsumingManager
          .subscribe(_keyCoordinatorMessageTopic, getKafkaPartitionNumberFromHelixPartition(_partitionName));
    }

    @Transition(from = "SLAVE", to = "OFFLINE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      LOGGER.info("Key coordinator message onBecomeOfflineFromSlave with partition: {}", _partitionName);
      _messageConsumingManager
          .unsubscribe(_keyCoordinatorMessageTopic, getKafkaPartitionNumberFromHelixPartition(_partitionName));
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("Key coordinator message onBecomeDroppedFromOffline with partition: {}", _partitionName);
    }
  }

  /** helix partitions name as something like keyCoordinatorMessageResource_3
   * parse this string to get the correct numeric value for partition
   * @return the numeric value of this partition
   */
  private int getKafkaPartitionNumberFromHelixPartition(String helixPartition) {
    String[] partitionNameComponents = helixPartition.split(HELIX_PARTITION_SEPARATOR);
    Preconditions.checkState(partitionNameComponents.length > 1,
        "partition name should have more than 1 parts: " + helixPartition);
    try {
      return Integer.parseInt(partitionNameComponents[partitionNameComponents.length - 1]);
    } catch (NumberFormatException ex) {
      LOGGER.error("failed to parse numeric partition value from helix message {}", helixPartition);
      throw new RuntimeException(ex);
    }
  }
}
