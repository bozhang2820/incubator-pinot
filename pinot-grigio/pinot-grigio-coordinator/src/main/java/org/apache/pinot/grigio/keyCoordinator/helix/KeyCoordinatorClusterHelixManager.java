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

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.grigio.keyCoordinator.api.KeyCoordinatorInstance;
import org.apache.pinot.grigio.keyCoordinator.internal.MessageConsumingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;


/**
 * This manages the key coordinator cluster (key coordinators as controller-participant)
 */
public class KeyCoordinatorClusterHelixManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorClusterHelixManager.class);

  private final String _helixZkURL;
  private final String _keyCoordinatorClusterName;
  private final String _keyCoordinatorMessageResourceName;
  private final String _keyCoordinatorId;
  private final HelixManager _controllerHelixManager;
  private final HelixManager _participantHelixManager;
  private final HelixAdmin _helixAdmin;

  public KeyCoordinatorClusterHelixManager(@Nonnull String zkURL, @Nonnull String keyCoordinatorClusterName,
                                           @Nonnull String keyCoordinatorId, @Nonnull MessageConsumingManager messageConsumingManager,
                                           @Nonnull KeyCoordinatorParticipantMastershipManager mastershipManager,
                                           @Nonnull HelixManager participantHelixManager,
                                           @Nonnull String keyCoordinatorMessageTopic, int keyCoordinatorMessagePartitionCount)
      throws Exception {
    _helixZkURL = zkURL;
    _keyCoordinatorClusterName = keyCoordinatorClusterName;
    _keyCoordinatorMessageResourceName = CommonConstants.Helix.KEY_COORDINATOR_MESSAGE_RESOURCE_NAME;
    _keyCoordinatorId = keyCoordinatorId;

    _controllerHelixManager = HelixSetupUtils.setup(_keyCoordinatorClusterName, _helixZkURL, _keyCoordinatorId);
    _helixAdmin = _controllerHelixManager.getClusterManagmentTool();

    IdealState keyCoordinatorMessageResourceIdealState = _helixAdmin
        .getResourceIdealState(_keyCoordinatorClusterName, _keyCoordinatorMessageResourceName);
    if (keyCoordinatorMessageResourceIdealState == null) {
      // todo: update rebalance strategy
      _helixAdmin.addResource(_keyCoordinatorClusterName, _keyCoordinatorMessageResourceName,
          keyCoordinatorMessagePartitionCount, MasterSlaveSMD.name, IdealState.RebalanceMode.CUSTOMIZED.name());
    }

    try {
      _helixAdmin.addInstance(_keyCoordinatorClusterName, new InstanceConfig(_keyCoordinatorId));
    } catch (final HelixException ex) {
      LOGGER.info("key coordinator instance {} already exist in helix cluster {}", _keyCoordinatorId,
          _keyCoordinatorClusterName);
    }

    _participantHelixManager = participantHelixManager;
    _participantHelixManager.getStateMachineEngine().registerStateModelFactory(MasterSlaveSMD.name,
        new KeyCoordinatorMessageStateModelFactory(messageConsumingManager, mastershipManager, keyCoordinatorMessageTopic));
    _participantHelixManager.connect();
  }

  public HelixManager getControllerHelixManager() {
    return _controllerHelixManager;
  }

  public List<String> getAllInstances() {
    return _helixAdmin.getInstancesInCluster(_keyCoordinatorClusterName);
  }

  public void addInstance(KeyCoordinatorInstance keyCoordinatorInstance) {
    _helixAdmin.addInstance(_keyCoordinatorClusterName, keyCoordinatorInstance.toInstanceConfig());
  }

  public void dropInstance(KeyCoordinatorInstance keyCoordinatorInstance) {
    _helixAdmin.dropInstance(_keyCoordinatorClusterName, keyCoordinatorInstance.toInstanceConfig());
  }

  public void rebalance() {
    _helixAdmin.rebalance(_keyCoordinatorClusterName, _keyCoordinatorMessageResourceName,
        CommonConstants.Helix.KEY_COORDINATOR_MESSAGE_RESOURCE_REPLICA_COUNT);
  }
}
