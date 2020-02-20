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

import org.apache.helix.HelixManager;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager for leadership status of key coordinator controllers. This listens to leadership changes
 * and updates local cache for the leadership status.
 */
public class KeyCoordinatorControllerLeadershipManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorControllerLeadershipManager.class);

  private final HelixManager _controllerHelixManager;

  private volatile boolean _isControllerLeader = false;

  public KeyCoordinatorControllerLeadershipManager(HelixManager controllerHelixManager) {
    _controllerHelixManager = controllerHelixManager;
    _controllerHelixManager
        .addControllerListener((ControllerChangeListener) notificationContext -> onControllerChange());
  }

  public synchronized boolean isControllerLeader() {
    return _isControllerLeader;
  }

  private synchronized void onControllerChange() {
    boolean newIsControllerLeader = _controllerHelixManager.isLeader();
    LOGGER.info("Key coordinator controller isControllerLeader status changed from {} to {}", _isControllerLeader, newIsControllerLeader);
    _isControllerLeader = newIsControllerLeader;
  }
}
