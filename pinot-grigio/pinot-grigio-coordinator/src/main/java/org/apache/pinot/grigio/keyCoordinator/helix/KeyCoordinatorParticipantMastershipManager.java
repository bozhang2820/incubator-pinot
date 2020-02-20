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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;


/**
 * Manager for the master/slave status of key coordinator participants (key coordinator message consumers)
 */
public class KeyCoordinatorParticipantMastershipManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorParticipantMastershipManager.class);

  protected ConcurrentHashMap<Integer, Boolean> _isParticipantMaster;

  public boolean isParticipantMaster(int partition) {
    return _isParticipantMaster.get(partition);
  }

  public void setParticipantMaster(int partition, boolean participantMaster) {
    _isParticipantMaster.put(partition, participantMaster);
  }
}