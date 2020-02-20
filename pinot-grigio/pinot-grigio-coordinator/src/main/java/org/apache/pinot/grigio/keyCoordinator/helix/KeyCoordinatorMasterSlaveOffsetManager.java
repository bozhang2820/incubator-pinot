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

import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager for the synchronization between participant masters and slaves.
 *
 * This is to make sure that the slaves would not be ahead of their masters, and process messages that their masters have not processed.
 */
public class KeyCoordinatorMasterSlaveOffsetManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorMasterSlaveOffsetManager.class);

  private static final String OFFSET_PROCESSED_ZN_PATH_PREFIX = "/OFFSET_PROCESSED_";
  private static final String OFFSET_KEY = "OFFSET";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public KeyCoordinatorMasterSlaveOffsetManager(HelixManager helixManager) {
    _propertyStore = helixManager.getHelixPropertyStore();
  }

  public boolean setOffsetProcessedToPropertyStore(int partition, long offset) {
    String znodePath = getZnodePath(partition);
    ZNRecord record = new ZNRecord(znodePath);
    record.setLongField(OFFSET_KEY, offset);
    return _propertyStore.set(znodePath, record, AccessOption.PERSISTENT);
  }

  public long getOffsetProcessedFromPropertyStore(int partition) {
    String znodePath = getZnodePath(partition);
    ZNRecord record = _propertyStore.get(znodePath, null, AccessOption.PERSISTENT);
    if (record == null) {
      return 0L;
    }
    return Long.parseLong(record.getSimpleField(OFFSET_KEY));
  }

  private String getZnodePath(int partition) {
    return OFFSET_PROCESSED_ZN_PATH_PREFIX + partition;
  }
}