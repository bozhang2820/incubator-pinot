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
package org.apache.pinot.grigio.common.messages;

import java.io.Serializable;
import java.util.Objects;

/**
 * this message contains the following 4 attributes:
 * 1. segmentName: the name of the segment for the pinot record we are going to update
 * 2. value: the value to update the virtual column to, could be any value we desired to use (the value of new validFrom/validUntil column)
 * 3. updateEventType: insert/delete, used to indicate which column to update
 * 4. kafka offset: the offset of the pinot record we are going to update.
 *
 * segment updater will use the segment name & offset to identify the location of the pinot record, and use the
 * updateEventType to decide which virtual column to update. And it will use value to update the corresponding column.
 */
public class LogCoordinatorMessage implements Serializable {
  private final String _segmentName;
  private final long _value;
  private final LogEventType _updateEventType;
  private long _kafkaOffset;

  public String getSegmentName() {
    return _segmentName;
  }

  public long getValue() {
    return _value;
  }

  public LogEventType getUpdateEventType() {
    return _updateEventType;
  }

  public long getKafkaOffset() {
    return _kafkaOffset;
  }

  public LogCoordinatorMessage(String segmentName, long kafkaOffset,
                               long newValue, LogEventType updateEventType) {
    this._segmentName = segmentName;
    this._value = newValue;
    this._updateEventType = updateEventType;
    this._kafkaOffset = kafkaOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LogCoordinatorMessage that = (LogCoordinatorMessage) o;
    return _value == that._value &&
        _kafkaOffset == that._kafkaOffset &&
        Objects.equals(_segmentName, that._segmentName) &&
        _updateEventType == that._updateEventType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentName, _value, _updateEventType, _kafkaOffset);
  }

  public String toString() {
    return _segmentName + "|"  + _updateEventType + "|" + _value + "|" + _kafkaOffset;
  }
}
