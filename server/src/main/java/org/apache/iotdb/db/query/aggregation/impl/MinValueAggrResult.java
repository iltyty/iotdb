/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.aggregation.impl;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.MinMaxInfo;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class MinValueAggrResult extends AggregateResult {

  public MinValueAggrResult(TSDataType dataType) {
    super(dataType, AggregationType.MIN_VALUE);
    reset();
  }

  @Override
  public Object getResult() {
    return hasCandidateResult() ? getValue() : null;
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    MinMaxInfo minInfo = statistics.getMinInfo();
    updateResult(minInfo);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage) {
    updateResultFromPageData(dataInThisPage, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound) {
    Comparable<Object> minVal = null;
    Set<Long> bottomTimestamps = new HashSet<>();
    while (dataInThisPage.hasCurrent()
        && dataInThisPage.currentTime() < maxBound
        && dataInThisPage.currentTime() >= minBound) {
      if (minVal == null || minVal.compareTo(dataInThisPage.currentValue()) > 0) {
        minVal = (Comparable<Object>) dataInThisPage.currentValue();
        bottomTimestamps.clear();
        bottomTimestamps.add(dataInThisPage.currentTime());
      } else if (minVal.compareTo(dataInThisPage.currentValue()) == 0) {
        bottomTimestamps.add(dataInThisPage.currentTime());
      }
      dataInThisPage.next();
    }
    updateResult(new MinMaxInfo<>(minVal, bottomTimestamps));
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Comparable<Object> minVal = null;
    Set<Long> bottomTimes = new HashSet<>();
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null && (minVal == null || minVal.compareTo(values[i]) > 0)) {
        minVal = (Comparable<Object>) values[i];
        bottomTimes.clear();
        bottomTimes.add(timestamps[i]);
      } else if (values[i] != null && minVal.compareTo(values[i]) == 0) {
        bottomTimes.add(timestamps[i]);
      }
    }
    updateResult(new MinMaxInfo<>(minVal, bottomTimes));
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    Comparable<Object> minVal = null;
    Set<Long> bottomTimes = new HashSet<>();
    for (int i = 0; i < length; i++) {
      if (values[i] != null && (minVal == null || minVal.compareTo(values[i]) > 0)) {
        minVal = (Comparable<Object>) values[i];
        bottomTimes.clear();
        bottomTimes.add(timestamps[i]);
      } else if (values[i] != null && minVal.compareTo(values[i]) == 0) {
        bottomTimes.add(timestamps[i]);
      }
    }
    updateResult(new MinMaxInfo<>(minVal, bottomTimes));
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  /** @author Yuyuan Kang */
  @Override
  public void merge(AggregateResult another) {
    if (another.getResult() != null) {
      Object value = another.getResult();
      this.updateResult((MinMaxInfo) value);
    }
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {}

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) {}

  /** @author Yuyuan Kang */
  private void updateResult(MinMaxInfo minInfo) {
    if (minInfo == null || minInfo.val == null) {
      return;
    }
    if (!hasCandidateResult() || minInfo.compareTo(getValue()) <= 0) {
      setValue(minInfo);
    }
  }
}
