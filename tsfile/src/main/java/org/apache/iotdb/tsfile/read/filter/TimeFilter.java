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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.common.StatField;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.Between;
import org.apache.iotdb.tsfile.read.filter.operator.Eq;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.read.filter.operator.GtEq;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.apache.iotdb.tsfile.read.filter.operator.Lt;
import org.apache.iotdb.tsfile.read.filter.operator.LtEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotFilter;

import java.util.*;

public class TimeFilter {

  private TimeFilter() {}

  public static TimeEq eq(long value) {
    return new TimeEq(value);
  }

  public static TimeGt gt(long value) {
    return new TimeGt(value);
  }

  public static TimeGtEq gtEq(long value) {
    return new TimeGtEq(value);
  }

  public static TimeLt lt(long value) {
    return new TimeLt(value);
  }

  public static TimeLtEq ltEq(long value) {
    return new TimeLtEq(value);
  }

  public static TimeNotFilter not(Filter filter) {
    return new TimeNotFilter(filter);
  }

  public static TimeNotEq notEq(long value) {
    return new TimeNotEq(value);
  }

  public static TimeIn in(Set<Long> values, boolean not) {
    return new TimeIn(values, not);
  }

  public static TimeBetween between(long value1, long value2, boolean not) {
    return new TimeBetween(value1, value2, not);
  }

  public static class TimeBetween extends Between {

    private TimeBetween(long value1, long value2, boolean not) {
      super(value1, value2, FilterType.TIME_FILTER, not);
    }

    @Override
    public List<TimeRange> getTimeRange() {
      if (!not) {
        return Collections.singletonList(new TimeRange((long) value1, (long) value2));
      }
      TimeRange timeRange1 = new TimeRange(Long.MIN_VALUE, (long) value1);
      TimeRange timeRange2 = new TimeRange((long) value2, Long.MAX_VALUE);
      timeRange1.setRightClose(false);
      timeRange2.setLeftClose(false);
      return Arrays.asList(timeRange1, timeRange2);
    }

    @Override
    public String getSQLString() {
      if (!not) {
        return String.format(
            "(%s <= %d AND %s >= %d)",
            StatField.END_TIMESTAMP, value2, StatField.START_TIMESTAMP, value1);
      }
      return String.format(
          "(%s < %d OR %s > %d)",
          StatField.END_TIMESTAMP, value1, StatField.START_TIMESTAMP, value2);
    }
  }

  public static class TimeIn extends In {

    private TimeIn(Set<Long> values, boolean not) {
      super(values, FilterType.TIME_FILTER, not);
    }

    private List<TimeRange> getInTimeRange() {
      List<TimeRange> res = new ArrayList<>();
      values.forEach(x -> res.add(new TimeRange((long) x, (long) x)));
      return TimeRange.sortAndMerge(res);
    }

    @Override
    public List<TimeRange> getTimeRange() {
      List<TimeRange> res = getInTimeRange();
      if (!not) {
        return res;
      }
      return TimeRange.getComplement(res);
    }

    @Override
    public String getSQLString() {
      StringJoiner stringJoiner = new StringJoiner(" OR ");
      if (!not) {
        values.forEach(value -> stringJoiner.add(String.format(
            "(%s = %d AND %s = %d)",
            StatField.START_TIMESTAMP, value, StatField.END_TIMESTAMP, value)));
        return stringJoiner.toString();
      }

      List<Long> valueList = new ArrayList<>(values);
      Collections.sort(valueList);
      int n = valueList.size();
      for (int i = 0; i < n; i++) {
        long curValue = valueList.get(i);
        if (i == 0) {
          stringJoiner.add(String.format(
              "(%s < %d)",
              StatField.END_TIMESTAMP, curValue));
        }
        if (i > 0) {
          long preValue = valueList.get(i - 1);
          stringJoiner.add(String.format(
              "(%s < %d AND %s > %d)",
              StatField.END_TIMESTAMP, curValue, StatField.START_TIMESTAMP, preValue));
        }
        if (i == n - 1) {
          stringJoiner.add(String.format(
              "(%s > %d)",
              StatField.START_TIMESTAMP, curValue));
        }
      }
      return stringJoiner.toString();
    }
  }

  public static class TimeEq extends Eq {

    private TimeEq(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRange() {
      return Collections.singletonList(new TimeRange((long) value, (long) value));
    }

    @Override
    public String getSQLString() {
      return String.format(
          "(%s = %d AND %s = %d)",
          StatField.START_TIMESTAMP, value, StatField.END_TIMESTAMP, value);
    }
  }

  public static class TimeNotEq extends NotEq {

    private TimeNotEq(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRange() {
      TimeRange timeRange1 = new TimeRange(Long.MIN_VALUE, (long) value);
      TimeRange timeRange2 = new TimeRange((long) value, Long.MAX_VALUE);
      timeRange1.setRightClose(false);
      timeRange2.setLeftClose(false);
      return Arrays.asList(timeRange1, timeRange2);
    }

    @Override
    public String getSQLString() {
      return String.format(
          "(%s < %d OR %s > %d)",
          StatField.END_TIMESTAMP, value, StatField.START_TIMESTAMP, value);
    }
  }

  public static class TimeGt extends Gt {

    private TimeGt(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRange() {
      TimeRange timeRange = new TimeRange((long) value, Long.MAX_VALUE);
      timeRange.setLeftClose(false);
      return Collections.singletonList(timeRange);
    }

    @Override
    public String getSQLString() {
      return String.format(
          "(%s > %d)",
          StatField.START_TIMESTAMP, value);
    }
  }

  public static class TimeGtEq extends GtEq {

    private TimeGtEq(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRange() {
      TimeRange timeRange = new TimeRange((long) value, Long.MAX_VALUE);
      return Collections.singletonList(timeRange);
    }

    @Override
    public String getSQLString() {
      return String.format(
          "(%s >= %d)",
          StatField.START_TIMESTAMP, value);
    }
  }

  public static class TimeLt extends Lt {

    private TimeLt(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRange() {
      TimeRange timeRange = new TimeRange(Long.MIN_VALUE, (long) value);
      timeRange.setRightClose(false);
      return Collections.singletonList(timeRange);
    }

    @Override
    public String getSQLString() {
      return String.format(
          "(%s < %d)",
          StatField.END_TIMESTAMP, value);
    }
  }

  public static class TimeLtEq extends LtEq {

    private TimeLtEq(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRange() {
      TimeRange timeRange = new TimeRange(Long.MIN_VALUE, (long) value);
      return Collections.singletonList(timeRange);
    }

    @Override
    public String getSQLString() {
      return String.format(
          "(%s <= %d)",
          StatField.END_TIMESTAMP, value);
    }
  }

  public static class TimeNotFilter extends NotFilter {

    private TimeNotFilter(Filter filter) {
      super(filter);
    }
  }

  /**
   * returns a default time filter by whether it's an ascending query.
   *
   * <p>If the data is read in descending order, we use the largest timestamp to set to the filter,
   * so the filter should be TimeLtEq. If the data is read in ascending order, we use the smallest
   * timestamp to set to the filter, so the filter should be TimeGtEq.
   */
  public static Filter defaultTimeFilter(boolean ascending) {
    return ascending ? TimeFilter.gtEq(Long.MIN_VALUE) : TimeFilter.ltEq(Long.MAX_VALUE);
  }
}
