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
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class OperatorTest {

  private static final long EFFICIENCY_TEST_COUNT = 10000000;
  private static final long TESTED_TIMESTAMP = 1513585371L;

  @Test
  public void testEq() {
    Filter timeEq = TimeFilter.eq(100L);
    List<TimeRange> timeRangeList1 = timeEq.getTimeRange();
    Assert.assertTrue(timeEq.satisfy(100, 100));
    Assert.assertFalse(timeEq.satisfy(101, 100));
    Assert.assertEquals(1, timeRangeList1.size());
    Assert.assertEquals(new TimeRange(100L, 100L), timeRangeList1.get(0));

    Filter filter2 = FilterFactory.and(TimeFilter.eq(100L), ValueFilter.eq(50));
    List<TimeRange> timeRangeList2 = filter2.getTimeRange();
    Assert.assertTrue(filter2.satisfy(100, 50));
    Assert.assertFalse(filter2.satisfy(100, 51));
    Assert.assertEquals(1, timeRangeList1.size());
    Assert.assertEquals(new TimeRange(100L, 100L), timeRangeList1.get(0));

    Filter filter3 = ValueFilter.eq(true);
    List<TimeRange> timeRangeList3 = filter3.getTimeRange();
    Assert.assertTrue(filter3.satisfy(100, true));
    Assert.assertFalse(filter3.satisfy(100, false));
    Assert.assertNull(timeRangeList3);
  }

  @Test
  public void testGt() {
    Filter timeGt = TimeFilter.gt(TESTED_TIMESTAMP);
    List<TimeRange> timeRangeList = timeGt.getTimeRange();
    Assert.assertEquals(1, timeRangeList.size());
    Assert.assertEquals(
        new TimeRange(TESTED_TIMESTAMP, Long.MAX_VALUE, false, true), timeRangeList.get(0));
    Assert.assertTrue(timeGt.satisfy(TESTED_TIMESTAMP + 1, 100));
    Assert.assertFalse(timeGt.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeGt.satisfy(TESTED_TIMESTAMP - 1, 100));

    Filter valueGt = ValueFilter.gt(0.01f);
    Assert.assertTrue(valueGt.satisfy(TESTED_TIMESTAMP, 0.02f));
    Assert.assertFalse(valueGt.satisfy(TESTED_TIMESTAMP, 0.01f));
    Assert.assertFalse(valueGt.satisfy(TESTED_TIMESTAMP, -0.01f));

    Filter binaryFilter = ValueFilter.gt(new Binary("test1"));
    Assert.assertTrue(binaryFilter.satisfy(TESTED_TIMESTAMP, new Binary("test2")));
    Assert.assertFalse(binaryFilter.satisfy(TESTED_TIMESTAMP, new Binary("test0")));
  }

  @Test
  public void testGtEq() {
    Filter timeGtEq = TimeFilter.gtEq(TESTED_TIMESTAMP);
    List<TimeRange> timeRangeList = timeGtEq.getTimeRange();
    Assert.assertEquals(1, timeRangeList.size());
    Assert.assertEquals(new TimeRange(TESTED_TIMESTAMP, Long.MAX_VALUE), timeRangeList.get(0));
    Assert.assertTrue(timeGtEq.satisfy(TESTED_TIMESTAMP + 1, 100));
    Assert.assertTrue(timeGtEq.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeGtEq.satisfy(TESTED_TIMESTAMP - 1, 100));

    Filter valueGtEq = ValueFilter.gtEq(0.01);
    Assert.assertTrue(valueGtEq.satisfy(TESTED_TIMESTAMP, 0.02));
    Assert.assertTrue(valueGtEq.satisfy(TESTED_TIMESTAMP, 0.01));
    Assert.assertFalse(valueGtEq.satisfy(TESTED_TIMESTAMP, -0.01));
  }

  @Test
  public void testLt() {
    Filter timeLt = TimeFilter.lt(TESTED_TIMESTAMP);
    List<TimeRange> timeRangeList = timeLt.getTimeRange();
    Assert.assertEquals(1, timeRangeList.size());
    Assert.assertEquals(
        new TimeRange(Long.MIN_VALUE, TESTED_TIMESTAMP, true, false), timeRangeList.get(0));
    Assert.assertTrue(timeLt.satisfy(TESTED_TIMESTAMP - 1, 100));
    Assert.assertFalse(timeLt.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeLt.satisfy(TESTED_TIMESTAMP + 1, 100));

    Filter valueLt = ValueFilter.lt(100L);
    Assert.assertTrue(valueLt.satisfy(TESTED_TIMESTAMP, 99L));
    Assert.assertFalse(valueLt.satisfy(TESTED_TIMESTAMP, 100L));
    Assert.assertFalse(valueLt.satisfy(TESTED_TIMESTAMP, 101L));
  }

  @Test
  public void testLtEq() {
    Filter timeLtEq = TimeFilter.ltEq(TESTED_TIMESTAMP);
    List<TimeRange> timeRangeList = timeLtEq.getTimeRange();
    Assert.assertEquals(1, timeRangeList.size());
    Assert.assertEquals(new TimeRange(Long.MIN_VALUE, TESTED_TIMESTAMP), timeRangeList.get(0));
    Assert.assertTrue(timeLtEq.satisfy(TESTED_TIMESTAMP - 1, 100));
    Assert.assertTrue(timeLtEq.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeLtEq.satisfy(TESTED_TIMESTAMP + 1, 100));

    Filter valueLtEq = ValueFilter.ltEq(100L);
    Assert.assertTrue(valueLtEq.satisfy(TESTED_TIMESTAMP, 99L));
    Assert.assertTrue(valueLtEq.satisfy(TESTED_TIMESTAMP, 100L));
    Assert.assertFalse(valueLtEq.satisfy(TESTED_TIMESTAMP, 101L));
  }

  @Test
  public void testNot() {
    Filter timeLt = TimeFilter.not(TimeFilter.lt(TESTED_TIMESTAMP));
    List<TimeRange> timeRangeList = timeLt.getTimeRange();
    Assert.assertEquals(1, timeRangeList.size());
    Assert.assertEquals(new TimeRange(TESTED_TIMESTAMP, Long.MAX_VALUE), timeRangeList.get(0));
    Assert.assertFalse(timeLt.satisfy(TESTED_TIMESTAMP - 1, 100));
    Assert.assertTrue(timeLt.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertTrue(timeLt.satisfy(TESTED_TIMESTAMP + 1, 100));

    Filter valueLt = ValueFilter.not(ValueFilter.lt(100L));
    Assert.assertFalse(valueLt.satisfy(TESTED_TIMESTAMP, 99L));
    Assert.assertTrue(valueLt.satisfy(TESTED_TIMESTAMP, 100L));
    Assert.assertTrue(valueLt.satisfy(TESTED_TIMESTAMP, 101L));
  }

  @Test
  public void testNotEq() {
    Filter timeNotEq = TimeFilter.notEq(100L);
    List<TimeRange> timeRangeList = timeNotEq.getTimeRange();
    Assert.assertEquals(2, timeRangeList.size());
    Assert.assertEquals(new TimeRange(Long.MIN_VALUE, 100L, true, false), timeRangeList.get(0));
    Assert.assertEquals(new TimeRange(100L, Long.MAX_VALUE, false, true), timeRangeList.get(1));
    Assert.assertFalse(timeNotEq.satisfy(100, 100));
    Assert.assertTrue(timeNotEq.satisfy(101, 100));

    Filter valueNotEq = ValueFilter.notEq(50);
    Assert.assertFalse(valueNotEq.satisfy(100, 50));
    Assert.assertTrue(valueNotEq.satisfy(100, 51));
  }

  @Test
  public void testIn() {
    Filter timeIn = TimeFilter.in(new HashSet<>(Arrays.asList(-100L, 0L, 100L)), false);
    List<TimeRange> inTimeRangeList = timeIn.getTimeRange();
    Assert.assertEquals(3, inTimeRangeList.size());
    Assert.assertEquals(new TimeRange(-100L, -100L), inTimeRangeList.get(0));
    Assert.assertEquals(new TimeRange(0L, 0L), inTimeRangeList.get(1));
    Assert.assertEquals(new TimeRange(100L, 100L), inTimeRangeList.get(2));
    Filter timeInNot = TimeFilter.in(new HashSet<>(Arrays.asList(-100L, 0L, 100L)), true);
    List<TimeRange> inNotTimeRangeList = timeInNot.getTimeRange();
    Assert.assertEquals(4, inNotTimeRangeList.size());
    Assert.assertEquals(
        new TimeRange(Long.MIN_VALUE, -100L, true, false), inNotTimeRangeList.get(0));
    Assert.assertEquals(new TimeRange(-100L, 0L, false, false), inNotTimeRangeList.get(1));
    Assert.assertEquals(new TimeRange(0L, 100L, false, false), inNotTimeRangeList.get(2));
    Assert.assertEquals(
        new TimeRange(100L, Long.MAX_VALUE, false, true), inNotTimeRangeList.get(3));
  }

  @Test
  public void testBetween() {
    Filter timeBetween = TimeFilter.between(-100L, 100L, false);
    List<TimeRange> betweenTimeRangeList = timeBetween.getTimeRange();
    Assert.assertEquals(1, betweenTimeRangeList.size());
    Assert.assertEquals(new TimeRange(-100L, 100L), betweenTimeRangeList.get(0));
    Filter timeBetweenNot = TimeFilter.between(-100L, 100L, true);
    List<TimeRange> betweenNotTimeRangeList = timeBetweenNot.getTimeRange();
    Assert.assertEquals(2, betweenNotTimeRangeList.size());
    Assert.assertEquals(
        new TimeRange(Long.MIN_VALUE, -100L, true, false), betweenNotTimeRangeList.get(0));
    Assert.assertEquals(
        new TimeRange(100L, Long.MAX_VALUE, false, true), betweenNotTimeRangeList.get(1));
  }

  @Test
  public void testAndOr() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
    List<TimeRange> andTimeRangeList = andFilter.getTimeRange();
    Assert.assertEquals(1, andTimeRangeList.size());
    Assert.assertEquals(new TimeRange(100L, Long.MAX_VALUE, false, true), andTimeRangeList.get(0));
    Assert.assertTrue(andFilter.satisfy(101L, 50d));
    Assert.assertFalse(andFilter.satisfy(101L, 60d));
    Assert.assertFalse(andFilter.satisfy(99L, 50d));

    Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));
    List<TimeRange> orTimeRangeList = orFilter.getTimeRange();
    Assert.assertEquals(1, orTimeRangeList.size());
    Assert.assertEquals(new TimeRange(100L, Long.MAX_VALUE, false, true), orTimeRangeList.get(0));
    Assert.assertTrue(orFilter.satisfy(101L, 50d));
    Assert.assertFalse(orFilter.satisfy(101L, 60d));
    Assert.assertTrue(orFilter.satisfy(1000L, 50d));

    Filter andFilter2 = FilterFactory.and(orFilter, ValueFilter.notEq(50.0));
    Assert.assertFalse(andFilter2.satisfy(101L, 50d));
    Assert.assertFalse(andFilter2.satisfy(101L, 60d));
    Assert.assertTrue(andFilter2.satisfy(1000L, 51d));

    Filter orFilter2 = FilterFactory.or(andFilter2, TimeFilter.between(-100, 0, false));
    List<TimeRange> orTimeRangeList2 = orFilter2.getTimeRange();
    Assert.assertEquals(2, orTimeRangeList2.size());
    Assert.assertEquals(new TimeRange(-100, 0), orTimeRangeList2.get(0));

    Filter andFilter3 = FilterFactory.and(andFilter2, TimeFilter.between(200, 1000, false));
    List<TimeRange> andTimeRangeList3 = andFilter3.getTimeRange();
    Assert.assertEquals(1, andTimeRangeList3.size());
    Assert.assertEquals(new TimeRange(200, 1000), andTimeRangeList3.get(0));

    Filter andFilter4 = FilterFactory.and(andFilter3, TimeFilter.lt(200));
    List<TimeRange> andTimeRangeList4 = andFilter4.getTimeRange();
    Assert.assertEquals(0, andTimeRangeList4.size());

    Filter andFilter5 = FilterFactory.and(andFilter3, TimeFilter.gt(1000));
    List<TimeRange> andTimeRangeList5 = andFilter5.getTimeRange();
    Assert.assertEquals(0, andTimeRangeList5.size());
  }

  @Test
  public void testGetSQLString() {
    // TimeFilter tests
    Filter timeEqFilter = TimeFilter.eq(100L);
    Assert.assertEquals(
        String.format("(%s = 100 AND %s = 100)", StatField.START_TIMESTAMP, StatField.END_TIMESTAMP),
        timeEqFilter.getSQLString());

    Filter timeEqNotFilter = TimeFilter.notEq(100L);
    Assert.assertEquals(
        String.format("(%s < 100 OR %s > 100)", StatField.END_TIMESTAMP, StatField.START_TIMESTAMP),
        timeEqNotFilter.getSQLString());

    Filter timeLtFilter = TimeFilter.lt(100L);
    Assert.assertEquals(
        String.format("(%s < 100)", StatField.END_TIMESTAMP),
        timeLtFilter.getSQLString());

    Filter timeLtEqFilter = TimeFilter.ltEq(100L);
    Assert.assertEquals(
        String.format("(%s <= 100)", StatField.END_TIMESTAMP),
        timeLtEqFilter.getSQLString());

    Filter timeGtFilter = TimeFilter.gt(100L);
    Assert.assertEquals(
        String.format("(%s > 100)", StatField.START_TIMESTAMP),
        timeGtFilter.getSQLString());

    Filter timeGtEqFilter = TimeFilter.gtEq(100L);
    Assert.assertEquals(
        String.format("(%s >= 100)", StatField.START_TIMESTAMP),
        timeGtEqFilter.getSQLString());

    Filter timeInFilter = TimeFilter.in(new HashSet<Long>(Arrays.asList(100L, 200L)), false);
    Assert.assertEquals(
        String.format("(%s = 100 AND %s = 100) OR (%s = 200 AND %s = 200)",
            StatField.START_TIMESTAMP, StatField.END_TIMESTAMP,
            StatField.START_TIMESTAMP, StatField.END_TIMESTAMP),
        timeInFilter.getSQLString());

    Filter timeInNotFilter = TimeFilter.in(new HashSet<Long>(Arrays.asList(100L, 200L)), true);
    Assert.assertEquals(
        String.format("(%s < 100) OR (%s < 200 AND %s > 100) OR (%s > 200)",
            StatField.END_TIMESTAMP, StatField.END_TIMESTAMP,
            StatField.START_TIMESTAMP, StatField.START_TIMESTAMP),
        timeInNotFilter.getSQLString());

    Filter timeBetweenFilter = TimeFilter.between(100L, 200L, false);
    Assert.assertEquals(
        String.format("(%s <= 200 AND %s >= 100)", StatField.END_TIMESTAMP, StatField.START_TIMESTAMP),
        timeBetweenFilter.getSQLString());

    Filter timeBetweenNotFilter = TimeFilter.between(100L, 200L, true);
    Assert.assertEquals(
        String.format("(%s < 100 OR %s > 200)", StatField.END_TIMESTAMP, StatField.START_TIMESTAMP),
        timeBetweenNotFilter.getSQLString());

    Filter timeNotFilter = TimeFilter.not(timeBetweenNotFilter);
    Assert.assertEquals(
        String.format("NOT (%s < 100 OR %s > 200)", StatField.END_TIMESTAMP, StatField.START_TIMESTAMP),
        timeNotFilter.getSQLString());

    // ValueFilter tests
    Filter valueEqFilter = ValueFilter.eq(100L);
    Assert.assertEquals(
        String.format("(%s = 100 AND %s = 100)", StatField.MIN_VALUE, StatField.MAX_VALUE),
        valueEqFilter.getSQLString());

    Filter valueEqNotFilter = ValueFilter.notEq(100L);
    Assert.assertEquals(
        String.format("(%s < 100 OR %s > 100)", StatField.MAX_VALUE, StatField.MIN_VALUE),
        valueEqNotFilter.getSQLString());

    Filter valueLtFilter = ValueFilter.lt(100L);
    Assert.assertEquals(
        String.format("(%s < 100)", StatField.MAX_VALUE),
        valueLtFilter.getSQLString());

    Filter valueLtEqFilter = ValueFilter.ltEq(100L);
    Assert.assertEquals(
        String.format("(%s <= 100)", StatField.MAX_VALUE),
        valueLtEqFilter.getSQLString());

    Filter valueGtFilter = ValueFilter.gt(100L);
    Assert.assertEquals(
        String.format("(%s > 100)", StatField.MIN_VALUE),
        valueGtFilter.getSQLString());

    Filter valueGtEqFilter = ValueFilter.gtEq(100L);
    Assert.assertEquals(
        String.format("(%s >= 100)", StatField.MIN_VALUE),
        valueGtEqFilter.getSQLString());

    Filter valueInFilter = ValueFilter.in(new HashSet<Long>(Arrays.asList(100L, 200L)), false);
    Assert.assertEquals(
        String.format("(%s = 100 AND %s = 100) OR (%s = 200 AND %s = 200)",
            StatField.MIN_VALUE, StatField.MAX_VALUE,
            StatField.MIN_VALUE, StatField.MAX_VALUE),
        valueInFilter.getSQLString());

    Filter valueInNotFilter = ValueFilter.in(new HashSet<Long>(Arrays.asList(100L, 200L)), true);
    Assert.assertEquals(
        String.format("(%s < 100) OR (%s < 200 AND %s > 100) OR (%s > 200)",
            StatField.MAX_VALUE, StatField.MAX_VALUE,
            StatField.MIN_VALUE, StatField.MIN_VALUE),
        valueInNotFilter.getSQLString());
  }

  @Test
  public void testWrongUsage() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(true));
    try {
      andFilter.satisfy(101L, 50);
      Assert.fail();
    } catch (ClassCastException e) {

    }
  }

  @Test
  public void efficiencyTest() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
    Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));

    long startTime = System.currentTimeMillis();
    for (long i = 0; i < EFFICIENCY_TEST_COUNT; i++) {
      orFilter.satisfy(i, i + 0.1);
    }
    long endTime = System.currentTimeMillis();
    System.out.println(
        "EfficiencyTest for Filter: \n\tFilter Expression = "
            + orFilter
            + "\n\tCOUNT = "
            + EFFICIENCY_TEST_COUNT
            + "\n\tTotal Time = "
            + (endTime - startTime)
            + "ms.");
  }
}
