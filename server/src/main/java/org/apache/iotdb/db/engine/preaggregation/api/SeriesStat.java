package org.apache.iotdb.db.engine.preaggregation.api;

import org.apache.iotdb.tsfile.read.common.BatchData;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SeriesStat implements Cloneable {
  public static final String sumFiled = "sum";
  public static final String cntField = "count";
  public static final String squareSumField = "square_sum";
  public static final String minValueField = "min_value";
  public static final String maxValueField = "max_value";
  public static final String startTimestampField = "start_timestamp";
  public static final String endTimestampField = "end_timestamp";

  private long cnt;
  private double sum;
  private double squareSum;
  private double minValue = Long.MAX_VALUE;
  private double maxValue = Long.MIN_VALUE;
  private long startTimestamp = Long.MAX_VALUE;
  private long endTimestamp = Long.MIN_VALUE;

  public SeriesStat() {}

  // need to ensure rs.next() == true
  public SeriesStat(ResultSet rs) throws SQLException {
    cnt = rs.getLong(cntField);
    sum = rs.getDouble(sumFiled);
    squareSum = rs.getDouble(squareSumField);
    minValue = rs.getDouble(minValueField);
    maxValue = rs.getDouble(maxValueField);
    startTimestamp = rs.getLong(startTimestampField);
    endTimestamp = rs.getLong(endTimestampField);
  }

  public SeriesStat(BatchData batchData) {
    if (batchData.isEmpty()) {
      return;
    }

    cnt = batchData.length();
    startTimestamp = batchData.getMinTimestamp();
    endTimestamp = batchData.getMaxTimestamp();

    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      Object value = batchData.currentValue();
      switch (batchData.getDataType()) {
        case INT32:
          transformInt((int) value);
          break;
        case INT64:
          transformLong((long) value);
          break;
        case FLOAT:
          transformFloat((float) value);
          break;
        case DOUBLE:
          transformDouble((double) value);
          break;
        default:
          break;
      }
      batchData.next();
    }
  }

  private void transformInt(int value) {
    sum += value;
    squareSum += value * value;
    minValue = Math.min(minValue, value);
    maxValue = Math.max(maxValue, value);
  }

  private void transformLong(long value) {
    sum += value;
    squareSum += value * value;
    minValue = Math.min(minValue, value);
    maxValue = Math.max(maxValue, value);
  }

  private void transformFloat(float value) {
    sum += value;
    squareSum += value * value;
    minValue = Math.min(minValue, value);
    maxValue = Math.max(maxValue, value);
  }

  private void transformDouble(double value) {
    sum += value;
    squareSum += value * value;
    minValue = Math.min(minValue, value);
    maxValue = Math.max(maxValue, value);
  }

  public void merge(SeriesStat stat) {
    cnt += stat.cnt;
    sum += stat.sum;
    squareSum += stat.squareSum;
    minValue = Math.min(minValue, stat.minValue);
    maxValue = Math.max(maxValue, stat.maxValue);
    startTimestamp = Math.min(startTimestamp, stat.startTimestamp);
    endTimestamp = Math.max(endTimestamp, stat.endTimestamp);
  }

  public long getCnt() {
    return cnt;
  }

  public double getSum() {
    return sum;
  }

  public double getSquareSum() {
    return squareSum;
  }

  public double getMinValue() {
    return minValue;
  }

  public double getMaxValue() {
    return maxValue;
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  @Override
  public SeriesStat clone() {
    try {
      SeriesStat clone = (SeriesStat) super.clone();
      clone.cnt = cnt;
      clone.sum = sum;
      clone.squareSum = squareSum;
      clone.minValue = minValue;
      clone.maxValue = maxValue;
      clone.startTimestamp = startTimestamp;
      clone.endTimestamp = endTimestamp;
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new AssertionError();
    }
  }
}
