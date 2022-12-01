package org.apache.iotdb.db.engine.preaggregation.api;

import org.apache.iotdb.tsfile.read.common.BatchData;

public class SeriesStat {
  private long cnt;
  private double sum;
  private double squareSum;
  private double minValue = Long.MAX_VALUE;
  private double maxValue = Long.MIN_VALUE;
  private long startTimestamp = Long.MAX_VALUE;
  private long endTimestamp = Long.MIN_VALUE;

  public SeriesStat() {}

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
}
