package org.apache.iotdb.db.engine.preaggregation.exception;


import org.apache.iotdb.db.query.aggregation.AggregationType;

public class UnsupportedAggregationTypeException extends Exception {

  public UnsupportedAggregationTypeException(AggregationType aggregationType) {
    super("Unsupported aggregation type: " + aggregationType);
  }
}
