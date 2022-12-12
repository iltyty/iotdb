package org.apache.iotdb.db.query.aggregation.impl;

import org.apache.iotdb.db.engine.preaggregation.api.SeriesStat;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class PreAggSumResult extends SumAggrResult {

  public PreAggSumResult(TSDataType seriesDataType) {
    super(seriesDataType);
    setAggregationType(AggregationType.PREAGG_SUM);
  }

  @Override
  public void merge(Object sumVal) throws UnSupportedDataTypeException {
    super.updateSum(sumVal);
  }

  @Override
  public void updateResultFromSeriesStat(SeriesStat seriesStat) {
    if (seriesStat == null || seriesStat.getCnt() == 0) {
      return;
    }

    setTime(seriesStat.getStartTimestamp());
    setDoubleValue(getDoubleValue() + seriesStat.getSum());
  }
}
