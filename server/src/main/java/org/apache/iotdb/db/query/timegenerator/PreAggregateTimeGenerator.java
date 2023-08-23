package org.apache.iotdb.db.query.timegenerator;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.preaggregation.api.TsFileSeriesStat;
import org.apache.iotdb.db.engine.preaggregation.rdbms.RDBMS;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesPreAggregateReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;
import java.util.Map;

public class PreAggregateTimeGenerator extends ServerTimeGenerator {

  private static final RDBMS DB = RDBMS.getInstance();

  public PreAggregateTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan) throws StorageEngineException {
    super(context, queryPlan);
  }

  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException {
    Filter valueFilter = expression.getFilter();
    PartialPath path = (PartialPath) expression.getSeriesPath();
    TSDataType dataType = path.getSeriesType();
    QueryDataSource queryDataSource;
    try {
      queryDataSource =
          QueryResourceManager.getInstance()
              .getQueryDataSource(path, context, valueFilter, queryPlan.isAscending());
      // update valueFilter by TTL
      valueFilter = queryDataSource.updateFilterUsingTTL(valueFilter);
      fullFilter = valueFilter;
    } catch (Exception e) {
      throw new IOException(e);
    }

    Filter timeFilter = getTimeFilter(valueFilter);

    Map<String, TsFileSeriesStat> rdbmsResult = DB.aggregate((PartialPath) expression.getSeriesPath(), valueFilter);

    return new SeriesPreAggregateReader(
        rdbmsResult,
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        queryDataSource,
        timeFilter,
        valueFilter,
        null,
        queryPlan.isAscending());
  }
}
