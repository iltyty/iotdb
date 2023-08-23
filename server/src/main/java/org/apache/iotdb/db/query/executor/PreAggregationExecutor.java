package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.preaggregation.api.SeriesStat;
import org.apache.iotdb.db.engine.preaggregation.api.TsFileSeriesStat;
import org.apache.iotdb.db.engine.preaggregation.exception.UnsupportedAggregationTypeException;
import org.apache.iotdb.db.engine.preaggregation.rdbms.RDBMS;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesPreAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PreAggregationExecutor extends AggregationExecutor {

  private static final RDBMS DB = RDBMS.getInstance();

  protected PreAggregationExecutor(QueryContext context, AggregationPlan aggregationPlan) {
    super(context, aggregationPlan);
  }

  @Override
  protected void aggregateOneSeries(
      PartialPath seriesPath, List<Integer> indexes, Set<String> measurements, Filter timeFilter)
      throws IOException, QueryProcessException, StorageEngineException {
    List<AggregateResult> ascAggregateResultList = new ArrayList<>();
    List<AggregateResult> descAggregateResultList = new ArrayList<>();
    boolean[] isAsc = new boolean[aggregateResultList.length];

    TSDataType tsDataType = dataTypes.get(indexes.get(0));
    for (int i : indexes) {
      // construct AggregateResult
      AggregateResult aggregateResult =
          AggregateResultFactory.getAggrResultByName(aggregations.get(i), tsDataType);
      if (aggregateResult.isAscending()) {
        ascAggregateResultList.add(aggregateResult);
        isAsc[i] = true;
      } else {
        descAggregateResultList.add(aggregateResult);
      }
    }

    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance()
            .getQueryDataSource(seriesPath, context, timeFilter, ascending);
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    // get rdbms aggregation result
    Map<String, TsFileSeriesStat> rdbmsResult = DB.aggregate(seriesPath, timeFilter);

    if (!ascAggregateResultList.isEmpty()) {
      QueryUtils.fillOrderIndexes(queryDataSource, seriesPath.getDevice(), true);
      int remainingToCalculate = ascAggregateResultList.size();
      boolean[] isCalculatedArray = new boolean[ascAggregateResultList.size()];
      try {
        remainingToCalculate = aggregateFromRDBMS(
            rdbmsResult, ascAggregateResultList, isCalculatedArray, remainingToCalculate);
      } catch (UnsupportedAggregationTypeException e) {
        throw new QueryProcessException(e.getMessage());
      }

      if (remainingToCalculate > 0) {
        SeriesPreAggregateReader seriesReader =
            new SeriesPreAggregateReader(
                rdbmsResult,
                seriesPath,
                measurements,
                tsDataType,
                context,
                queryDataSource,
                timeFilter,
                null,
                null,
                true);
        aggregateFromReader(
            seriesReader, ascAggregateResultList, isCalculatedArray, remainingToCalculate);
      }
    }
    if (!descAggregateResultList.isEmpty()) {
      QueryUtils.fillOrderIndexes(queryDataSource, seriesPath.getDevice(), false);
      int remainingToCalculate = descAggregateResultList.size();
      boolean[] isCalculatedArray = new boolean[descAggregateResultList.size()];
      try {
        remainingToCalculate = aggregateFromRDBMS(
            rdbmsResult, descAggregateResultList, isCalculatedArray, remainingToCalculate);
      } catch (UnsupportedAggregationTypeException e) {
        throw new QueryProcessException(e.getMessage());
      }

      if (remainingToCalculate > 0) {
        SeriesPreAggregateReader seriesReader =
            new SeriesPreAggregateReader(
                rdbmsResult,
                seriesPath,
                measurements,
                tsDataType,
                context,
                queryDataSource,
                timeFilter,
                null,
                null,
                false);
        aggregateFromReader(
            seriesReader, descAggregateResultList, isCalculatedArray, remainingToCalculate);
      }
    }

    int ascIndex = 0;
    int descIndex = 0;
    for (int i : indexes) {
      aggregateResultList[i] =
          isAsc[i]
              ? ascAggregateResultList.get(ascIndex++)
              : descAggregateResultList.get(descIndex++);
    }
  }

  private void aggregateTsFileSeriesStat(
      AggregateResult aggregateResult,
      Map<String, TsFileSeriesStat> tsFileSeriesStatMap) throws UnsupportedAggregationTypeException {
    if (aggregateResult == null || tsFileSeriesStatMap == null) {
      return;
    }
    AggregationType aggregationType = aggregateResult.getAggregationType();
    for (Map.Entry<String, TsFileSeriesStat> entry : tsFileSeriesStatMap.entrySet()) {
      TsFileSeriesStat stat = entry.getValue();
      SeriesStat fileStat = stat.getFileStat();
      Map<Long, SeriesStat> chunkStats = stat.getChunkStats();
      Map<Long, Map<Long, SeriesStat>> pageStats = stat.getPageStats();
      if (fileStat != null) {
        aggregateResult.merge(fileStat.getStatValue(aggregationType));
      }
      for (SeriesStat chunkStat : chunkStats.values()) {
        aggregateResult.merge(chunkStat.getStatValue(aggregationType));
      }
      for (Map<Long, SeriesStat> stats : pageStats.values()) {
        for (SeriesStat pageStat : stats.values()) {
          aggregateResult.merge(pageStat.getStatValue(aggregationType));
        }
      }
    }
  }

  private void aggregateFromRDBMS(
      PartialPath seriesPath,
      Filter filter,
      AggregateResult aggregateResult
  ) throws UnsupportedAggregationTypeException {
    Map<String, TsFileSeriesStat> rdbmsResult =
        DB.aggregate(seriesPath, filter);
    aggregateTsFileSeriesStat(aggregateResult, rdbmsResult);
  }

  private int aggregateFromRDBMS(
      Map<String, TsFileSeriesStat> rdbmsResult,
      List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate) throws UnsupportedAggregationTypeException {
    int newRemainingToCalculate = remainingToCalculate;
    for (int i = 0; i < aggregateResultList.size(); i++) {
      if (isCalculatedArray[i]) {
        continue;
      }
      AggregateResult aggregateResult = aggregateResultList.get(i);
      aggregateTsFileSeriesStat(aggregateResult, rdbmsResult);
      if (aggregateResult.hasFinalResult()) {
        isCalculatedArray[i] = true;
        newRemainingToCalculate--;
        if (newRemainingToCalculate == 0) {
          return 0;
        }
      }
    }
    return newRemainingToCalculate;
  }

  private void aggregateFromReader(
      SeriesPreAggregateReader seriesReader,
      List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate)
      throws QueryProcessException, IOException {
    while (seriesReader.hasNextFile()) {
      if (seriesReader.isCurrentFileCalculated()) {
        seriesReader.skipCurrentFile();
        continue;
      }
      while (seriesReader.hasNextChunk()) {
        if (seriesReader.isCurrentChunkCalculated()) {
          seriesReader.skipCurrentChunk();
          continue;
        }
        remainingToCalculate =
            aggregatePages(
                seriesReader, aggregateResultList, isCalculatedArray, remainingToCalculate);
        if (remainingToCalculate == 0) {
          return;
        }
      }
    }
  }

  private static int aggregatePages(
      SeriesPreAggregateReader seriesReader,
      List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate)
      throws IOException, QueryProcessException {
    while (seriesReader.hasNextPage()) {
      if (seriesReader.isCurrentPageCalculated()) {
        seriesReader.skipCurrentPage();
        continue;
      }
      IBatchDataIterator batchDataIterator = seriesReader.nextPage().getBatchDataIterator();
      remainingToCalculate =
          aggregateBatchData(
              aggregateResultList, isCalculatedArray, remainingToCalculate, batchDataIterator);
    }
    return remainingToCalculate;
  }

  protected void aggregateFromRDBMS(
      Filter filter,
      Map<IReaderByTimestamp, List<List<Integer>>> readerToAggrIndexesMap
  ) throws QueryProcessException {
    for (Map.Entry<IReaderByTimestamp, List<List<Integer>>> entry :
        readerToAggrIndexesMap.entrySet()) {
      PartialPath seriesPath =
          ((SeriesReaderByTimestamp) entry.getKey()).getSeriesPath();
      for (int i = 0; i < entry.getValue().size(); i++) {
        for (Integer index : entry.getValue().get(i)) {
          AggregateResult aggregateResult = aggregateResultList[index];
          try {
            aggregateFromRDBMS(seriesPath, filter, aggregateResult);
          } catch (UnsupportedAggregationTypeException e) {
            throw new QueryProcessException(e.getMessage());
          }
        }
      }
    }
  }
}
