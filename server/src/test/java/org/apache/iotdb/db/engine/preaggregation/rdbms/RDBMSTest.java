package org.apache.iotdb.db.engine.preaggregation.rdbms;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.preaggregation.api.TsFileSeriesStat;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import junit.framework.TestCase;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Assert;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

public class RDBMSTest extends TestCase {

  private static final RDBMS DB = RDBMS.getInstance();

  public void testAggregate() throws IllegalPathException {
    PartialPath seriesPath = new PartialPath("root.test.g_0.d_0.s_0");
    AggregationType aggregationType = AggregationType.PREAGG_SUM;
    Filter timeFilter =
        FilterFactory.or(
            TimeFilter.between(1600000000000L, 1600769999000L, false),
            TimeFilter.gt(1606930000000L));
    Pair<Double, Double> aggregateResult = DB.aggregate(seriesPath, aggregationType, timeFilter);
    String a = "";
  }

  public void testGetNewTimeFilter() {
    Filter res;
    Path seriesPath = new Path("root.test.g_0.d_0.s_0");
    res = DB.getNewTimeFilter(null, seriesPath);
    Assert.assertNull(res);

    Filter timeFilter =
        FilterFactory.or(
            TimeFilter.between(1600000000000L, 1600769999000L, false),
            TimeFilter.gt(1606930000000L));

    res = DB.getNewTimeFilter(timeFilter, seriesPath);
  }

  public void testGetReadFileSeriesSQL() {
    Filter timeFilter = TimeFilter.gt(1606930000000L);
    String sql = DB.getReadFileSeriesSQL(timeFilter);
    String a = "";
  }

  public void testGetReadChunkSeriesSQL() {
    Filter timeFilter = TimeFilter.gt(1606930000000L);
    String sql1 = DB.getReadChunkSeriesSQL(null, timeFilter);
    String sql2 = DB.getReadChunkSeriesSQL(new HashSet<>(), timeFilter);
    String sql3 = DB.getReadChunkSeriesSQL(new HashSet<>(Arrays.asList("file1", "file2", "file3")), timeFilter);
  }

  public void testGetAllStatsUsed() {
    Filter timeFilter = TimeFilter.gt(1606930000000L);
    String seriesPath = "root.test.g_0.d_0.s_0";
    Map<String, TsFileSeriesStat>  res = DB.getStatsUsed(seriesPath, timeFilter);
    String a = "";
  }
}
