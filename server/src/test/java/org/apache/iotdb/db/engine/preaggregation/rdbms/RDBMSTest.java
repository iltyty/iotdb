package org.apache.iotdb.db.engine.preaggregation.rdbms;

import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import junit.framework.TestCase;
import org.checkerframework.checker.units.qual.A;
import org.junit.Assert;

public class RDBMSTest extends TestCase {

  private static final RDBMS DB = RDBMS.getInstance();

  public void testAggregate() {
    Path seriesPath = new Path("root.test.g_0.d_0.s_0");
    AggregationType aggregationType = AggregationType.PREAGG_SUM;
    Filter timeFilter =
        FilterFactory.or(
            TimeFilter.between(1600000000000L, 1600769999000L, false),
            TimeFilter.gt(1606930000000L));
    double aggregateResult = DB.aggregate(seriesPath, aggregationType, timeFilter);
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
}
