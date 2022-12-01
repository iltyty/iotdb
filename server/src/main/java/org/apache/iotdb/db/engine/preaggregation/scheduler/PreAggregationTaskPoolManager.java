package org.apache.iotdb.db.engine.preaggregation.scheduler;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.rescon.AbstractPoolManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreAggregationTaskPoolManager extends AbstractPoolManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PreAggregationTaskPoolManager.class);

  private static final int THREAD_NUM = 4;

  private PreAggregationTaskPoolManager() {
    pool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            THREAD_NUM, ThreadName.PRE_AGGREGATION_SERVICE.getName());
  }

  public static PreAggregationTaskPoolManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public void start() {
    if (pool == null) {
      pool = IoTDBThreadPoolFactory.newFixedThreadPool(THREAD_NUM, "PreAggregationComputation");
    }
    LOGGER.info("Pre-Aggregation task manager started.");
  }

  @Override
  public void stop() {
    super.stop();
    LOGGER.info("Pre-Aggregation task manager stopped.");
  }

  @Override
  public String getName() {
    return "Pre-Aggregation Computation Task";
  }

  private static class InstanceHolder {
    private static final PreAggregationTaskPoolManager INSTANCE =
        new PreAggregationTaskPoolManager();
  }
}
