package org.apache.iotdb.db.engine.preaggregation.scheduler;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.engine.preaggregation.task.PreAggregationTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PreAggregationScheduler implements IService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PreAggregationScheduler.class);

  private static final PreAggregationTaskPoolManager poolManager =
      PreAggregationTaskPoolManager.getInstance();

  private ScheduledExecutorService service;

  private PreAggregationScheduler() {}

  public static PreAggregationScheduler getInstance() {
    return InstanceHolder.INSTANCE;
  }

  @Override
  public void start() throws StartupException {
    poolManager.start();
    service =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ServiceType.PRE_AGGREGATION_SCHEDULER_SERVICE.getName());
    service.schedule(this::run, 0, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    service.shutdownNow();
    poolManager.stop();
  }

  private void run() {
    PreAggregationTask task = new PreAggregationTask();
    poolManager.submit(task);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PRE_AGGREGATION_SCHEDULER_SERVICE;
  }

  private static class InstanceHolder {
    private static final PreAggregationScheduler INSTANCE = new PreAggregationScheduler();
  }
}
