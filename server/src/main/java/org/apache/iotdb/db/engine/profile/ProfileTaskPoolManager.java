package org.apache.iotdb.db.engine.profile;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.rescon.AbstractPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProfileTaskPoolManager extends AbstractPoolManager {

  public static final String POOL_NAME = "ProfileComputationPool";
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ProfileTaskPoolManager.class);

  private ProfileTaskPoolManager() {
    pool = IoTDBThreadPoolFactory.newFixedThreadPool(1, POOL_NAME);
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public void start() {
    if (pool == null) {
      pool = IoTDBThreadPoolFactory.newFixedThreadPool(1, POOL_NAME);
    }
    LOGGER.info("Profile computation pool manager started");
  }

  @Override
  public String getName() {
    return POOL_NAME;
  }

  public static ProfileTaskPoolManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final ProfileTaskPoolManager INSTANCE =
        new ProfileTaskPoolManager();
  }
}
