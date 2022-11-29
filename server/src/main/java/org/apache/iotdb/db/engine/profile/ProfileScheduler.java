package org.apache.iotdb.db.engine.profile;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProfileScheduler implements IService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ProfileScheduler.class);

  public static final ProfileTaskPoolManager poolManager =
      ProfileTaskPoolManager.getInstance();

  private ScheduledExecutorService profileTask;


  @Override
  public void start() throws StartupException {
    poolManager.start();
    profileTask =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Profile Scheduler");
    profileTask.schedule(this::computeProfile, 0, TimeUnit.MILLISECONDS);

  }

  @Override
  public void stop() {
    profileTask.shutdownNow();
    poolManager.stop();
  }

  private void computeProfile() {
    ProfileComputationTask2 task = new ProfileComputationTask2();
    poolManager.submit(task);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PROFILE_SCHEDULE_SERVICE;
  }

  public static ProfileScheduler getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final ProfileScheduler INSTANCE = new ProfileScheduler();
  }
}
