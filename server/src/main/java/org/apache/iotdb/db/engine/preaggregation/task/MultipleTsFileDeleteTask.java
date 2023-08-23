package org.apache.iotdb.db.engine.preaggregation.task;

import org.apache.iotdb.db.engine.preaggregation.rdbms.RDBMS;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MultipleTsFileDeleteTask implements Runnable {
  public static final Logger LOGGER = LoggerFactory.getLogger(MultipleTsFileDeleteTask.class);

  private final List<TsFileResource> tsFileResources;

  private static final RDBMS DB = RDBMS.getInstance();

  public MultipleTsFileDeleteTask(List<TsFileResource> tsFileResources) {
    this.tsFileResources = tsFileResources;
  }

  @Override
  public void run() {
    DB.deleteAccordingToTsFileResources(tsFileResources);
  }
}
