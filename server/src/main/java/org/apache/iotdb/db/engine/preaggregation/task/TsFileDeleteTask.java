package org.apache.iotdb.db.engine.preaggregation.task;

import org.apache.iotdb.db.engine.preaggregation.rdbms.RDBMS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileDeleteTask implements Runnable {
  public static final Logger LOGGER = LoggerFactory.getLogger(TsFileDeleteTask.class);

  private final String tsFilePath;
  private static final RDBMS DB = RDBMS.getInstance();

  public TsFileDeleteTask(String tsFilePath) {
    this.tsFilePath = tsFilePath;
  }

  @Override
  public void run() {
    LOGGER.info(String.format("Deleting stat for %s", tsFilePath));
    DB.deleteFromFileTable(tsFilePath);
    LOGGER.info(String.format("TsFileDelete task for %s finished.", tsFilePath));
  }
}
