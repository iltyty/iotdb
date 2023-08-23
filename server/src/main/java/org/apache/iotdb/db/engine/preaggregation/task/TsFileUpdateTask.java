package org.apache.iotdb.db.engine.preaggregation.task;

import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.preaggregation.util.PreAggregationUtil;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

public class TsFileUpdateTask implements Runnable {
  public static final Logger LOGGER = LoggerFactory.getLogger(TsFileUpdateTask.class);

  private final String tsFilePath;

  private static BufferedWriter timeWriter;
  static {
    try {
      timeWriter = new BufferedWriter(new FileWriter("D:\\task.csv"));
      timeWriter.write("Time\n");
    } catch (IOException ignored) {}
  }

  public TsFileUpdateTask(String tsFilePath) {
    this.tsFilePath = tsFilePath;
  }

  @Override
  public void run() {
    LOGGER.info(String.format("Updating stat for %s", tsFilePath));

    long startTime = System.currentTimeMillis();

    Collection<Modification> modifications =
        new TsFileResource(new File(tsFilePath)).getModFile().getModifications();
    try {
      PreAggregationUtil.scanOneTsFile(tsFilePath, modifications);
      long endTime = System.currentTimeMillis();
      timeWriter.write(String.format("%d\n", endTime - startTime));
      timeWriter.flush();
      LOGGER.info(String.format("TsFileFlush task for %s finished.", tsFilePath));
    } catch (IOException e) {
      e.printStackTrace();
      LOGGER.error(e.getMessage());
    }
  }
}
