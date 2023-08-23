package org.apache.iotdb.db.engine.preaggregation.task;

import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.preaggregation.api.FileSeriesStat;
import org.apache.iotdb.db.engine.preaggregation.api.SeriesStat;
import org.apache.iotdb.db.engine.preaggregation.rdbms.RDBMS;
import org.apache.iotdb.db.engine.preaggregation.util.PreAggregationUtil;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PreAggregationTask implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PreAggregationTask.class);
  private static final RDBMS DB = RDBMS.getInstance();
  private static final String DATA_DIR =
      "D:\\\\Work\\\\Projects\\\\iotdb\\\\data\\\\data\\\\sequence\\\\root.test\\\\0\\\\0";

  @Override
  public void run() {
    Map<String, Long> allTsFilePaths = PreAggregationUtil.getAllTsFiles(DATA_DIR);
    List<String> filesPathsUpdating = DB.getAllTsFilesNeedUpdating(allTsFilePaths);
    if (filesPathsUpdating.isEmpty()) {
      return;
    }

    for (int i = 0; i < filesPathsUpdating.size(); i++) {
      String tsFilePath = filesPathsUpdating.get(i);
      LOGGER.info(String.format("%d: %s", i, tsFilePath));

      Collection<Modification> allModifications =
          new TsFileResource(new File(tsFilePath)).getModFile().getModifications();
      try {
        PreAggregationUtil.scanOneTsFile(tsFilePath, allModifications);
      } catch (IOException e) {
        e.printStackTrace();
        LOGGER.error(e.getMessage());
      }
    }
    LOGGER.info("Pre-Aggregation calculation finished.");
  }
}
