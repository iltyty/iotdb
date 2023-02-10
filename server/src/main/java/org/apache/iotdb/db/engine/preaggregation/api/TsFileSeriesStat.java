package org.apache.iotdb.db.engine.preaggregation.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Used to store used statistical information of one TsFile when querying.
public class TsFileSeriesStat {
  private boolean fileStatUsed;
  private Map<Long, Boolean> chunkStatsUsed;

  public TsFileSeriesStat() {
    fileStatUsed = false;
    chunkStatsUsed = new HashMap<>();
  }

  public boolean getFileStatUsed() {
    return fileStatUsed;
  }

  public Map<Long, Boolean> getChunkStatsUsed() {
    return chunkStatsUsed;
  }

  public void setFileStatUsed(boolean fileStatUsed) {
    this.fileStatUsed = fileStatUsed;
  }

  public void setChunkStatsUsed(long chunkOffset, boolean isUsed) {
    chunkStatsUsed.put(chunkOffset, isUsed);
  }
}
