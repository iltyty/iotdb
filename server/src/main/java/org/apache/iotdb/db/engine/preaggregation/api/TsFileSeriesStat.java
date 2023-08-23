package org.apache.iotdb.db.engine.preaggregation.api;

import java.util.HashMap;
import java.util.Map;

// Used to store used statistical information of one TsFile when querying.
public class TsFileSeriesStat {
  private SeriesStat fileStat;
  private final Map<Long, SeriesStat> chunkStats;
  private final Map<Long, Map<Long, SeriesStat>> pageStats;

  public TsFileSeriesStat() {
    chunkStats = new HashMap<>();
    pageStats = new HashMap<>();
  }

  public SeriesStat getFileStat() {
    return fileStat;
  }

  public Map<Long, SeriesStat> getChunkStats() {
    return chunkStats;
  }

  public Map<Long, Map<Long, SeriesStat>> getPageStats() {
    return pageStats;
  }

  public void setFileStat(SeriesStat fileStat) {
    this.fileStat = fileStat;
  }

  public void setChunkStats(long chunkOffset, SeriesStat stat) {
    chunkStats.put(chunkOffset, stat);
  }

  public void setPageStats(long chunkOffset, long pageStartTimestamp, SeriesStat stat) {
    this.pageStats.computeIfAbsent(chunkOffset, x -> new HashMap<>());
    this.pageStats.get(chunkOffset).put(pageStartTimestamp, stat);
  }
}
