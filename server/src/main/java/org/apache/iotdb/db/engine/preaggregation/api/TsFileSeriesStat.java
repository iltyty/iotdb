package org.apache.iotdb.db.engine.preaggregation.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Used to store statistical information of one TsFile when querying.
public class TsFileSeriesStat {
  private SeriesStat fileStat;
  private Map<Long, SeriesStat> chunkStats; // key: chunk offset
  private Map<Long, List<SeriesStat>> pageStats;

  public TsFileSeriesStat() {
    fileStat = new SeriesStat();
    chunkStats = new HashMap<>();
    pageStats = new HashMap<>();
  }

  public SeriesStat getFileStat() {
    return fileStat;
  }

  public Map<Long, SeriesStat> getChunkStats() {
    return chunkStats;
  }

  public Map<Long, List<SeriesStat>> getPageStats() {
    return pageStats;
  }

  public void setFileStat(SeriesStat fileStat) {
    this.fileStat = fileStat;
  }

  public void addToChunkStats(Long offset, SeriesStat stat) {
    chunkStats.put(offset, stat);
  }

  public void addToPageStats(Long offset, SeriesStat stat) {
    pageStats.putIfAbsent(offset, new ArrayList<>());
    pageStats.get(offset).add(stat);
  }
}
