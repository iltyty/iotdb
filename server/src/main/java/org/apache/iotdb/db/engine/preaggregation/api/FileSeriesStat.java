package org.apache.iotdb.db.engine.preaggregation.api;

import org.apache.iotdb.db.engine.preaggregation.rdbms.RDBMS;
import org.apache.iotdb.db.engine.preaggregation.util.PreAggregationUtil;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.List;

// Used to store statistical information of one TsFile when calculating.
// Write to RDBMS per chunk.
public class FileSeriesStat {
  private int fid = -1;
  private int sid = -1;

  private Path seriesPath;
  private String filePath;

  public int currentChunkIndex = -1;
  public long currentChunkOffset = 0;

  public SeriesStat fileStat;
  public SeriesStat chunkStat;
  public List<SeriesStat> pageStats;

  private static final RDBMS DB = RDBMS.getInstance();

  public FileSeriesStat(Path seriesPath, String filePath) {
    this.seriesPath = seriesPath;
    this.filePath = filePath;
    fileStat = new SeriesStat();
  }

  public void setCurrentChunkOffset(long currentChunkOffset) {
    this.currentChunkOffset = currentChunkOffset;
  }

  public void addPageSeriesStat(SeriesStat stat) {
    pageStats.add(stat);
    chunkStat.merge(stat);
  }

  public void startNewChunk() {
    currentChunkIndex++;
    chunkStat = new SeriesStat();
    pageStats = new ArrayList<>();
  }

  public void endChunk() {
    fileStat.merge(chunkStat);
    if (currentChunkIndex == 0) {
      // the first chunk in this TsFile
      sid = DB.writeToSeries(seriesPath);
      fid = DB.writeToFile(filePath, PreAggregationUtil.getFileVersion(filePath));
    }
    DB.writeOneChunkStat(fid, sid, this);
  }

  public void endFile() {
    DB.writeToFileSeriesStat(fid, sid, fileStat);
  }
}
