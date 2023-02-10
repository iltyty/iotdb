package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.preaggregation.api.SeriesStat;
import org.apache.iotdb.db.engine.preaggregation.api.TsFileSeriesStat;
import org.apache.iotdb.db.engine.preaggregation.rdbms.RDBMS;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class SeriesPreAggregateReader extends SeriesReader implements ManagedSeriesReader {

  private static final RDBMS DB = RDBMS.getInstance();
  private final Map<String, TsFileSeriesStat> tsFileSeriesStats;
  private String currentTsFilePath;

  private boolean hasRemaining;
  private boolean managedByQueryManager;

  private BatchData batchData;
  private boolean hasCachedBatchData = false;

  public SeriesPreAggregateReader(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    super(
        seriesPath,
        allSensors,
        dataType,
        context,
        dataSource,
        timeFilter,
        valueFilter,
        fileFilter,
        ascending);
    tsFileSeriesStats = DB.getStatsUsed(
        seriesPath,
        valueFilter != null ? valueFilter : timeFilter);
  }

  @Override
  public boolean hasNextFile() throws IOException {
    return super.hasNextFile();
  }

  @Override
  public void skipCurrentFile() {
    currentTsFilePath = "";
    super.skipCurrentFile();
  }

  @Override
  protected void unpackOneTimeSeriesMetadata(ITimeSeriesMetadata timeSeriesMetadata)
      throws IOException {
    super.unpackOneTimeSeriesMetadata(timeSeriesMetadata);
  }

  @Override
  protected void unpackSeqTsFileResource() throws IOException {
    TsFileResource tsFileResource = orderUtils.getNextSeqFileResource(true);
    ITimeSeriesMetadata timeSeriesMetadata =
        FileLoaderUtils.loadTimeSeriesMetadata(
            tsFileResource, seriesPath, context, getAnyFilter(), allSensors);
    if (timeSeriesMetadata != null) {
      currentTsFilePath = tsFileResource.getTsFile().getAbsolutePath();
      timeSeriesMetadata.setSeq(true);
      seqTimeSeriesMetadata.add(timeSeriesMetadata);
    }
  }

  @Override
  protected void unpackUnseqTsFileResource() throws IOException {
    TsFileResource tsFileResource = orderUtils.getNextUnseqFileResource(true);
    ITimeSeriesMetadata timeSeriesMetadata =
        FileLoaderUtils.loadTimeSeriesMetadata(
            tsFileResource, seriesPath, context, getAnyFilter(), allSensors);
    if (timeSeriesMetadata != null) {
      currentTsFilePath = tsFileResource.getTsFile().getAbsolutePath();
      timeSeriesMetadata.setModified(true);
      timeSeriesMetadata.setSeq(false);
      seqTimeSeriesMetadata.add(timeSeriesMetadata);
    }
  }

  private boolean containedByTimeFilter(Statistics statistics) {
    Filter timeFilter = getTimeFilter();
    return timeFilter == null
        || timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  public boolean canUseCurrentFileStatistics() throws IOException {
    return !isFileOverlapped()
        && containedByTimeFilter(currentFileStatistics())
        && tsFileSeriesStats.get(currentTsFilePath) != null;
  }

  @Override
  public boolean hasNextChunk() throws IOException {
    return super.hasNextChunk();
  }

  @Override
  public void skipCurrentChunk() {
    super.skipCurrentChunk();
  }

  public boolean hasNextPage() throws IOException {
    return super.hasNextPage();
  }

  public boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = currentPageStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !isPageOverlapped()
        && containedByTimeFilter(currentPageStatistics)
        && !currentPageModified();
  }

  public Statistics currentPageStatistics() {
    return super.currentPageStatistics();
  }

  public void skipCurrentPage() {
    super.skipCurrentPage();
  }

  public BatchData nextPage() throws IOException {
    return super.nextPage().flip();
  }

  public boolean isCurrentFileCalculated() {
    return tsFileSeriesStats.get(currentTsFilePath) != null
        && tsFileSeriesStats.get(currentTsFilePath).getFileStatUsed();
  }

  public boolean isCurrentChunkCalculated() {
    TsFileSeriesStat tsFileSeriesStat = tsFileSeriesStats.get(currentTsFilePath);
    if (tsFileSeriesStat == null) {
      return false;
    }
    Map<Long, Boolean> chunkStatsUsed = tsFileSeriesStat.getChunkStatsUsed();
    if (chunkStatsUsed == null) {
      return false;
    }
    long offset = firstChunkMetadata.getOffsetOfChunkHeader();
    return chunkStatsUsed.get(offset) != null
        && chunkStatsUsed.get(offset);
  }

  @Override
  public boolean isManagedByQueryManager() {
    return managedByQueryManager;
  }

  @Override
  public void setManagedByQueryManager(boolean managedByQueryManager) {
    this.managedByQueryManager = managedByQueryManager;
  }

  @Override
  public boolean hasRemaining() {
    return hasRemaining;
  }

  @Override
  public void setHasRemaining(boolean hasRemaining) {
    this.hasRemaining = hasRemaining;
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    if (hasCachedBatchData) {
      return true;
    }
    if (readPageData()) {
      hasCachedBatchData = true;
      return true;
    }
    if (readChunkData()) {
      hasCachedBatchData = true;
      return true;
    }
    while (hasNextFile()) {
      if (isCurrentFileCalculated()) {
        skipCurrentFile();
        continue;
      }
      if (readChunkData()) {
        hasCachedBatchData = true;
        return true;
      }
    }
    return hasCachedBatchData;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    if (hasCachedBatchData || hasNextBatch()) {
      hasCachedBatchData = false;
      return batchData;
    }
    throw new IOException("no next batch");
  }

  @Override
  public void close() throws IOException {
    // no resources need to close
  }

  private boolean readPageData() throws IOException {
    while (hasNextPage()) {
      batchData = nextPage();
      if (batchData != null && batchData.hasCurrent()) {
        return true;
      }
    }
    return false;
  }

  private boolean readChunkData() throws IOException {
    while (hasNextChunk()) {
      if (isCurrentChunkCalculated()) {
        skipCurrentChunk();
        continue;
      }
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }
}
